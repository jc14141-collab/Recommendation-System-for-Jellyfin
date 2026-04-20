"""
Inference engine - fetches candidates from API, reranks with MLP model.
"""
import os
import io
import csv
import numpy as np
import torch
import sys
import requests
import uuid

from datetime import datetime

sys.path.insert(0, "/app")

from scripts.data_loader import S3Config, build_boto3_s3_client, build_arrow_s3_filesystem
from scripts.retrain import RecommenderMLP



CANDIDATE_API = os.environ.get(
    "CANDIDATE_API",
    "http://online-service.mlops.svc.cluster.local:18080/candidates",
)

SERVING_URL = os.environ.get("SERVING_URL", "http://localhost:8002")

class RecommendationEngine:
    def __init__(self, s3_cfg, data_cfg):
        self.s3_cfg = s3_cfg
        self.data_cfg = data_cfg
        self.client = build_boto3_s3_client(s3_cfg)

        self.model = None
        self.movie_info = {}
        self.loaded_model_key = None

    def load_movies_csv(self):
        print("Loading movies.csv...")
        obj = self.client.get_object(Bucket='raw', Key='movies.csv')
        content = obj['Body'].read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            mid = int(row['movieId'])
            self.movie_info[mid] = {
                'title': row['title'],
                'genres': row['genres'],
            }
        print(f"  Loaded {len(self.movie_info)} movies")

    def load_embeddings(self, version=None):
        print("Embeddings will be fetched from candidate API on demand")

    def load_model(self, model_key="models/mlp/latest/model_mlp_best.pt"):
        if "lightgbm" in model_key:
            import lightgbm as lgb
            local_path = "/tmp/inference_model_lgb.txt"
            self.client.download_file("warehouse", model_key, local_path)
            self.model = lgb.Booster(model_file=local_path)
            self.loaded_model_key = model_key
            self._model_type = "lightgbm"
            print("  LightGBM model loaded!")
            return
        if self.loaded_model_key == model_key and self.model is not None:
            return

        print(f"Loading model from s3://warehouse/{model_key}...")
        local_path = "/tmp/inference_model.pt"
        self.client.download_file("warehouse", model_key, local_path)

        if "mlp_large" in model_key:
            hidden_dims = [1024, 512, 256, 128]
        else:
            hidden_dims = [512, 256, 128]
        self.model = RecommenderMLP(
            embedding_dim=self.data_cfg.get('embedding_dim', 384),
            hidden_dims=hidden_dims,
            dropout=0.0,
        )

        state_dict = torch.load(local_path, map_location='cpu', weights_only=True)
        new_state_dict = {}
        for k, v in state_dict.items():
            new_key = f"net.{k}" if not k.startswith("net.") else k
            new_state_dict[new_key] = v
        self.model.load_state_dict(new_state_dict)
        self.model.eval()
        self.loaded_model_key = model_key
        print("  Model loaded!")

    def _get_popular_fallback(self, top_n: int) -> list:
        try:
            if not self.movie_info:
                self.load_movies_csv()
            TOP_MOVIE_IDS = [
                318,
                858,
                527,
                1221,
                2959,
                1193,
                50,
                593,
                260,
                1196,
                4993,
                7153,
                296,
                356,
                2571,
                589,
                1270,
                364,
                3578,
                2858,
            ]
            ordered = [mid for mid in TOP_MOVIE_IDS if mid in self.movie_info]
            existing = set(ordered)
            for mid in self.movie_info:
                if mid not in existing:
                    ordered.append(mid)
                    ordered.append(mid)
            movie_ids = ordered[:top_n]
            results = []
            for i, mid in enumerate(movie_ids):
                info = self.movie_info.get(mid, {})
                results.append({
                    'movie_id': mid,
                    'title': info.get('title', f'Movie {mid}'),
                    'genres': info.get('genres', 'Unknown'),
                    'score': 0.0,
                    'method': 'popular_fallback_minio',
                    'candidate_rank': i + 1,
                    'candidate_score': 0.0,
                })
            return results
        except Exception as e:
            print(f"[inference] MinIO fallback failed: {e}")
            return []

    def recommend(self, user_id, top_n=10, model_key=None):
        if model_key:
            self.load_model(model_key)
        elif self.model is None:
            self.load_model()

        try:
            resp = requests.get(
                CANDIDATE_API,
                params={"user_id": user_id, "top_k": 50},
                headers={"Accept": "application/json"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
        except Exception as e:
            print(f"[inference] Candidate API failed: {e}, using MinIO popular fallback")
            fallback = self._get_popular_fallback(top_n)
            if fallback:
                return fallback, None
            return None, f"Candidate API error and MinIO fallback failed: {str(e)}"

        if not items:
            print(f"[inference] No candidates for user {user_id}, using MinIO popular fallback")
            fallback = self._get_popular_fallback(top_n)
            if fallback:
                return fallback, None
            return None, f"No candidates for user {user_id}"

        category = data.get("category", "popular")
        has_embeddings = "embedding" in items[0] and "user_embedding" in data

        if has_embeddings:
            payload = {
                "request_id": f"{user_id}-{int(datetime.utcnow().timestamp() * 1000)}",
                "user_id": str(user_id),
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "request_k": top_n,
                "user_embedding": data["user_embedding"],
                "candidates": [
                    {
                        "movie_id": str(item["movie_id"]),
                        "movie_embedding": item["embedding"],
                    }
                    for item in items
                ],
            }

            try:
                serving_resp = requests.post(
                    f"{SERVING_URL}/recommend",
                    json=payload,
                    timeout=30,
                )
                serving_resp.raise_for_status()
                serving_data = serving_resp.json()

                item_map = {str(item["movie_id"]): item for item in items}
                is_fallback = serving_data.get("fallback_used", False)

                results = []
                for rec in serving_data.get("recommendations", []):
                    movie_id_str = str(rec["movie_id"])
                    try:
                        mid = int(movie_id_str)
                    except ValueError:
                        mid = movie_id_str

                    info = self.movie_info.get(mid, {})
                    original_item = item_map.get(movie_id_str, {})

                    results.append({
                        'movie_id': mid,
                        'title': info.get('title', f'Movie {mid}'),
                        'genres': info.get('genres', 'Unknown'),
                        'score': float(rec["score"]),
                        'method': 'popular_fallback' if is_fallback else 'mlp_reranking',
                        'candidate_rank': original_item.get('rank', rec["rank"]),
                        'candidate_score': float(original_item.get('score', 0)),
                    })

            except Exception as e:
                print(f"[inference] Serving failed, falling back to local model: {e}")
                user_emb = np.array(data["user_embedding"], dtype=np.float32)
                movie_ids = [item["movie_id"] for item in items]
                movie_embs = np.array([item["embedding"] for item in items], dtype=np.float32)
                user_embs = np.tile(user_emb, (len(movie_ids), 1))

                if hasattr(self, '_model_type') and self._model_type == 'lightgbm':
                    cosine_sim = np.sum(user_embs * movie_embs, axis=1) / (
                                np.linalg.norm(user_embs, axis=1) * np.linalg.norm(movie_embs, axis=1) + 1e-8)
                    dot_product = np.sum(user_embs * movie_embs, axis=1)
                    l2_dist = np.linalg.norm(user_embs - movie_embs, axis=1)
                    X = np.hstack([user_embs, movie_embs, cosine_sim.reshape(-1, 1), dot_product.reshape(-1, 1),
                                   l2_dist.reshape(-1, 1)])
                    scores = self.model.predict(X)
                    ranked_idx = np.argsort(scores)[::-1][:top_n]
                    results = []
                    for idx in ranked_idx:
                        mid = movie_ids[idx]
                        info = self.movie_info.get(mid, {})
                        results.append({
                            'movie_id': mid,
                            'title': info.get('title', f'Movie {mid}'),
                            'genres': info.get('genres', 'Unknown'),
                            'score': float(scores[idx]),
                            'method': 'lightgbm_reranking',
                            'candidate_rank': items[idx].get('rank', 0),
                            'candidate_score': float(items[idx].get('score', 0)),
                        })
                    return results, None

                with torch.no_grad():
                    user_t = torch.tensor(user_embs, dtype=torch.float32)
                    movie_t = torch.tensor(movie_embs, dtype=torch.float32)
                    scores = self.model(user_t, movie_t).numpy()

                ranked_idx = np.argsort(scores)[::-1][:top_n]
                results = []
                for idx in ranked_idx:
                    mid = movie_ids[idx]
                    info = self.movie_info.get(mid, {})
                    results.append({
                        'movie_id': mid,
                        'title': info.get('title', f'Movie {mid}'),
                        'genres': info.get('genres', 'Unknown'),
                        'score': float(scores[idx]),
                        'method': 'mlp_reranking',
                        'candidate_rank': items[idx].get('rank', 0),
                        'candidate_score': float(items[idx].get('score', 0)),
                    })


        else:
            sorted_items = sorted(items, key=lambda x: x.get('score', 0), reverse=True)[:20]
            max_score = max(item.get('score', 1) for item in sorted_items) or 1  # avoid division by zero
            results = []
            for i, item in enumerate(sorted_items):
                mid = item["movie_id"]
                info = self.movie_info.get(mid, {})
                results.append({
                    'movie_id': mid,
                    'title': info.get('title', f'Movie {mid}'),
                    'genres': info.get('genres', 'Unknown'),
                    'score': round(float(item.get('score', 0)) / max_score, 4),  # normalize to 0-1
                    'method': 'popular_fallback',
                    'candidate_rank': item.get('rank', i + 1),
                    'candidate_score': float(item.get('score', 0)),
                })

        return results, None

    def get_user_ids(self):
        return [37257905,32218290,40262109,13637575,82016510,72043515,93240786,89131271,94374605,13471527,12404661,29348212,41348220,63798927,77349752,932407]

    def get_model_versions(self):
        resp = self.client.list_objects_v2(
            Bucket='warehouse', Prefix='models/mlp/', Delimiter='/'
        )
        versions = []
        for p in resp.get('CommonPrefixes', []):
            name = p['Prefix'].split('/')[-2]
            if name != 'latest':
                versions.append(name)
        versions.append('latest')
        return versions
