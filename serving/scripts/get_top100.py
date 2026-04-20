import json
import sys
from pathlib import Path

import requests


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_payload(user_part: dict, movie_candidates: list[dict], request_k: int | None = None) -> dict:
    if not isinstance(user_part, dict):
        raise ValueError("user_part JSON must be an object.")

    if not isinstance(movie_candidates, list) or len(movie_candidates) == 0:
        raise ValueError("movie_candidates JSON must be a non-empty list.")

    required_user_fields = ["request_id", "user_id", "timestamp", "request_k", "user_embedding"]
    for field in required_user_fields:
        if field not in user_part:
            raise ValueError(f"user_part missing required field: {field}")

    for idx, item in enumerate(movie_candidates[:3]):
        if "movie_id" not in item or "movie_embedding" not in item:
            raise ValueError(
                f"movie_candidates item at index {idx} must contain movie_id and movie_embedding"
            )

    payload = {
        "request_id": str(user_part["request_id"]),
        "user_id": str(user_part["user_id"]),
        "timestamp": user_part["timestamp"],
        "request_k": int(request_k if request_k is not None else user_part["request_k"]),
        "user_embedding": user_part["user_embedding"],
        "candidates": movie_candidates,
    }

    if "client_context" in user_part:
        payload["client_context"] = user_part["client_context"]
    else:
        payload["client_context"] = {
            "surface": "homepage",
            "device_type": "web",
            "app_version": "0.1.0",
        }

    return payload


def request_recommendations(payload: dict, base_url: str) -> dict:
    endpoint = f"{base_url.rstrip('/')}/recommend"
    resp = requests.post(endpoint, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    if len(sys.argv) < 3:
        print(
            "Usage: python scripts/get_top100_from_parts.py <user_part_json> <movie_candidates_json> [base_url] [top_k]"
        )
        sys.exit(1)

    user_path = Path(sys.argv[1])
    movie_candidates_path = Path(sys.argv[2])
    base_url = sys.argv[3] if len(sys.argv) >= 4 else "http://127.0.0.1:8002"
    top_k = int(sys.argv[4]) if len(sys.argv) >= 5 else None

    user_part = load_json(user_path)
    movie_candidates = load_json(movie_candidates_path)

    payload = build_payload(user_part, movie_candidates, request_k=top_k)

    print(
        f"[info] user_id={payload['user_id']} candidates={len(payload['candidates'])} request_k={payload['request_k']}"
    )
    print(f"[info] POST {base_url.rstrip('/')}/recommend")

    result = request_recommendations(payload, base_url)

    print(
        f"[result] model_version={result.get('model_version')} "
        f"fallback_used={result.get('fallback_used')} "
        f"latency_ms={result.get('latency_ms')}"
    )

    recommendations = result.get("recommendations", [])
    print(f"[result] returned={len(recommendations)}")

    for item in recommendations:
        print(
            f"#{item['rank']:>3} movie_id={item['movie_id']} score={item['score']:.8f} reason={item['reason']}"
        )

    output_path = Path("topk_result.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"[saved] {output_path.resolve()}")


if __name__ == "__main__":
    main()