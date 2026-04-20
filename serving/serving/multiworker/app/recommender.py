from __future__ import annotations

import json
import os
import threading
from collections import deque
from pathlib import Path
from time import perf_counter
from typing import Optional

import numpy as np
import onnxruntime as ort

from .metrics import (
    CANDIDATE_COUNT,
    FALLBACK_TOTAL,
    REQUEST_LATENCY,
    REQUESTS_TOTAL,
    RETURN_COUNT,
    RECOMMENDATION_SCORE,
)
from .schemas import RecommendRequest, RecommendResponse, RecommendationItem

import boto3
from botocore.client import Config as BotocoreConfig

EMBEDDING_DIM = 384

def _download_onnx_from_minio(dest: Path) -> None:
    endpoint   = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]
    bucket     = os.getenv("MINIO_BUCKET", "warehouse")
    obj_key    = os.getenv("ONNX_OBJECT",  "models/mlp/latest/model_mlp_best.onnx")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotocoreConfig(signature_version="s3v4"),
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"[ServingState] Downloading s3://{bucket}/{obj_key} -> {dest}")
    s3.download_file(bucket, obj_key, str(dest))
    print("[ServingState] Download complete.")

LATENCY_THRESHOLD_S: float = float(os.getenv("LATENCY_FALLBACK_THRESHOLD_S", "1"))

_CIRCUIT_WINDOW: int = 50
_CIRCUIT_FALLBACK_RATE: float = 0.30

_STATE_FILE = Path(os.getenv("MODEL_STATE_FILE", "/tmp/jellyfin_model_state.json"))


def _normalize_mode(value: str) -> str:
    mode = (value or "model").strip().lower()
    return mode if mode in {"model", "fallback"} else "model"


def resolve_model_path(filename: str) -> Path:
    here = Path(__file__).resolve()
    candidates = [
        here.parents[1] / "models" / filename,
        Path.cwd() / "models" / filename,
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        f"Could not find {filename}. Tried: " + ", ".join(str(p) for p in candidates)
    )


def _create_session(path: Path) -> ort.InferenceSession:
    return ort.InferenceSession(str(path), providers=["CPUExecutionProvider"])


def _resolve_fallback_ids_path() -> Path:
    env_override = os.getenv("FALLBACK_IDS_FILE", "")
    if env_override:
        return Path(env_override)
    return Path(__file__).resolve().parent / "fallback_popular_movies.json"


def _load_fallback_ids(path: Path) -> list[str]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return [str(v) for v in data if v is not None]
    if isinstance(data, dict):
        values = data.get("movie_ids") or data.get("item_ids") or []
        return [str(v) for v in values if v is not None]
    return []


class ServingState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._mode: str = _normalize_mode(os.getenv("SERVING_MODE", "model"))
        self._model_version: str = os.getenv("MODEL_VERSION", "mlp-best-onnx-multiworker-v2")
        local_onnx = Path("/tmp/model_mlp_best.onnx")
        if os.getenv("MINIO_ENDPOINT") and not local_onnx.exists():
            _download_onnx_from_minio(local_onnx)
            self._model_path = local_onnx
        else:
            self._model_path = local_onnx if local_onnx.exists() else resolve_model_path("model_mlp_best.onnx")
        self._session: ort.InferenceSession = _create_session(self._model_path)
        self._state_mtime: float = 0.0
        self._recent_fallback: deque[bool] = deque(maxlen=_CIRCUIT_WINDOW)
        self._fallback_ids_path: Path = _resolve_fallback_ids_path()
        self._fallback_ids: list[str] = _load_fallback_ids(self._fallback_ids_path)
        self._fallback_pool_size: int = max(
            1, min(500, int(os.getenv("FALLBACK_POOL_SIZE", "100")))
        )

    def _check_state_file(self) -> None:
        if not _STATE_FILE.exists():
            return
        try:
            mtime = _STATE_FILE.stat().st_mtime
        except OSError:
            return
        if mtime <= self._state_mtime:
            return
        try:
            with open(_STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            new_mode = _normalize_mode(state.get("mode", ""))
            new_model_path_str = state.get("model_path", "")
            new_version = state.get("model_version", "")
            with self._lock:
                self._mode = new_mode
                if new_model_path_str:
                    p = Path(new_model_path_str)
                    if p.exists() and p != self._model_path:
                        self._session = _create_session(p)
                        self._model_path = p
                if new_version:
                    self._model_version = new_version
                self._state_mtime = mtime
        except Exception:
            pass

    def _write_state_file(self) -> None:
        with self._lock:
            state = {
                "mode": self._mode,
                "model_path": str(self._model_path),
                "model_version": self._model_version,
            }
        try:
            _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = _STATE_FILE.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f)
            tmp.replace(_STATE_FILE)
        except Exception:
            pass

    def _record_fallback(self, used: bool) -> None:
        with self._lock:
            self._recent_fallback.append(used)
            if (
                len(self._recent_fallback) >= _CIRCUIT_WINDOW
                and self._mode == "model"
            ):
                rate = sum(self._recent_fallback) / len(self._recent_fallback)
                if rate > _CIRCUIT_FALLBACK_RATE:
                    self._mode = "fallback"

    @property
    def mode(self) -> str:
        with self._lock:
            return self._mode

    @property
    def model_version(self) -> str:
        with self._lock:
            return self._model_version

    @property
    def session(self) -> ort.InferenceSession:
        with self._lock:
            return self._session

    @property
    def fallback_ids(self) -> list[str]:
        with self._lock:
            return self._fallback_ids

    @property
    def fallback_pool_size(self) -> int:
        with self._lock:
            return self._fallback_pool_size

    def set_mode(self, mode: str) -> None:
        with self._lock:
            self._mode = _normalize_mode(mode)
        self._write_state_file()

    def reload_model(self, model_path: str, model_version: Optional[str] = None) -> None:
        p = Path(model_path)
        new_session = _create_session(p)
        with self._lock:
            self._session = new_session
            self._model_path = p
            if model_version:
                self._model_version = model_version
            self._recent_fallback.clear()
            self._mode = "model"
        self._write_state_file()

    def get_runtime_flags(self) -> dict:
        with self._lock:
            recent = list(self._recent_fallback)
        rate = (sum(recent) / len(recent)) if recent else 0.0
        with self._lock:
            return {
                "serving_mode": self._mode,
                "model_version": self._model_version,
                "model_path": str(self._model_path),
                "fallback_pool_size": self._fallback_pool_size,
                "fallback_ids_count": len(self._fallback_ids),
                "circuit_fallback_rate": round(rate, 4),
                "circuit_window": _CIRCUIT_WINDOW,
                "circuit_fallback_rate_threshold": _CIRCUIT_FALLBACK_RATE,
                "latency_fallback_threshold_s": LATENCY_THRESHOLD_S,
            }


STATE = ServingState()


def _build_features(req: RecommendRequest) -> tuple[np.ndarray, np.ndarray, list[str]]:
    if not req.candidates:
        empty = np.empty((0, EMBEDDING_DIM), dtype=np.float32)
        return empty, empty, []

    user = np.asarray(req.user_embedding, dtype=np.float32)
    movie_ids = [c.movie_id for c in req.candidates]
    movies = np.asarray([c.movie_embedding for c in req.candidates], dtype=np.float32)
    users = np.broadcast_to(user, movies.shape).astype(np.float32)

    return users, movies, movie_ids


def _fallback_movie_ids(req: RecommendRequest) -> list[str]:
    ids = STATE.fallback_ids
    if ids:
        return ids[: STATE.fallback_pool_size]
    if req.candidates:
        return [c.movie_id for c in req.candidates][: STATE.fallback_pool_size]
    return []


def _build_fallback_response(
    req: RecommendRequest, started: float, reason: str
) -> RecommendResponse:
    movie_ids = _fallback_movie_ids(req)
    top_k = min(req.request_k, len(movie_ids))
    recommendations = [
        RecommendationItem(rank=rank, movie_id=movie_ids[idx], score=0.0, reason=reason)
        for rank, idx in enumerate(range(top_k), start=1)
    ]
    latency_s = perf_counter() - started

    FALLBACK_TOTAL.labels(reason=reason).inc()
    RETURN_COUNT.observe(len(recommendations))
    REQUESTS_TOTAL.labels(mode="fallback", status="ok").inc()
    REQUEST_LATENCY.labels(mode="fallback").observe(latency_s)
    STATE._record_fallback(True)

    return RecommendResponse(
        request_id=req.request_id,
        user_id=req.user_id,
        timestamp=req.timestamp,
        model_version=f"fallback-popular-{STATE.fallback_pool_size}-v1",
        fallback_used=True,
        latency_ms=round(latency_s * 1000, 3),
        recommendations=recommendations,
    )


def score_request(req: RecommendRequest) -> RecommendResponse:
    STATE._check_state_file()

    started = perf_counter()
    CANDIDATE_COUNT.observe(len(req.candidates))

    if STATE.mode == "fallback":
        return _build_fallback_response(req, started, reason="fallback_mode_enabled")

    if not req.candidates:
        latency_s = perf_counter() - started
        REQUESTS_TOTAL.labels(mode="model", status="ok").inc()
        REQUEST_LATENCY.labels(mode="model").observe(latency_s)
        RETURN_COUNT.observe(0)
        STATE._record_fallback(False)
        return RecommendResponse(
            request_id=req.request_id,
            user_id=req.user_id,
            timestamp=req.timestamp,
            model_version=STATE.model_version,
            fallback_used=False,
            latency_ms=round(latency_s * 1000, 3),
            recommendations=[],
        )

    users, movies, movie_ids = _build_features(req)

    try:
        scores = STATE.session.run(
            ["scores"],
            {
                "user_embedding": users,  # (N, 384) float32
                "movie_embedding": movies,  # (N, 384) float32
            }
        )[0].reshape(-1)
    except Exception:
        return _build_fallback_response(req, started, reason="fallback_inference_error")

    elapsed_s = perf_counter() - started
    if elapsed_s > LATENCY_THRESHOLD_S:
        return _build_fallback_response(req, started, reason="fallback_latency_exceeded")

    top_k = min(req.request_k, len(movie_ids))
    top_indices = np.argsort(scores)[::-1][:top_k]

    for score in scores:
        RECOMMENDATION_SCORE.observe(float(score))

    recommendations = [
        RecommendationItem(
            rank=rank,
            movie_id=movie_ids[idx],
            score=round(float(scores[idx]), 8),
            reason="ranked_by_onnx_model",
        )
        for rank, idx in enumerate(top_indices, start=1)
    ]

    REQUESTS_TOTAL.labels(mode="model", status="ok").inc()
    REQUEST_LATENCY.labels(mode="model").observe(elapsed_s)
    RETURN_COUNT.observe(len(recommendations))
    STATE._record_fallback(False)

    return RecommendResponse(
        request_id=req.request_id,
        user_id=req.user_id,
        timestamp=req.timestamp,
        model_version=STATE.model_version,
        fallback_used=False,
        latency_ms=round(elapsed_s * 1000, 3),
        recommendations=recommendations,
    )
