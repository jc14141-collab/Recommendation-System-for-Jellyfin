from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from .metrics import collect_metrics
from .recommender import STATE, score_request
from .schemas import RecommendRequest, RecommendResponse

from .metrics import collect_metrics, USER_CLICKS

app = FastAPI(
    title="Jellyfin Recommender Multiworker API",
    description="ONNX Runtime scorer — multi-worker deployment with Prometheus monitoring",
    version="0.4.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class SetModeRequest(BaseModel):
    mode: str


class RollbackRequest(BaseModel):
    model_path: str
    model_version: str | None = None


class FeedbackRequest(BaseModel):
    request_id: str
    user_id: str
    clicked_movie_id: str
    clicked_rank: int

@app.post("/feedback")
def feedback(body: FeedbackRequest):
    USER_CLICKS.labels(rank=body.clicked_rank).inc()
    return {"status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok", **STATE.get_runtime_flags()}


@app.post("/admin/reload")
def reload_model(minio_key: str, version: str):
    """Reload model from MinIO key"""
    try:
        dest = Path("/tmp/model_mlp_best.onnx")
        os.environ["ONNX_OBJECT"] = minio_key
        _download_onnx_from_minio(dest)
        STATE.load_model(dest)
        STATE.model_version = version
        STATE.mode = "model"
        STATE.reset_circuit_breaker()
        return {"status": "ok", "model_version": version}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
def metrics() -> Response:
    body, content_type = collect_metrics()
    return Response(content=body, media_type=content_type)


@app.post("/recommend", response_model=RecommendResponse)
def recommend_endpoint(req: RecommendRequest):
    return score_request(req)


@app.post("/admin/set-mode")
def set_mode(body: SetModeRequest):
    STATE.set_mode(body.mode)
    return {"status": "ok", "mode": STATE.mode}


@app.post("/admin/rollback")
def rollback(body: RollbackRequest):
    try:
        STATE.reload_model(body.model_path, body.model_version)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {
        "status": "ok",
        "model_version": STATE.model_version,
        "serving_mode": STATE.mode,
    }
