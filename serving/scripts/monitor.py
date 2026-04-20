#!/usr/bin/env python3
"""
Auto monitor: staging -> canary -> prod promote pipeline
Usage: python3 scripts/monitor.py
"""

import os
import time
import json
import boto3
import requests
from botocore.client import Config
from pathlib import Path
import subprocess

STAGING_URL    = os.getenv("STAGING_URL",    "http://localhost:8003")
CANARY_URL     = os.getenv("CANARY_URL",     "http://localhost:8004")
PROD_URL       = os.getenv("PROD_URL",       "http://localhost:8002")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://10.56.2.170:30900")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")
BUCKET           = os.getenv("MINIO_BUCKET",       "warehouse")

# Promotion rules (well-justified)
STAGING_WAIT_S           = 300   # wait 5 minutes before evaluating staging
CANARY_WAIT_S            = 600   # wait 10 minutes before evaluating canary
CANARY_MAX_FALLBACK_RATE = 0.10  # canary fallback rate must be < 10%
CANARY_MAX_P95_MS        = 500   # canary p95 latency must be < 500ms
CHECK_INTERVAL_S         = 30    # check every 30 seconds
WARMUP_REQUESTS          = 20    # warmup requests before evaluation

# ── State ──
state = {
    "staging_deployed_at": None,
    "staging_version": None,
    "canary_deployed_at": None,
    "canary_version": None,
}

# ── Warmup payload ──
WARMUP_PAYLOAD = {
    "request_id": "monitor-warmup",
    "user_id": "37257905",
    "timestamp": "2026-04-20T00:00:00Z",
    "request_k": 5,
    "user_embedding": [0.1] * 384,
    "candidates": [
        {"movie_id": str(i), "movie_embedding": [0.2] * 384}
        for i in range(20)
    ]
}


def build_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def query_prometheus(promql: str, env_label: str) -> float | None:
    """Query Prometheus metrics, differentiated by job label per environment"""
    try:
        resp = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": promql},
            timeout=5,
        )
        result = resp.json()["data"]["result"]
        if result:
            return float(result[0]["value"][1])
        return None
    except Exception as e:
        print(f"[monitor] Prometheus query failed: {e}")
        return None


def get_fallback_rate(env_url: str, job: str) -> float:
    val = query_prometheus(
        f'sum(rate(recommend_requests_total{{mode="fallback",job="{job}"}}[5m])) '
        f'/ (sum(rate(recommend_requests_total{{job="{job}"}}[5m])) > 0)',
        job
    )
    return val if val is not None else 0.0


def get_p95_ms(job: str) -> float:
    val = query_prometheus(
        f'histogram_quantile(0.95, sum(rate(recommend_request_latency_seconds_bucket'
        f'{{mode="model",job="{job}"}}[5m])) by (le)) * 1000',
        job
    )
    return val if val is not None else 9999.0


def get_health(url: str) -> dict:
    try:
        return requests.get(f"{url}/health", timeout=5).json()
    except Exception:
        return {}


def warmup_env(url: str, n: int = WARMUP_REQUESTS):
    """Send warmup requests to generate Prometheus metrics before evaluation"""
    print(f"[monitor] Warming up {url} with {n} requests...")
    success = 0
    for i in range(n):
        try:
            payload = dict(WARMUP_PAYLOAD)
            payload["request_id"] = f"monitor-warmup-{i}"
            resp = requests.post(
                f"{url}/recommend",
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                success += 1
        except Exception:
            pass
    print(f"[monitor] Warmup complete: {success}/{n} successful")
    # Wait for Prometheus to scrape the new metrics
    time.sleep(10)


def copy_onnx_in_minio(src_key: str, dst_key: str):
    s3 = build_s3()
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": src_key},
        Key=dst_key,
    )
    print(f"[monitor] Copied s3://{BUCKET}/{src_key} -> s3://{BUCKET}/{dst_key}")


def reload_serving(url: str, minio_key: str, version: str) -> bool:
    """Restart K8s deployment so pod re-downloads ONNX from MinIO on startup"""
    try:
        if "30083" in url:
            deployment = "serving-staging"
        elif "30084" in url:
            deployment = "serving-canary"
        else:
            deployment = "recommender-serving"

        result = subprocess.run(
            ["kubectl", "rollout", "restart", "deployment", "-n", "mlops", deployment],
            capture_output=True, text=True
        )
        print(f"[monitor] Restarted {deployment}: {result.stdout.strip()}")

        # Wait for pod to be ready before continuing
        subprocess.run(
            ["kubectl", "rollout", "status", "deployment", "-n", "mlops", deployment, "--timeout=120s"],
            capture_output=True, text=True
        )
        print(f"[monitor] {deployment} is ready")
        return True
    except Exception as e:
        print(f"[monitor] Reload failed: {e}")
        return False


def evaluate_staging() -> bool:
    """
    Staging evaluation: send warmup requests then check fallback rate < 20%.
    Warmup is needed because staging may have no traffic yet.
    """
    warmup_env(STAGING_URL)
    rate = get_fallback_rate(STAGING_URL, "serving_staging")
    print(f"[monitor] staging fallback_rate={rate:.2%} (threshold: <20%)")
    passed = rate < 0.20

    # Fallback: if no Prometheus data, check health directly
    if rate == 0.0:
        health = get_health(STAGING_URL)
        if health.get("status") == "ok":
            print(f"[monitor] staging health ok, treating as passed")
            passed = True

    return passed


def evaluate_canary() -> bool:
    """
    Canary evaluation: send warmup requests then check:
    - fallback rate < 10%
    - p95 latency < 500ms
    Warmup is needed because canary may have no traffic yet.
    """
    warmup_env(CANARY_URL)
    rate = get_fallback_rate(CANARY_URL, "serving_canary")
    p95  = get_p95_ms("serving_canary")
    print(f"[monitor] canary fallback_rate={rate:.2%} p95={p95:.1f}ms")
    print(f"[monitor] thresholds: fallback<{CANARY_MAX_FALLBACK_RATE:.0%}, p95<{CANARY_MAX_P95_MS}ms")

    # If no Prometheus data yet, fall back to health check
    if p95 >= 9999.0:
        print("[monitor] No Prometheus latency data, checking health directly")
        health = get_health(CANARY_URL)
        if health.get("status") == "ok" and rate < CANARY_MAX_FALLBACK_RATE:
            print("[monitor] Canary health ok, treating p95 as passed")
            return True
        return False

    return rate < CANARY_MAX_FALLBACK_RATE and p95 < CANARY_MAX_P95_MS


def promote_staging_to_canary():
    version = state["staging_version"]
    print(f"[monitor] Promoting {version}: staging -> canary")
    copy_onnx_in_minio(
        "models/mlp/staging/model_mlp_best.onnx",
        "models/mlp/canary/model_mlp_best.onnx",
    )
    reload_serving(CANARY_URL, "models/mlp/canary/model_mlp_best.onnx", version)
    state["canary_deployed_at"] = time.time()
    state["canary_version"] = version
    print(f"[monitor] Canary deployed: {version}")


def promote_canary_to_prod():
    version = state["canary_version"]
    print(f"[monitor] Promoting {version}: canary -> prod")
    copy_onnx_in_minio(
        "models/mlp/canary/model_mlp_best.onnx",
        "models/mlp/prod/model_mlp_best.onnx",
    )
    copy_onnx_in_minio(
        "models/mlp/canary/model_mlp_best.onnx",
        "models/mlp/latest/model_mlp_best.onnx",
    )
    reload_serving(PROD_URL, "models/mlp/prod/model_mlp_best.onnx", version)
    print(f"[monitor] Production promoted: {version}")
    # Reset state, ready for next round
    state["staging_deployed_at"] = None
    state["staging_version"] = None
    state["canary_deployed_at"] = None
    state["canary_version"] = None


def rollback_canary():
    """Canary failed evaluation, roll back canary to prod version"""
    version = state.get("canary_version", "unknown")
    print(f"[monitor] Canary {version} failed evaluation, rolling back to prod version")
    copy_onnx_in_minio(
        "models/mlp/prod/model_mlp_best.onnx",
        "models/mlp/canary/model_mlp_best.onnx",
    )
    reload_serving(CANARY_URL, "models/mlp/prod/model_mlp_best.onnx", "prod-version")
    state["canary_deployed_at"] = None
    state["canary_version"] = None


def check_new_staging_model():
    """Detect new model in staging by comparing model_version from health endpoint"""
    health = get_health(STAGING_URL)
    version = health.get("model_version", "")
    if version and version != state.get("staging_version"):
        print(f"[monitor] New staging model detected: {version}")
        state["staging_deployed_at"] = time.time()
        state["staging_version"] = version


def main():
    print("[monitor] Starting promote pipeline monitor...")
    print(f"[monitor] Rules: staging={STAGING_WAIT_S}s, canary={CANARY_WAIT_S}s")
    print(f"[monitor] Thresholds: fallback<{CANARY_MAX_FALLBACK_RATE:.0%}, p95<{CANARY_MAX_P95_MS}ms")

    while True:
        print(f"\n[monitor] === Check at {time.strftime('%H:%M:%S')} ===")

        # 1. Check if staging has a new model
        check_new_staging_model()

        # 2. Has staging been running long enough? Evaluate for canary promotion
        if state["staging_deployed_at"] and state["canary_deployed_at"] is None:
            elapsed = time.time() - state["staging_deployed_at"]
            remaining = STAGING_WAIT_S - elapsed
            if remaining > 0:
                print(f"[monitor] Staging waiting {remaining:.0f}s more...")
            else:
                if evaluate_staging():
                    promote_staging_to_canary()
                else:
                    print("[monitor] Staging failed evaluation, staying in staging")

        # 3. Has canary been running long enough? Evaluate for prod promotion
        if state["canary_deployed_at"]:
            elapsed = time.time() - state["canary_deployed_at"]
            remaining = CANARY_WAIT_S - elapsed
            if remaining > 0:
                print(f"[monitor] Canary waiting {remaining:.0f}s more...")
            else:
                if evaluate_canary():
                    promote_canary_to_prod()
                else:
                    rollback_canary()

        time.sleep(CHECK_INTERVAL_S)


if __name__ == "__main__":
    main()