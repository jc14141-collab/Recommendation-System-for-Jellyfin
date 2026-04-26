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

STAGING_URL    = os.getenv("STAGING_URL",    "http://localhost:30083")
CANARY_URL     = os.getenv("CANARY_URL",     "http://localhost:30084")
PROD_URL       = os.getenv("PROD_URL",       "http://localhost:30082")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:30090")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:30900")
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
    "staging_etag": None,
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

DEPLOYMENTS = {
    "staging": "serving-staging",
    "canary": "serving-canary",
    "prod": "serving-prod",
}

STAGING_ONNX_KEY = os.getenv(
    "STAGING_ONNX_KEY",
    "models/mlp/staging/model_mlp_best.onnx",
)



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


def reload_serving(env: str) -> bool:
    """Restart K8s deployment so pod re-downloads ONNX from MinIO on startup."""
    try:
        deployment = DEPLOYMENTS[env]

        result = subprocess.run(
            ["kubectl", "rollout", "restart", "deployment", "-n", "mlops", deployment],
            capture_output=True,
            text=True,
        )
        print(f"[monitor] Restarted {deployment}: {result.stdout.strip()} {result.stderr.strip()}")

        status = subprocess.run(
            ["kubectl", "rollout", "status", "deployment", "-n", "mlops", deployment, "--timeout=120s"],
            capture_output=True,
            text=True,
        )
        print(f"[monitor] {deployment} rollout status: {status.stdout.strip()} {status.stderr.strip()}")

        return status.returncode == 0

    except Exception as e:
        print(f"[monitor] Reload failed for {env}: {e}")
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

    if reload_serving("canary"):
        state["canary_deployed_at"] = time.time()
        state["canary_version"] = version
        print(f"[monitor] Canary deployed: {version}")
    else:
        print("[monitor] Failed to reload canary after promotion")


def promote_canary_to_prod():
    version = state["canary_version"]
    print(f"[monitor] Promoting {version}: canary -> prod/latest")

    copy_onnx_in_minio(
        "models/mlp/canary/model_mlp_best.onnx",
        "models/mlp/prod/model_mlp_best.onnx",
    )

    copy_onnx_in_minio(
        "models/mlp/canary/model_mlp_best.onnx",
        "models/mlp/latest/model_mlp_best.onnx",
    )

    if reload_serving("prod"):
        print(f"[monitor] Production promoted: {version}")
        print("[monitor] latest/ updated to match prod")
    else:
        print("[monitor] Failed to reload prod after promotion")

    state["staging_deployed_at"] = None
    state["staging_version"] = None
    state["canary_deployed_at"] = None
    state["canary_version"] = None


def rollback_canary():
    """Canary failed evaluation, roll back canary to prod version."""
    version = state.get("canary_version", "unknown")
    print(f"[monitor] Canary {version} failed evaluation, rolling back to prod version")

    copy_onnx_in_minio(
        "models/mlp/prod/model_mlp_best.onnx",
        "models/mlp/canary/model_mlp_best.onnx",
    )

    reload_serving("canary")

    state["canary_deployed_at"] = None
    state["canary_version"] = None


def get_s3_object_etag(key: str) -> str | None:
    try:
        s3 = build_s3()
        resp = s3.head_object(Bucket=BUCKET, Key=key)
        return resp.get("ETag", "").replace('"', "")
    except Exception as e:
        print(f"[monitor] Could not read s3://{BUCKET}/{key}: {e}")
        return None

def check_new_staging_model():
    """
    Detect new model in staging by checking MinIO object ETag.
    When export_to_onnx overwrites staging/model_mlp_best.onnx,
    the ETag changes, so monitor treats it as a new staging model.
    """
    etag = get_s3_object_etag(STAGING_ONNX_KEY)
    if not etag:
        print("[monitor] No staging ONNX found yet.")
        return

    previous_etag = state.get("staging_etag")

    if previous_etag is None:
        state["staging_etag"] = etag
        print(f"[monitor] Baseline staging model recorded: {etag[:12]}")
        return

    if etag != previous_etag:
        version = f"staging-etag-{etag[:12]}"
        print(f"[monitor] New staging model detected: {version}")

        state["staging_etag"] = etag
        state["staging_version"] = version

        if reload_serving("staging"):
            state["staging_deployed_at"] = time.time()
            print(f"[monitor] Staging reloaded for new model: {version}")
        else:
            print("[monitor] Failed to reload staging; will retry next check.")


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