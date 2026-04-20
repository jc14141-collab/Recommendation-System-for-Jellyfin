#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_ENV="$PROJECT_ROOT/config.env"

echo "Project root: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

# ── Load config.env ──
if [ ! -f "$CONFIG_ENV" ]; then
    echo "[error] config.env not found at $CONFIG_ENV"
    exit 1
fi
set -a
source "$CONFIG_ENV"
set +a
echo "[ok] Loaded config from $CONFIG_ENV"

echo ""
echo "========================================"
echo " Step 1: Generate .env for docker compose"
echo "========================================"
cat > "$PROJECT_ROOT/.env" << EOF
MINIO_ENDPOINT=${MINIO_ENDPOINT}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY}
MINIO_BUCKET=${MINIO_BUCKET}
ONNX_OBJECT=${ONNX_OBJECT}
GF_SECURITY_ADMIN_USER=${GF_SECURITY_ADMIN_USER:-admin}
GF_SECURITY_ADMIN_PASSWORD=${GF_SECURITY_ADMIN_PASSWORD:-admin}
EOF
echo ".env written to $PROJECT_ROOT/.env"

echo ""
echo "========================================"
echo " Step 2: Initialize MinIO staging/canary/prod paths"
echo "========================================"
python3 - << PYEOF
import boto3, os
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url="${MINIO_ENDPOINT}",
    aws_access_key_id="${MINIO_ACCESS_KEY}",
    aws_secret_access_key="${MINIO_SECRET_KEY}",
    config=Config(signature_version="s3v4"),
)

src = "models/mlp/latest/model_mlp_best.onnx"
try:
    s3.head_object(Bucket="${MINIO_BUCKET}", Key=src)
    print(f"[ok] Source exists: s3://${MINIO_BUCKET}/{src}")
except Exception as e:
    print(f"[error] Source not found: s3://${MINIO_BUCKET}/{src}")
    print(f"        Make sure training has completed and exported ONNX first.")
    raise SystemExit(1)

for env in ["staging", "canary", "prod"]:
    dst = f"models/mlp/{env}/model_mlp_best.onnx"
    try:
        s3.head_object(Bucket="${MINIO_BUCKET}", Key=dst)
        print(f"[skip] Already exists: s3://${MINIO_BUCKET}/{dst}")
    except Exception:
        s3.copy_object(
            Bucket="${MINIO_BUCKET}",
            CopySource={"Bucket": "${MINIO_BUCKET}", "Key": src},
            Key=dst,
        )
        print(f"[ok]   Copied -> s3://${MINIO_BUCKET}/{dst}")
PYEOF

echo ""
echo "========================================"
echo " Step 3: Start containers"
echo "========================================"
COMPOSE_FILE="$PROJECT_ROOT/docker/docker-compose-multiworker.yaml"
docker compose --env-file "$PROJECT_ROOT/.env" -f "$COMPOSE_FILE" up --build -d

echo ""
echo "========================================"
echo " Step 4: Wait for containers to start (15s)..."
echo "========================================"
sleep 15

echo ""
echo "========================================"
echo " Step 5: Health check"
echo "========================================"
all_ok=true
for port in 8002 8003 8004; do
    env_name="prod"
    [ "$port" = "8003" ] && env_name="staging"
    [ "$port" = "8004" ] && env_name="canary"
    result=$(curl -sf "http://localhost:$port/health" 2>/dev/null || echo "FAILED")
    if echo "$result" | grep -q "ok"; then
        mode=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('serving_mode','?'))" 2>/dev/null || echo "?")
        version=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('model_version','?'))" 2>/dev/null || echo "?")
        echo "[ok]  $env_name (port $port) — mode=$mode version=$version"
    else
        echo "[fail] $env_name (port $port) — not responding"
        all_ok=false
    fi
done

echo ""
echo "========================================"
echo " Step 6: Prometheus targets"
echo "========================================"
sleep 15
curl -s "${PROMETHEUS_URL}/api/v1/targets" 2>/dev/null | python3 - << 'PYEOF'
import sys, json
try:
    data = json.load(sys.stdin)
    for t in data["data"]["activeTargets"]:
        job = t["labels"].get("job", "?")
        health = t["health"]
        symbol = "[ok]  " if health == "up" else "[fail]"
        print(f"{symbol} {job} -> {health}")
except Exception as e:
    print(f"Could not parse Prometheus response: {e}")
PYEOF

echo ""
if $all_ok; then
    echo "========================================"
    echo " Init complete!"
    echo " Grafana: http://localhost:3000"
    echo " Prod:    ${PROD_URL}"
    echo " Staging: ${STAGING_URL}"
    echo " Canary:  ${CANARY_URL}"
    echo "========================================"
else
    echo "[warn] Some instances failed."
fi

echo ""
echo "========================================"
echo " Step 7: Install host dependencies"
echo "========================================"
pip3 install -r "$PROJECT_ROOT/requirements-host.txt" --quiet
echo "Dependencies installed."

echo ""
echo "========================================"
echo " Step 8: Start monitor.py"
echo "========================================"
pkill -f "monitor.py" 2>/dev/null || true

# Export config.env vars to monitor process environment
set -a
source "$CONFIG_ENV"
set +a

nohup python3 "$PROJECT_ROOT/scripts/monitor.py" > /tmp/monitor.log 2>&1 &
MONITOR_PID=$!
echo "Monitor started with PID: $MONITOR_PID"

sleep 3
if kill -0 $MONITOR_PID 2>/dev/null; then
    echo "[ok]  monitor.py is running"
    tail -5 /tmp/monitor.log
else
    echo "[warn] monitor.py failed to start, check /tmp/monitor.log"
fi

echo ""
echo "========================================"
echo " All done! System is fully operational."
echo " Monitor log: tail -f /tmp/monitor.log"
echo "========================================"