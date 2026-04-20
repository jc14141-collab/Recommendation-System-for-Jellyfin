#!/bin/bash


set -euo pipefail


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
echo "Project root: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

echo "========================================"
echo " Step 1:  .env"
echo "========================================"
cat > "$PROJECT_ROOT/.env" << 'EOF'
MINIO_ENDPOINT=http://10.56.2.170:30900
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_BUCKET=warehouse
ONNX_OBJECT=models/mlp/latest/model_mlp_best.onnx
EOF
echo ".env written to $PROJECT_ROOT/.env"

echo ""
echo "========================================"
echo " Step 2:  MinIO staging/canary/prod "
echo "========================================"
python3 - << 'PYEOF'
import boto3
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://10.56.2.170:30900",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    config=Config(signature_version="s3v4"),
)

src = "models/mlp/latest/model_mlp_best.onnx"

try:
    s3.head_object(Bucket="warehouse", Key=src)
    print(f"[ok] Source exists: s3://warehouse/{src}")
except Exception as e:
    print(f"[error] Source not found: s3://warehouse/{src}")
    print(f"        Make sure training has completed and exported ONNX first.")
    raise SystemExit(1)

for env in ["staging", "canary", "prod"]:
    dst = f"models/mlp/{env}/model_mlp_best.onnx"
    try:
        s3.head_object(Bucket="warehouse", Key=dst)
        print(f"[skip] Already exists: s3://warehouse/{dst}")
    except Exception:
        s3.copy_object(
            Bucket="warehouse",
            CopySource={"Bucket": "warehouse", "Key": src},
            Key=dst,
        )
        print(f"[ok]   Copied -> s3://warehouse/{dst}")
PYEOF

echo ""
echo "========================================"
echo " Step 3: start"
echo "========================================"
COMPOSE_FILE="$PROJECT_ROOT/docker/docker-compose-multiworker.yaml"
ENV_FILE="$PROJECT_ROOT/.env"

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up  --build -d

echo ""
echo "========================================"
echo " Step 4: wait (15s)..."
echo "========================================"
sleep 15

echo ""
echo "========================================"
echo " Step 5: check"
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
curl -s http://localhost:9090/api/v1/targets 2>/dev/null | python3 - << 'PYEOF'
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
    echo " Init complete! All serving instances up."
    echo " Grafana: http://localhost:3000"
    echo " Prod:    http://localhost:8002"
    echo " Staging: http://localhost:8003"
    echo " Canary:  http://localhost:8004"
    echo "========================================"
else
    echo "[warn] Some instances failed. Check logs:"
    echo "  docker compose -f $COMPOSE_FILE logs"
fi

echo ""
echo "========================================"
echo " Step 7: install requirements"
echo "========================================"
pip3 install -r "$PROJECT_ROOT/serving/requirements-host.txt" --quiet
echo "Dependencies installed."

echo ""
echo "========================================"
echo " Step 8: start monitor.py"
echo "========================================"
pkill -f "monitor.py" 2>/dev/null || true

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