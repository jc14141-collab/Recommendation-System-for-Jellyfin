#!/bin/bash
set -euo pipefail

CONFIG="${1:-config/config.yaml}"
PT_PATH="/tmp/model_mlp_best.pt"
ONNX_PATH="/tmp/model_mlp_best.onnx"
STAGING_URL="http://172.17.0.1:8003"

echo "========================================"
echo " Export .pt -> .onnx & upload to MinIO"
echo "========================================"
python3 scripts/export_to_onnx.py \
    --config    "$CONFIG"   \
    --pt-path   "$PT_PATH"  \
    --onnx-path "$ONNX_PATH"

echo ""
echo "========================================"
echo " Deploy to staging"
echo "========================================"

VERSION=$(python3 -c "
import yaml, re
cfg = yaml.safe_load(open('$CONFIG'))
key = cfg['model_output']['version_key']
m = re.search(r'(v\d+)', key)
print(m.group(1) if m else 'unknown')
")

curl -s -X POST "$STAGING_URL/admin/rollback" \
  -H "Content-Type: application/json" \
  -d "{\"model_path\": \"$ONNX_PATH\", \"model_version\": \"$VERSION\"}" \
  && echo "Staging deployed: $VERSION" \
  || echo "Staging deploy failed"

echo ""
echo "========================================"
echo " Pipeline complete! monitor.py handles promotion"
echo "========================================"