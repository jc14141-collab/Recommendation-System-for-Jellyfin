#!/usr/bin/env bash
set -euo pipefail

FRONTEND_REPO="${FRONTEND_REPO:-https://github.com/Teqqquila/JF-frontend.git}"
BACKEND_REPO="${BACKEND_REPO:-https://github.com/jellyfin/jellyfin.git}"
BASE_DIR="${BASE_DIR:-$HOME/custom-jellyfin-managed}"
FRONTEND_DIR="${FRONTEND_DIR:-$BASE_DIR/jellyfin-web}"
BACKEND_DIR="${BACKEND_DIR:-$BASE_DIR/jellyfin}"
DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}"
DOTNET_BIN="${DOTNET_BIN:-$DOTNET_ROOT/dotnet}"
SERVICE_NAME="${SERVICE_NAME:-custom-jellyfin}"

echo "[1/8] Installing OS dependencies..."
sudo apt update
sudo apt install -y wget git curl ffmpeg build-essential jq python3

echo "[2/8] Installing Node.js 24 if needed..."
if ! command -v node >/dev/null 2>&1 || ! node --version | grep -q '^v24\.'; then
  curl -fsSL https://deb.nodesource.com/setup_24.x | sudo -E bash -
  sudo apt install -y nodejs
fi
node --version
npm --version

echo "[3/8] Preparing managed source directories..."
mkdir -p "$BASE_DIR"

if [ ! -d "$FRONTEND_DIR/.git" ]; then
  git clone "$FRONTEND_REPO" "$FRONTEND_DIR"
else
  git -C "$FRONTEND_DIR" fetch origin
  FRONTEND_BRANCH="$(git -C "$FRONTEND_DIR" rev-parse --abbrev-ref HEAD)"
  git -C "$FRONTEND_DIR" pull --ff-only origin "$FRONTEND_BRANCH"
fi

if [ ! -d "$BACKEND_DIR/.git" ]; then
  git clone "$BACKEND_REPO" "$BACKEND_DIR"
else
  git -C "$BACKEND_DIR" fetch origin
  BACKEND_BRANCH="$(git -C "$BACKEND_DIR" rev-parse --abbrev-ref HEAD)"
  git -C "$BACKEND_DIR" pull --ff-only origin "$BACKEND_BRANCH"
fi

echo "[4/8] Installing matching .NET SDK channel..."
mkdir -p "$DOTNET_ROOT"
GLOBAL_JSON="$BACKEND_DIR/global.json"
if [ ! -f "$GLOBAL_JSON" ]; then
  echo "Missing $GLOBAL_JSON" >&2
  exit 1
fi

DOTNET_CHANNEL="$(
  python3 - <<'PY' "$GLOBAL_JSON"
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)
version = data.get("sdk", {}).get("version", "")
parts = version.split(".")
if len(parts) >= 2:
    print(f"{parts[0]}.{parts[1]}")
else:
    print("10.0")
PY
)"

if [ ! -x "$DOTNET_BIN" ] || ! "$DOTNET_BIN" --list-sdks 2>/dev/null | grep -q "^${DOTNET_CHANNEL}\."; then
  INSTALL_SCRIPT="$BASE_DIR/dotnet-install.sh"
  wget -q https://dot.net/v1/dotnet-install.sh -O "$INSTALL_SCRIPT"
  chmod +x "$INSTALL_SCRIPT"
  "$INSTALL_SCRIPT" --channel "$DOTNET_CHANNEL"
fi

export DOTNET_ROOT
export PATH="$DOTNET_ROOT:$DOTNET_ROOT/tools:$PATH"
"$DOTNET_BIN" --version

echo "[5/8] Building custom Jellyfin frontend..."
cd "$FRONTEND_DIR"
npm ci
npm run build:production
test -f "$FRONTEND_DIR/dist/index.html"

echo "[6/8] Preparing runtime directories..."
sudo mkdir -p /mnt/block/movies
sudo chown -R "$USER":"$USER" /mnt/block/movies
mkdir -p "$HOME/.config/jellyfin" "$HOME/.local/share/jellyfin" "$HOME/.cache/jellyfin"

echo "[7/8] Installing systemd service..."
SERVICE_FILE="$BASE_DIR/${SERVICE_NAME}.service"
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Custom Jellyfin (official backend + JF-frontend)
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$BACKEND_DIR
Environment=DOTNET_ROOT=$DOTNET_ROOT
Environment=PATH=$DOTNET_ROOT:$DOTNET_ROOT/tools:/usr/local/bin:/usr/bin:/bin
ExecStart=$DOTNET_BIN run --project Jellyfin.Server -- -w $FRONTEND_DIR/dist
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo cp "$SERVICE_FILE" "/etc/systemd/system/${SERVICE_NAME}.service"
sudo systemctl daemon-reload

echo "[7.5/8] Stopping older manual Jellyfin processes if they are still running..."
pkill -f "$HOME/jellyfin/Jellyfin.Server/bin/Debug/net10.0/jellyfin" >/dev/null 2>&1 || true
pkill -f "$DOTNET_BIN run --project Jellyfin.Server -- -w $HOME/jellyfin-web/dist" >/dev/null 2>&1 || true

sudo systemctl enable --now "${SERVICE_NAME}.service"

echo "[8/8] Verifying service..."
sleep 8
sudo systemctl --no-pager --full status "${SERVICE_NAME}.service" | sed -n '1,30p'
curl -I http://127.0.0.1:8096 || true

echo
echo "Custom Jellyfin has been deployed from:"
echo "  Frontend: $FRONTEND_REPO"
echo "  Backend : $BACKEND_REPO"
echo "Managed source directories:"
echo "  $FRONTEND_DIR"
echo "  $BACKEND_DIR"
echo "Service name:"
echo "  ${SERVICE_NAME}.service"
echo "Expected external URL:"
echo "  http://<server-ip>:8096/web/#/home"
