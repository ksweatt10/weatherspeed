#!/usr/bin/env bash
# WeatherSpeed Bot — VPS deployment script
# Usage: ./deploy.sh
# Never edit files directly on VPS; always deploy via this script.

set -euo pipefail

VPS_USER="ubuntu"
VPS_HOST="${VPS_HOST:-}"   # set env var or edit below
VPS_DIR="/home/ubuntu/weatherspeed"
REPO="https://github.com/ksweatt10/weatherspeed.git"
SERVICE="weatherspeed"

if [[ -z "$VPS_HOST" ]]; then
  echo "ERROR: set VPS_HOST env var first.  e.g.  VPS_HOST=1.2.3.4 ./deploy.sh"
  exit 1
fi

echo "==> Deploying WeatherSpeed to $VPS_USER@$VPS_HOST:$VPS_DIR"

# Push local commits to GitHub first
echo "==> Pushing to GitHub..."
git push

# SSH into VPS and pull + restart
ssh "$VPS_USER@$VPS_HOST" bash <<REMOTE
  set -euo pipefail

  # Clone on first deploy, pull on subsequent
  if [ ! -d "$VPS_DIR/.git" ]; then
    echo "--- First deploy: cloning repo"
    git clone $REPO $VPS_DIR
  else
    echo "--- Pulling latest"
    cd $VPS_DIR && git pull --ff-only
  fi

  cd $VPS_DIR

  # Install/update dependencies in venv
  if [ ! -d venv ]; then
    python3 -m venv venv
  fi
  venv/bin/pip install -q --upgrade pip
  venv/bin/pip install -q -r requirements.txt

  # Reload systemd service
  sudo systemctl daemon-reload
  sudo systemctl restart $SERVICE
  sudo systemctl enable  $SERVICE
  echo "--- Service status:"
  sudo systemctl status  $SERVICE --no-pager -l | head -20
REMOTE

echo "==> Deploy complete!"
echo "    Dashboard: http://$VPS_HOST:8002"
