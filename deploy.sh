#!/usr/bin/env bash
# Deploy latest code to GitHub and VPS, then restart the bot.
# Usage: ./deploy.sh
# Requires: sshpass (brew install sshpass)

set -e

VPS_HOST="172.93.191.139"
VPS_USER="root"
VPS_PASS="JourneyExample93_"
VPS_DIR="/root/weatherspeed"

echo "=== Pushing to GitHub ==="
git push origin main

echo ""
echo "=== Deploying to VPS ==="
sshpass -p "$VPS_PASS" ssh -o StrictHostKeyChecking=no "$VPS_USER@$VPS_HOST" "
  set -e
  cd $VPS_DIR

  echo '--- Pulling latest code ---'
  git fetch origin
  git reset --hard origin/main

  echo '--- Installing dependencies ---'
  venv/bin/pip install -q -r requirements.txt

  echo '--- Restarting bot ---'
  systemctl restart weatherspeed
  sleep 5
  systemctl is-active weatherspeed && echo 'Bot is running.' || echo 'WARNING: bot failed to start!'

  echo ''
  echo '--- Startup log ---'
  tail -20 /tmp/weatherspeed.log
"

echo ""
echo "=== Done ==="
echo "    Dashboard: http://$VPS_HOST:8002"
echo "    Logs:      ssh root@$VPS_HOST 'tail -f /tmp/weatherspeed.log'"
