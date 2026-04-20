#!/usr/bin/env bash
set -e

APP_NAME="the-sipae-api"
APP_DIR="/var/www/the-sipae-api"
BRANCH="main"

echo "========================================"
echo "Deploying ${APP_NAME}"
echo "========================================"

cd "$APP_DIR"

echo "1. Pulling latest code..."
git pull origin "$BRANCH"

echo "2. Creating venv if missing..."
if [ ! -d "venv" ]; then
  python3 -m venv venv
fi

echo "3. Installing Python dependencies..."
source venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
deactivate

echo "4. Starting or restarting PM2 app..."
if pm2 describe "$APP_NAME" >/dev/null 2>&1; then
  pm2 restart "$APP_NAME" --update-env
else
  pm2 start ecosystem.config.js --only "$APP_NAME" --update-env
fi

echo "5. Saving PM2 process list..."
pm2 save

echo "========================================"
echo "Deploy complete"
echo "========================================"