#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_NAME="the-sipae-api"
RUNTIME_ENV="${APP_DIR}/.runtime.env"
PORT_RANGE_START=18100
PORT_RANGE_END=18149

echo "========================================"
echo "Deploying ${APP_NAME}"
echo "========================================"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

port_in_use() {
  local port="$1"
  ss -ltnH "( sport = :${port} )" 2>/dev/null | grep -q .
}

pick_first_free_port() {
  local port
  for port in $(seq "${PORT_RANGE_START}" "${PORT_RANGE_END}"); do
    if ! port_in_use "${port}"; then
      echo "${port}"
      return 0
    fi
  done

  echo "No free port found in reserved range ${PORT_RANGE_START}-${PORT_RANGE_END}" >&2
  exit 1
}

require_cmd git
require_cmd python3
require_cmd pm2
require_cmd ss

cd "${APP_DIR}"

if [ ! -f ".env" ]; then
  echo "Missing ${APP_DIR}/.env" >&2
  exit 1
fi

if [ ! -f "${RUNTIME_ENV}" ]; then
  PORT="$(pick_first_free_port)"
  cat > "${RUNTIME_ENV}" <<EOF
PM2_APP_NAME=${APP_NAME}
PORT=${PORT}
EOF
  echo "Assigned port ${PORT} and wrote ${RUNTIME_ENV}"
fi

set -a
source "${RUNTIME_ENV}"
set +a

if [ -z "${PM2_APP_NAME:-}" ] || [ -z "${PORT:-}" ]; then
  echo "Invalid ${RUNTIME_ENV}" >&2
  exit 1
fi

if port_in_use "${PORT}"; then
  if ! pm2 describe "${PM2_APP_NAME}" >/dev/null 2>&1; then
    echo "Configured port ${PORT} is already in use by another process." >&2
    exit 1
  fi
fi

git pull origin main

if [ ! -d "venv" ]; then
  python3 -m venv venv
fi

./venv/bin/python -m pip install --upgrade pip
./venv/bin/pip install -r requirements.txt

if pm2 describe "${PM2_APP_NAME}" >/dev/null 2>&1; then
  pm2 restart ecosystem.config.js --only "${PM2_APP_NAME}" --update-env
else
  pm2 start ecosystem.config.js --only "${PM2_APP_NAME}" --update-env
fi

echo
echo "APP_NAME=${PM2_APP_NAME}"
echo "PORT=${PORT}"
echo "LOCAL_URL=http://127.0.0.1:${PORT}"
echo
pm2 status "${PM2_APP_NAME}"