#!/usr/bin/env bash
# GCE startup script for the producer VM (Debian 12). Runs on EVERY boot, so
# `make up` (which starts the VM) always deploys the latest producer code.
#
# Configuration comes from instance metadata set by Terraform:
#   producer-code-uri   gs://<bucket>/producer
#   pubsub-project      project id
#   pubsub-topic        topic id
#   symbols             e.g. btcusdt,ethusdt
#   streams             e.g. trade
set -euo pipefail

APP_DIR=/opt/producer
APP_USER=producer
MD_URL="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
md() { curl -sf -H "Metadata-Flavor: Google" "${MD_URL}/$1"; }

CODE_URI="$(md producer-code-uri)"
PROJECT="$(md pubsub-project)"
TOPIC="$(md pubsub-topic)"
SYMBOLS="$(md symbols || echo btcusdt,ethusdt)"
STREAMS="$(md streams || echo trade)"

echo "[startup] deploying producer from ${CODE_URI}"

# e2-micro has 1 GB RAM: add a small swap file once, so pip installs never OOM.
if [[ ! -f /swapfile ]]; then
  fallocate -l 1G /swapfile && chmod 600 /swapfile && mkswap /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
swapon -a || true

# Check the package itself: Debian ships the venv module without ensurepip,
# so "python3 -m venv --help" succeeds even when venv creation would fail.
if ! dpkg -s python3-venv >/dev/null 2>&1; then
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-venv
fi
id -u "${APP_USER}" >/dev/null 2>&1 || useradd --system --home-dir "${APP_DIR}" --shell /usr/sbin/nologin "${APP_USER}"

mkdir -p "${APP_DIR}"
gcloud storage cp "${CODE_URI}/producer.py" "${CODE_URI}/requirements.txt" "${APP_DIR}/"

# Re-create the venv only when requirements change (keeps boots fast).
REQ_HASH="$(sha256sum "${APP_DIR}/requirements.txt" | cut -d' ' -f1)"
if [[ ! -f "${APP_DIR}/venv/.req-${REQ_HASH}" ]]; then
  rm -rf "${APP_DIR}/venv"
  python3 -m venv "${APP_DIR}/venv"
  "${APP_DIR}/venv/bin/pip" install --quiet --upgrade pip
  "${APP_DIR}/venv/bin/pip" install --quiet -r "${APP_DIR}/requirements.txt"
  touch "${APP_DIR}/venv/.req-${REQ_HASH}"
fi
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"

cat > /etc/default/producer <<ENV
GCP_PROJECT=${PROJECT}
PUBSUB_TOPIC=${TOPIC}
SYMBOLS=${SYMBOLS}
STREAMS=${STREAMS}
LOG_TO_CLOUD=true
PYTHONUNBUFFERED=1
ENV

cat > /etc/systemd/system/producer.service <<'UNIT'
[Unit]
Description=Binance WebSocket -> Pub/Sub producer
Wants=network-online.target
After=network-online.target

[Service]
User=producer
EnvironmentFile=/etc/default/producer
ExecStart=/opt/producer/venv/bin/python /opt/producer/producer.py
Restart=always
RestartSec=5
# Let the producer flush pending Pub/Sub batches on stop.
KillSignal=SIGTERM
TimeoutStopSec=30
MemoryMax=600M

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
# This script runs on every boot and starts the service itself. If the unit were
# enabled, systemd would also start it early at boot (before the network is ready)
# and the restart below would kill that first process, losing a few seconds of trades.
systemctl disable producer.service
systemctl restart producer.service
echo "[startup] producer running"
