#!/usr/bin/env bash
set -e
cd /home/ubuntu/Bigdata-NASA-Eonet-Pipeline

git pull origin main
COMMIT_HASH=$(git rev-parse --short HEAD)
COMMIT_MSG=$(git log -1 --pretty=%s)

sudo systemctl restart eonet-producer eonet-stream

/home/ubuntu/Bigdata-NASA-Eonet-Pipeline/venv/bin/python \
  /home/ubuntu/Bigdata-NASA-Eonet-Pipeline/setup/scripts/log_lifecycle_event.py \
  --type DEPLOYMENT \
  --component "git-${COMMIT_HASH}" \
  --title "Deployed commit ${COMMIT_HASH}" \
  --details "${COMMIT_MSG}" \
  --tags "deploy,git"
