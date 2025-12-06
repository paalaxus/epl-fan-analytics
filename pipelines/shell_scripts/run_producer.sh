#!/bin/bash
echo "=============================================="
echo "   STARTING PRODUCER"
echo "=============================================="

# Activate venv if present
if [ -d "../epl_venv" ]; then
  # shellcheck source=/dev/null
  source ../epl_venv/bin/activate
fi
docker compose up -d kafka
python3 new_producer.py
