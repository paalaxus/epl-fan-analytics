#!/bin/bash
echo "=============================================="
echo "   STARTING STREAMLIT DASHBOARD"
echo "=============================================="

if [ -d "../epl_venv" ]; then
  # shellcheck source=/dev/null
  source ../epl_venv/bin/activate
fi

streamlit run realtime_dashboard.py --server.port=8502
