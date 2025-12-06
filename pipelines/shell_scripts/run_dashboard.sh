#!/bin/bash
echo "=============================================="
echo "   STARTING STREAMLIT DASHBOARDS"
echo "=============================================="

# Activate virtual environment
if [ -d "../epl_venv" ]; then
  # shellcheck source=/dev/null
  source ../epl_venv/bin/activate
fi

# Start Retailer Dashboard on port 8501
echo "Starting Retailer Dashboard on port 8501..."
streamlit run retail_dashboard.py --server.port=8501 &

# Start Fan Store Dashboard on port 8502
echo "Starting Fan Store on port 8502..."
streamlit run fan_store.py --server.port=8502 &

echo "=============================================="
echo "Dashboards Running:"
echo " Retailer Dashboard → http://localhost:8501"
echo " Fan Storefront     → http://localhost:8502"
echo "=============================================="

