#!/bin/bash

#############################################
# CONFIG
#############################################

VENV_PATH="epl_consumer_env"
STREAMLIT_APP="pipelines/new_dashboard.py"
STREAMLIT_PORT=8501
NGROK_REGION="us"   # change to eu/ap if needed


#############################################
# STARTUP
#############################################

echo "=============================================="
echo "   EPL Fan Dashboard + Ngrok Public Hosting   "
echo "=============================================="
echo ""

# ---- Step 1: Activate virtual environment ----
if [ -d "$VENV_PATH" ]; then
    echo "[1/4] Activating virtual environment..."
    source "$VENV_PATH/bin/activate"
else
    echo "❌ Virtual environment not found at: $VENV_PATH"
    exit 1
fi

# ---- Step 2: Launch Streamlit in background ----
echo "[2/4] Starting Streamlit dashboard on port $STREAMLIT_PORT..."
streamlit run "$STREAMLIT_APP" --server.port $STREAMLIT_PORT > streamlit.log 2>&1 &

STREAMLIT_PID=$!
echo "    Streamlit PID: $STREAMLIT_PID"
sleep 3

# ---- Step 3: Start Ngrok tunnel ----
echo "[3/4] Starting ngrok tunnel..."
ngrok http --region=$NGROK_REGION $STREAMLIT_PORT > ngrok.log 2>&1 &

NGROK_PID=$!
sleep 3

# ---- Step 4: Print public URL ----
echo "[4/4] Fetching public URL..."
PUBLIC_URL=$(curl -s localhost:4040/api/tunnels | grep -o "https://[^\"]*")

if [ -z "$PUBLIC_URL" ]; then
    echo "❌ Failed to get public URL. Check ngrok.log"
else
    echo "=============================================="
    echo "   🎉 Your Dashboard is LIVE on the Internet 🎉"
    echo "----------------------------------------------"
    echo "  🌍  PUBLIC URL: $PUBLIC_URL"
    echo "----------------------------------------------"
    echo "  Send this link to your teammate!"
    echo "=============================================="
fi

# Keep the script alive
wait

