#!/bin/bash

# Path to venv
VENV_PATH="$HOME/Downloads/EPL_Pipeline/pipelines/epl_consumer_env"

# Create venv if it doesn't exist
if [ ! -d "$VENV_PATH" ]; then
    python3 -m venv "$VENV_PATH"
fi

# Activate the venv
source "$VENV_PATH/bin/activate"

echo "Virtual environment activated: $VENV_PATH"

