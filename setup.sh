#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Update pip ---
echo "Updating pip..."
python3 -m pip install --upgrade pip

# --- Create virtual environment ---
if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv venv
else
  echo "Virtual environment 'venv' already exists."
fi


# --- Activate virtual environment and install dependencies ---
echo "Activating virtual environment and installing dependencies..."
source venv/bin/activate
pip install -r requirements.txt

echo "Setup complete. Virtual environment 'venv' is ready and dependencies are installed."
echo "To activate the virtual environment in your current shell, run: source venv/bin/activate" 