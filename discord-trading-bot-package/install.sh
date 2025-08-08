#!/bin/bash

echo "Discord Trading Bot - One-Click Installer"
echo "========================================"
echo

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed"
    echo "Please install Python 3.11 or higher"
    exit 1
fi

echo "Python found, checking version..."
python3 -c "import sys; print(f'Python {sys.version}'); sys.exit(0 if sys.version_info >= (3,11) else 1)"
if [ $? -ne 0 ]; then
    echo "ERROR: Python 3.11 or higher is required"
    exit 1
fi

echo
echo "Installing dependencies..."
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

if [ $? -ne 0 ]; then
    echo
    echo "ERROR: Failed to install dependencies"
    exit 1
fi

echo
echo "========================================"
echo "Installation completed successfully!"
echo "========================================"
echo
echo "Next steps:"
echo "1. Set up your environment variables (see DEPLOYMENT_INSTRUCTIONS.md)"
echo "2. Run: python3 main.py"
echo
echo "For deployment to Render, see DEPLOYMENT_INSTRUCTIONS.md"
echo