@echo off
echo Discord Trading Bot - One-Click Installer
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.11 or higher from https://python.org
    pause
    exit /b 1
)

echo Python found, checking version...
python -c "import sys; print(f'Python {sys.version}'); sys.exit(0 if sys.version_info >= (3,11) else 1)"
if errorlevel 1 (
    echo ERROR: Python 3.11 or higher is required
    pause
    exit /b 1
)

echo.
echo Installing dependencies...
pip install --upgrade pip
pip install -r requirements.txt

if errorlevel 1 (
    echo.
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo ========================================
echo Installation completed successfully!
echo ========================================
echo.
echo Next steps:
echo 1. Set up your environment variables (see DEPLOYMENT_INSTRUCTIONS.md)
echo 2. Run: python main.py
echo.
echo For deployment to Render, see DEPLOYMENT_INSTRUCTIONS.md
echo.
pause