@echo off
REM Build a single-file Windows executable for the Companies House lookup tool.
REM Run this from the project directory in a Developer Command Prompt or PowerShell.

echo Creating virtual environment (if not already present)...
if not exist ".venv" (
    python -m venv .venv
)

call .venv\Scripts\activate.bat

echo Installing dependencies...
pip install --upgrade pip
pip install -r requirements.txt
pip install pyinstaller

echo Building EXE with PyInstaller...
pyinstaller --noconsole --onefile gus_trace_tool.py

echo.
echo Build complete. Executable should be in the dist\ folder as gus_trace_tool.exe.
echo Remember: do not distribute an EXE that contains a real Companies House API key.
pause


