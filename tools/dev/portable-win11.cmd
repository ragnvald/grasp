@echo off
setlocal

set "TOOLS_DIR=%~dp0"
for %%I in ("%TOOLS_DIR%..\..") do set "ROOT=%%~fI\"
set "PYTHON_EXE=%ROOT%.venv\Scripts\python.exe"

if not exist "%PYTHON_EXE%" (
  echo Missing virtual environment: "%PYTHON_EXE%"
  echo.
  echo Create it with:
  echo   python -m venv .venv
  echo   .venv\Scripts\python.exe -m pip install -e .[dev,build]
  exit /b 1
)

pushd "%ROOT%" >nul
"%PYTHON_EXE%" -c "import PyInstaller" >nul 2>nul
if errorlevel 1 (
  echo Installing build dependencies into the virtual environment...
  "%PYTHON_EXE%" -m pip install -e .[build]
  if errorlevel 1 (
    popd >nul
    exit /b 1
  )
)

"%PYTHON_EXE%" "%ROOT%tools\dev\build_pyinstaller.py" --portable-win11
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul
exit /b %EXIT_CODE%
