@echo off
setlocal

set "ROOT=%~dp0"
set "PYTHON_EXE=%ROOT%.venv\Scripts\python.exe"

if not exist "%PYTHON_EXE%" (
  echo Missing virtual environment: "%PYTHON_EXE%"
  echo.
  echo Create it with:
  echo   python -m venv .venv
  echo   .venv\Scripts\python.exe -m pip install -e .[dev]
  exit /b 1
)

pushd "%ROOT%" >nul
"%PYTHON_EXE%" -c "import grasp" >nul 2>nul
if errorlevel 1 (
  echo Installing local package into the existing virtual environment...
  "%PYTHON_EXE%" -m pip install -e .[dev]
  if errorlevel 1 (
    popd >nul
    exit /b 1
  )
)

"%PYTHON_EXE%" -m grasp
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul
exit /b %EXIT_CODE%

