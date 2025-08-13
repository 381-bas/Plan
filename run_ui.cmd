@echo off
set "PROJ=%USERPROFILE%\Documents\Plan_qmk"
set "PY=%PROJ%\.venv\Scripts\python.exe"
if not exist "%PY%" (
  echo [ERROR] No existe %PY%  && pause && exit /b 1
)
pushd "%PROJ%"
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
start "Plan UI" /D "%PROJ%" "%PY%" -m streamlit run main.py
