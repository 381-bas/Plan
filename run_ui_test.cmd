@echo off
REM run_ui_test.cmd — Valida entorno completo + hooks + smoke test extendido antes de lanzar UI

setlocal

REM === 0) Parámetros del proyecto ===
set "PROJ=%USERPROFILE%\Documents\Plan_qmk"
set "PY=%PROJ%\.venv\Scripts\python.exe"
set "PC=%PROJ%\.venv\Scripts\pre-commit.exe"

REM === 1) Consola UTF‑8 (solo para CMD) ===
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

REM === 2) Validación de entorno ===
if not exist "%PROJ%\" (
  echo [ERROR] No existe la carpeta del proyecto: %PROJ%
  exit /b 1
)
if not exist "%PY%" (
  echo [ERROR] No se encontro Python del venv: %PY%
  exit /b 1
)
if not exist "%PC%" (
  echo [WARN ] pre-commit.exe no encontrado. Saltando hooks...
  goto smoke
)

pushd "%PROJ%"

REM === 3) Ejecutar hooks pre-commit ===
echo [INFO ] Ejecutando hooks...
"%PC%" install >nul
"%PC%" run -a
if errorlevel 1 (
  echo [ERROR] Fallaron los hooks pre-commit. Corrige antes de continuar.
  exit /b 1
)

:smoke
REM === 4) Smoke test extendido ===
echo [INFO ] Smoke test extendido...
"%PY%" -c "import sys, importlib; sys.path.insert(0, r'%PROJ%'); import main, services.forecast_engine, utils.alertas; from utils.alertas import _mes; print('- _mes:', _mes('2024-08-15'), repr(_mes(None)))"


if errorlevel 1 (
  echo [ERROR] Smoke test falló. Revisa errores de importación o dependencias.
  exit /b 1
)

REM === 5) Validaciones adicionales ===
if not exist "%PROJ%\main.py" (
  echo [ERROR] No se encuentra main.py en %PROJ%
  exit /b 1
)
if not exist "%PROJ%\.pre-commit-config.yaml" (
  echo [WARN ] Faltante: .pre-commit-config.yaml
)

REM === 6) Lanzar UI (Streamlit) ===
echo [INFO ] Lanzando Streamlit UI...
"%PY%" -m streamlit run "%PROJ%\main.py"

popd
exit /b %errorlevel%
