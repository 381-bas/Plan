@echo off
setlocal

rem Rama actual
for /f "delims=" %%b in ('git rev-parse --abbrev-ref HEAD') do set CUR=%%b

rem Bloquea si estás en main
if "%CUR%"=="main" (
  echo [ERROR] Estas en main. Usa: start_work.bat chore\mi-rama
  exit /b 1
)

rem Captura el mensaje completo (con espacios y parentesis) de forma segura
set "MSG=%*"
if "%MSG%"=="" (
  echo Uso: publish_changes.bat "tipo(scope): mensaje"
  exit /b 1
)

echo [pre-commit] Ejecutando hooks...
if exist ".venv\Scripts\pre-commit.exe" (
  .\.venv\Scripts\pre-commit.exe run -a
  if errorlevel 1 (
    echo [pre-commit] Hooks reescribieron archivos (p. ej. Black). Reintentando...
    .\.venv\Scripts\pre-commit.exe run -a || exit /b 1
  )
) else (
  echo (aviso) pre-commit no encontrado; continuando...
)

echo [status] Archivos cambiados:
git status

echo [add] Agregando cambios...
git add -A || exit /b 1

echo [commit] Creando commit...
git commit -m "%MSG%"
if errorlevel 1 (
  echo [INFO] Nada que commitear. ¿Guardaste los cambios?
  exit /b 1
)

echo [push] Subiendo rama %CUR%...
git push -u origin %CUR% || exit /b 1

echo [OK] Abre tu PR: https://github.com/381-bas/Plan/pull/new/%CUR%
endlocal
