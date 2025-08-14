@echo off
setlocal enabledelayedexpansion
set REPO=%USERPROFILE%\Documents\Plan_qmk
set BASE=main
set BR=%1

if "%BR%"=="" (
  echo Uso: start_work.bat chore\mi-rama
  exit /b 1
)

cd "%REPO%" || exit /b 1
echo [1/4] Fetch remotos...
git fetch --all --prune || exit /b 1

echo [2/4] Cambiando a %BASE% ...
git switch %BASE% || exit /b 1

echo [3/4] Actualizando %BASE% (ff-only)...
git pull --ff-only origin %BASE% || (echo [ERROR] Pull no fast-forward. Revisa conflictos. & exit /b 1)

echo [4/4] Creando/cambiando a la rama %BR% ...
git switch -c %BR% || git switch %BR% || exit /b 1

echo [OK] Rama activa: %BR%
endlocal
