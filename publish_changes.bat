@echo off
setlocal EnableExtensions

rem === Rama actual ===
for /f "delims=" %%b in ('git rev-parse --abbrev-ref HEAD') do set "CUR=%%b"

rem === Bloquea si estas en main ===
if /I "%CUR%"=="main" (
  echo [ERROR] Estas en main. Usa: start_work.bat chore\mi-rama
  exit /b 1
)

rem === Mensaje completo (soporta espacios y parentesis) ===
if "%~1"=="" goto NOMSG
set "MSG=%*"

echo [pre-commit] Ejecutando hooks...
if exist ".venv\Scripts\pre-commit.exe" goto HAS_PC
echo (aviso) pre-commit no encontrado; continuando...
goto AFTER_PC

:HAS_PC
call ".\.venv\Scripts\pre-commit.exe" run -a
if errorlevel 1 (
  echo [pre-commit] Hooks reescribieron archivos - reintentando
  call ".\.venv\Scripts\pre-commit.exe" run -a
  if errorlevel 1 goto FAIL
)

:AFTER_PC
echo [status] Archivos cambiados:
git status

echo [add] Agregando cambios...
git add -A || goto FAIL

rem === Escribir mensaje en archivo temporal (evita parsing de CMD) ===
set "TMPMSG=%TEMP%\commit_msg_%RANDOM%.txt"
powershell -NoProfile -Command "$m=$env:MSG; Set-Content -LiteralPath $env:TMPMSG -Value $m -Encoding UTF8"
if errorlevel 1 goto FAIL

echo [commit] Creando commit con -F "%TMPMSG%" ...
git commit -F "%TMPMSG%"
if errorlevel 1 (
  echo [INFO] Nada que commitear. ?Guardaste los cambios?
  del "%TMPMSG%" >nul 2>&1
  exit /b 1
)
del "%TMPMSG%" >nul 2>&1

echo [push] Subiendo rama %CUR%...
git push -u origin %CUR% || goto FAIL

echo [OK] Abriendo PR en tu navegador...
start "" "https://github.com/381-bas/Plan/compare/main...%CUR%"
echo [OK] Si no se abre, copia esta URL:
echo https://github.com/381-bas/Plan/compare/main...%CUR%

exit /b 0

:NOMSG
echo Uso: publish_changes.bat "tipo(scope): mensaje"
exit /b 1

:FAIL
echo [ERROR] Proceso fallo. Revisa mensajes arriba.
exit /b 1

