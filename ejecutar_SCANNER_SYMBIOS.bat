@echo off
cd /d "C:\Users\qmkbantiman\OneDrive - QMK SPA\GG\Python\Plan_Forecast"
py ejecutor_universal_scanner.py
if %errorlevel% neq 0 (
    echo ? Ocurrió un error durante el escaneo SCANNER.
    pause
) else (
    echo ? SCANNER ejecutado con éxito.
    pause
)
