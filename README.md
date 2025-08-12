# SYMBIOS — Plan_Forecast (bootstrap)

Repositorio con código y configuración (sin datos). Guardrails: B0/C1/C2/C4.

## Setup rápido (Windows PowerShell)
    cd "$env:USERPROFILE\Documents\Plan_qmk"
    py -3.12 -m venv .venv
    .venv\Scripts\Activate.ps1
    pip install --upgrade pip
    pip install -r requirements.txt
    python symbios_risklint.py

## Ejecutar la app
    python main.py

## CI
Workflow symbios-ci: instala deps, corre Risk-Lint y pytest (si hay tests).
