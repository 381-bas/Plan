# SYMBIOS — Plan_Forecast

Código y configuración (sin datos). Guardrails: B0/C1/C2/C4.

## Requisitos
- Python 3.12
- Git

## Setup (Windows PowerShell)
    cd "$env:USERPROFILE\Documents\Plan_qmk"
    py -3.12 -m venv .venv
    .\.venv\Scripts\Activate.ps1  # o: .\.venv\Scripts\activate.bat
    pip install --upgrade pip
    pip install -r requirements.txt
    copy .env.example .env

## Variables de entorno (archivo .env)
- SYMBIOS_ROOT: raíz del repo (.)
- SYMBIOS_DATA: carpeta de datos no versionados (./Informacion)
- SYMBIOS_TEMP: temporales/backups (./temp_ediciones)
- SYMBIOS_LOGS: logs (./utils/logs)
- BACKUP_FMT: parquet | pkl

## Comandos útiles
- Risk-Lint:    `python symbios_risklint.py`  → espera 0-0-0-0
- Scanner:      `python symbios_local_scanner.py --root . --out .\\scans --no-stamp`
- Ejecutar app: `python main.py`

## CI (GitHub Actions)
Workflow **symbios-ci**: instala deps, corre Risk-Lint y pytest (si hay tests).

## Política de ramas
- main: estable (CI verde, protegido).
- feature/*: trabajo; abrir PR a main.

## Mensajería C2 (sugerida)
`feat(x): descripción [C2:TAG123]` · `fix(y): descripción [C2:BUG045]`

## No versionar
Ver `.gitignore`: datos, logs, temp, scans/, __pycache__, etc.
