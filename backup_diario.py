﻿import utils.pickle_adapter  # noqa: F401  # habilita Parquet si BACKUP_FMT=parquet

# B_BCK001: Importaciones principales para backup y manejo de archivos
# # âˆ‚B_BCK001/âˆ‚B0
import os
import shutil
from datetime import datetime, timedelta

# B_BCK002: ConfiguraciÃ³n de rutas y archivos para backup diario
# # âˆ‚B_BCK002/âˆ‚B0
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_ORIGINAL = os.path.join(BASE_DIR, "Plan_Final_Final_Final.db")
CARPETA_BACKUPS = os.path.join(BASE_DIR, "backups")

# B_BCK003: CreaciÃ³n de carpeta de backups si no existe
# # âˆ‚B_BCK003/âˆ‚B0
os.makedirs(CARPETA_BACKUPS, exist_ok=True)

# B_BCK004: GeneraciÃ³n de nombre de archivo de backup diario con fecha
# # âˆ‚B_BCK004/âˆ‚B0
fecha_hoy = datetime.now().strftime("%Y%m%d")
nombre_backup = f"backup_{fecha_hoy}.db"
ruta_backup = os.path.join(CARPETA_BACKUPS, nombre_backup)

# B_BCK005: Copia del archivo original y registro de Ã©xito/error
# # âˆ‚B_BCK005/âˆ‚B0
try:
    shutil.copy2(DB_ORIGINAL, ruta_backup)
    print(f"âœ… Backup creado: {ruta_backup}")
except Exception as e:
    print(f"âŒ Error al crear backup: {e}")

# B_BCK006: RotaciÃ³n automÃ¡tica y eliminaciÃ³n de backups antiguos (>7 dÃ­as)
# # âˆ‚B_BCK006/âˆ‚B0
dias_retencion = 7
limite_fecha = datetime.now() - timedelta(days=dias_retencion)

for archivo in os.listdir(CARPETA_BACKUPS):
    if archivo.startswith("backup_") and archivo.endswith(".db"):
        fecha_str = archivo.replace("backup_", "").replace(".db", "")
        try:
            fecha_archivo = datetime.strptime(fecha_str, "%Y%m%d")
            if fecha_archivo < limite_fecha:
                ruta_eliminar = os.path.join(CARPETA_BACKUPS, archivo)
                os.remove(ruta_eliminar)
                print(f"ðŸ—‘ï¸ Backup eliminado por antigÃ¼edad: {archivo}")
        except ValueError:
            print(f"âš ï¸ Archivo ignorado por formato incorrecto: {archivo}")
