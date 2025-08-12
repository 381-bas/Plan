﻿# B74: ImportaciÃ³n de mÃ³dulos SCANNER (indexador y simulador)
# âˆ‚Báµ¢/âˆ‚Bâ±¼
from scanner_indexador_molecular import aplicar_indexador_en_directorio
from simulador_mutacional import (
    extraer_bloques_y_derivadas,
    simular_remocion_bloque,
    diagnostico_mutacional,
)
import os
from pathlib import Path

# B75: ConfiguraciÃ³n de ruta base de escaneo
# âˆ‚Báµ¢/âˆ‚Bâ±¼
RUTA_PLAN_UNIFICADO = os.getenv(
    "SYMBIOS_PLAN_PATH", str(Path(__file__).resolve().parent)  # default local
)


# B76: Indexador global sobre todos los .py del sistema
# âˆ‚Báµ¢/âˆ‚Bâ±¼
def ejecutar_indexador_global():
    print("ðŸš€ Iniciando scanner_indexador_molecular() sobre:", RUTA_PLAN_UNIFICADO)
    aplicar_indexador_en_directorio(RUTA_PLAN_UNIFICADO)


# B77: EjecuciÃ³n de simulaciÃ³n mutacional por bloque
# âˆ‚Báµ¢/âˆ‚Bâ±¼
def ejecutar_simulacion_mutacional(bloque: str):
    for root, _, files in os.walk(RUTA_PLAN_UNIFICADO):
        for file in files:
            if file.endswith(".py"):
                ruta = os.path.join(root, file)
                with open(ruta, "r", encoding="utf-8") as f:
                    contenido = f.read()
                estructura = extraer_bloques_y_derivadas(contenido)
                afectados = simular_remocion_bloque(bloque, estructura)
                print(f"ðŸ“‚ Archivo: {file}")
                diagnostico_mutacional(bloque, afectados)
                print("\n")


# B78: EjecuciÃ³n principal del sistema SCANNER
# âˆ‚Báµ¢/âˆ‚Bâ±¼
if __name__ == "__main__":
    ejecutar_indexador_global()
    # Ejemplo de simulaciÃ³n:
    ejecutar_simulacion_mutacional("B6")  # Puedes cambiar a cualquier BLOQUE
