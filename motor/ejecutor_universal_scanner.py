# B74: Importación de módulos SCANNER (indexador y simulador)
# ∂Bᵢ/∂Bⱼ
from scanner_indexador_molecular import aplicar_indexador_en_directorio
from simulador_mutacional import (
    extraer_bloques_y_derivadas,
    simular_remocion_bloque,
    diagnostico_mutacional,
)
import os
from pathlib import Path

# B75: Configuración de ruta base de escaneo
# ∂Bᵢ/∂Bⱼ
RUTA_PLAN_UNIFICADO = os.getenv(
    "SYMBIOS_PLAN_PATH", str(Path(__file__).resolve().parent)  # default local
)


# B76: Indexador global sobre todos los .py del sistema
# ∂Bᵢ/∂Bⱼ
def ejecutar_indexador_global():
    print("🚀 Iniciando scanner_indexador_molecular() sobre:", RUTA_PLAN_UNIFICADO)
    aplicar_indexador_en_directorio(RUTA_PLAN_UNIFICADO)


# B77: Ejecución de simulación mutacional por bloque
# ∂Bᵢ/∂Bⱼ
def ejecutar_simulacion_mutacional(bloque: str):
    for root, _, files in os.walk(RUTA_PLAN_UNIFICADO):
        for file in files:
            if file.endswith(".py"):
                ruta = os.path.join(root, file)
                with open(ruta, "r", encoding="utf-8") as f:
                    contenido = f.read()
                estructura = extraer_bloques_y_derivadas(contenido)
                afectados = simular_remocion_bloque(bloque, estructura)
                print(f"📂 Archivo: {file}")
                diagnostico_mutacional(bloque, afectados)
                print("\n")


# B78: Ejecución principal del sistema SCANNER
# ∂Bᵢ/∂Bⱼ
if __name__ == "__main__":
    ejecutar_indexador_global()
    # Ejemplo de simulación:
    ejecutar_simulacion_mutacional("B6")  # Puedes cambiar a cualquier BLOQUE
