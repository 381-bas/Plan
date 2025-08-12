# B_SYS001: Importaciones principales para limpieza de archivos .pyc
# # ∂B_SYS001/∂B0
import os


# B_SYS002: Función para limpiar archivos .pyc de forma recursiva en un directorio base
# # ∂B_SYS002/∂B0
def limpiar_pyc(ruta_base: str):
    eliminados = []

    for root, _, files in os.walk(ruta_base):
        for file in files:
            if file.endswith(".pyc"):
                ruta_completa = os.path.join(root, file)
                try:
                    os.remove(ruta_completa)
                    eliminados.append(ruta_completa)
                except Exception as e:
                    print(f"❌ Error al eliminar {ruta_completa}: {e}")

    print("🧼 Limpieza de archivos .pyc completada.")
    print(f"Total eliminados: {len(eliminados)} archivos")
    for e in eliminados:
        print("🗑️", e)


# B_SYS003: Ejecución directa de limpieza en ruta específica
# # ∂B_SYS003/∂B0
limpiar_pyc(r"C:/Users/qmkbantiman/OneDrive - QMK SPA/GG/Python/Plan_Forecast")
