# generador_numeracion_bloques.py · MODO REFACTOR ♻️
# Fecha: 2025-06-23 · Firma: PRONT

import os
import re


def renumerar_bloques_en_archivo(ruta_archivo):
    nueva_ruta = ruta_archivo + ".renum"
    with open(ruta_archivo, "r", encoding="utf-8") as f:
        lineas = f.readlines()

    bloque_actual = 0
    lineas_actualizadas = []
    for linea in lineas:
        if re.match(r"#\s*B\d+[:：]", linea):
            linea = re.sub(r"#\s*B\d+[:：]", f"# B{bloque_actual}:", linea)
            bloque_actual += 1
        lineas_actualizadas.append(linea)

    with open(nueva_ruta, "w", encoding="utf-8") as f:
        f.writelines(lineas_actualizadas)

    return nueva_ruta


def procesar_directorio(directorio_base):
    archivos = []
    for root, _, files in os.walk(directorio_base):
        for file in files:
            if file.endswith(".py"):
                ruta = os.path.join(root, file)
                nueva = renumerar_bloques_en_archivo(ruta)
                archivos.append((ruta, nueva))
    return archivos


if __name__ == "__main__":
    BASE = r"C:/Users/qmkbantiman/OneDrive - QMK SPA/GG/Python/Plan_Forecast"
    resultados = procesar_directorio(BASE)
    print(
        f"✅ Renumeración aplicada a {len(resultados)} archivos. Archivos .renum generados."
    )
