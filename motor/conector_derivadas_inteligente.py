# conector_∂_inteligente.py2 · VERSIÓN REFORZADA
# Firma: PRONT · Modo Refactor ♻️

import pandas as pd
from collections import defaultdict


def construir_matriz_derivadas(path_indexador):
    try:
        df = pd.read_csv(path_indexador, sep=";", encoding="utf-8", dtype=str).fillna(
            ""
        )
    except Exception as e:
        print(f"⚠️ Error al leer archivo: {e}")
        return set()

    bloque_funcion = defaultdict(set)
    bloque_invoca = defaultdict(set)

    for _, row in df.iterrows():
        bloque = row["Bloque"].strip()
        archivo = row["Archivo"].strip()
        key = f"{archivo}:{bloque}"

        funciones = [
            f.strip() for f in row["Funciones e Import"].split(",") if f.strip()
        ]
        for func in funciones:
            if func.isidentifier():
                bloque_invoca[key].add(func)

    for _, row in df.iterrows():
        bloque = row["Bloque"].strip()
        archivo = row["Archivo"].strip()
        key = f"{archivo}:{bloque}"

        funciones = [
            f.strip() for f in row["Funciones e Import"].split(",") if f.strip()
        ]
        for func in funciones:
            if func.isidentifier():
                bloque_funcion[func].add(key)

    relaciones_derivadas = set()

    for b_origen, funciones_llamadas in bloque_invoca.items():
        for funcion in funciones_llamadas:
            posibles_destinos = bloque_funcion.get(funcion, set())
            for b_destino in posibles_destinos:
                if b_origen != b_destino:
                    relaciones_derivadas.add((b_origen, b_destino))

    return relaciones_derivadas


if __name__ == "__main__":
    archivo = r"C:/Users/qmkbantiman/OneDrive - QMK SPA/GG/Python/Plan_Forecast/scanner_index_global.txt"
    relaciones = construir_matriz_derivadas(archivo)

    if relaciones:
        print(f"🔗 Relaciones ∂ detectadas: {len(relaciones)}\n")
        for origen, destino in sorted(relaciones):
            print(f"∂({origen})/∂({destino})")
    else:
        print("⚠️ No se encontraron relaciones ∂. Verifica contenido del indexador.")
