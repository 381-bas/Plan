# B_DOC001: Comentarios menores (solo documentación, sin cambiar lógica)
# ∂B_DOC001/∂B0
# Mantiene exportación única a 8. Plan_unificado.txt y rutas fijas.
# ggg
# ggg
# “no-op”

import os
import re
import sys
import hashlib
import json
from typing import List, Dict, Iterable
from datetime import datetime

# --- Ubicaciones requeridas (requisitos del usuario) ---
# 0) Carpeta del proyecto
RUTA_BASE = r"C:\Users\qmkbantiman\Documents\Plan_qmk"
# 1) Ruta de salida
RUTA_INST = r"C:\Users\qmkbantiman\Documents"
# Archivo único de salida (requisito 3: unificar en 1)
ARCHIVO_SALIDA = os.path.join(RUTA_INST, "8. Plan_unificado.txt")

# Extensiones y exclusiones
EXTENSIONES_VALIDAS = [".py"]
ARCHIVOS_EXCLUIDOS = {"backup_diario.py"}
DIRS_EXCLUIDAS = {".git", ".venv", "__pycache__", "audits", "Inst", "backups", "motor"}


# B_EXP014: Generación de hash SHA256 por archivo
# # ∂B_EXP014/∂B0
def obtener_hash(filepath: str) -> str:
    """Devuelve hash SHA256 de un archivo dado."""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for bloque in iter(lambda: f.read(4096), b""):
            sha256.update(bloque)
    return sha256.hexdigest()


# B_EXP017: Clasificación de módulo según ruta relativa
# # ∂B_EXP017/∂B0
def clasificar_modulo(ruta_relativa: str) -> str:
    """
    Devuelve categoría de un archivo según su ubicación:
    - modulos   → MÓDULOS FUNCIONALES
    - utils     → UTILIDADES Y LÓGICA
    - core      → NÚCLEO CENTRAL
    - forecast  → FORECAST
    - service   → SERVICIOS
    - motor     → EXCLUIDOS (si corresponde excluirlos)
    - otro      → NÚCLEO / OTROS
    """
    ruta = ruta_relativa.replace("\\", "/")
    if "/modulos/" in ruta:
        return "MÓDULOS FUNCIONALES (/modulos/)"
    elif "/utils/" in ruta:
        return "UTILIDADES Y LÓGICA (/utils/)"
    elif "/core/" in ruta:
        return "NÚCLEO CENTRAL (/core/)"
    elif "/forecast/" in ruta:
        return "FORECAST (/forecast/)"
    elif "/service/" in ruta:
        return "SERVICIOS (/service/)"
    elif "/motor/" in ruta:
        return "EXCLUIDOS (/motor/)"
    else:
        return "NÚCLEO / OTROS (/root/)"


# B_EXP015: Recolección recursiva de archivos válidos para escaneo/exportación
# # ∂B_EXP015/∂B0
def recolectar_archivos(carpeta: str, cargar_contenido: bool = True) -> List[dict]:
    """
    Escanea directorio base recursivamente:
    - Ignora carpetas y archivos excluidos
    - Lee contenido, fecha, hash y categoría (si se indica)
    """
    modulos = []

    for dirpath, dirnames, archivos in os.walk(carpeta):
        # Filtra directorios excluidos in-place para que os.walk no entre en ellos
        dirnames[:] = [d for d in dirnames if d not in DIRS_EXCLUIDAS]

        for archivo in sorted(archivos):
            if (
                os.path.splitext(archivo)[1] in EXTENSIONES_VALIDAS
                and archivo not in ARCHIVOS_EXCLUIDOS
            ):
                ruta_completa = os.path.join(dirpath, archivo)
                ruta_relativa = os.path.relpath(ruta_completa, RUTA_BASE)
                hash_valor = obtener_hash(ruta_completa)
                modificado = datetime.fromtimestamp(
                    os.path.getmtime(ruta_completa)
                ).strftime("%Y-%m-%d %H:%M:%S")
                categoria = clasificar_modulo(ruta_relativa)

                contenido = ""
                if cargar_contenido:
                    with open(ruta_completa, encoding="utf-8") as f:
                        contenido = f.read()

                modulos.append(
                    {
                        "nombre": archivo,
                        "ruta": ruta_relativa,
                        "categoria": categoria,
                        "hash": hash_valor,
                        "modificado": modificado,
                        "contenido": contenido,
                    }
                )

    # Orden determinístico por categoría y ruta
    modulos.sort(key=lambda m: (m["categoria"], m["ruta"]))
    return modulos


# B_EXP002: Extracción estructural SCANNER por archivo y derivadas
# # ∂B_EXP002/∂B0
def extraer_bloques_y_derivadas(ruta_archivo: str, ruta_base: str) -> List[dict]:
    """
    Extrae bloques SCANNER desde un archivo `.py`:
    - Bloques Bᵢ con descripción (acepta B19, B_v005, B_(1W), etc.)
    - Derivada ∂Bᵢ/∂Bⱼ (si existe)
    - Funciones vivas (candidatas)
    - Observaciones estructurales
    """
    bloques: List[dict] = []
    bloque_actual = None
    descripcion = ""
    derivada = ""
    funciones: List[str] = []
    observaciones: List[str] = []
    leyendo_derivada = False

    try:
        with open(ruta_archivo, "r", encoding="utf-8") as f:
            for linea in f:
                s = linea.strip()

                match_bloque = re.match(r"#\s*(B[\w\(\)_]+):\s*(.*)", s)
                if match_bloque:
                    if bloque_actual:
                        bloques.append(
                            {
                                "archivo": os.path.relpath(ruta_archivo, ruta_base),
                                "bloque": bloque_actual,
                                "descripcion": descripcion,
                                "derivada": derivada,
                                "funciones": funciones,
                                "observaciones": observaciones,
                            }
                        )
                    bloque_actual = match_bloque.group(1)
                    descripcion = match_bloque.group(2)
                    derivada = ""
                    funciones = []
                    observaciones = []
                    leyendo_derivada = True
                    continue

                if leyendo_derivada:
                    match_derivada = re.match(r"#\s*∂[^/]+/∂[^\s]+", s)
                    if match_derivada:
                        derivada = s.replace("#", "").strip()
                    leyendo_derivada = False

                # funciones vistas
                funciones_en_linea = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", s)
                funciones.extend(funciones_en_linea)

                if s.startswith("import ") or s.startswith("from "):
                    funciones.append(s)
                    if "*" in s:
                        observaciones.append("import con asterisco (*), revisar")

        if bloque_actual:
            bloques.append(
                {
                    "archivo": os.path.relpath(ruta_archivo, ruta_base),
                    "bloque": bloque_actual,
                    "descripcion": descripcion,
                    "derivada": derivada,
                    "funciones": funciones,
                    "observaciones": observaciones,
                }
            )

    except Exception as e:
        print(f"⚠️ Error al procesar {ruta_archivo}: {e}")

    return bloques


# B_EXP003: Escaneo estructural completo SCANNER para todos los módulos
# # ∂B_EXP003/∂B0
def escanear_estructura_scanner(modulos: List[dict]) -> List[dict]:
    """Aplica extracción SCANNER a cada módulo recolectado."""
    bloques: List[dict] = []
    for modulo in modulos:
        ruta_relativa = os.path.join(RUTA_BASE, modulo["ruta"])
        bloques_modulo = extraer_bloques_y_derivadas(ruta_relativa, RUTA_BASE)
        bloques.extend(bloques_modulo)
    return bloques


# B_EXP004: Limpieza de bloques SCANNER antes de exportar
# # ∂B_EXP004/∂B0
def limpiar_bloques(bloques: List[dict]) -> List[dict]:
    """Filtra funciones basura y normaliza campos clave de SCANNER."""
    IGNORAR_FUNCIONES = {
        # Sintaxis/primitivas/UI/DSL comunes
        "import",
        "from",
        "as",
        "int",
        "str",
        "float",
        "list",
        "dict",
        "set",
        "bool",
        "range",
        "print",
        "def",
        "open",
        "with",
        "len",
        "get",
        "copy",
        "append",
        "join",
        "read",
        "write",
        "replace",
        "update",
        "close",
        "astype",
        "fillna",
        "groupby",
        "merge",
        "sum",
        "drop",
        "unique",
        "tolist",
        "sorted",
        "markdown",
        "pivot_table",
        "DataFrame",
        "read_sql",
        "read_sql_query",
        "execute",
    }

    bloques_limpios: List[dict] = []
    for bloque in bloques:
        limpio: Dict[str, object] = {}
        for clave in [
            "bloque",
            "archivo",
            "descripcion",
            "funciones",
            "derivada",
            "sugeridas",
            "conflictos",
            "observaciones",
        ]:
            if clave == "funciones" and clave in bloque:
                funciones_filtradas: List[str] = []
                for f in bloque["funciones"]:
                    nombre = re.sub(r"[^\w]", "", f.split("(")[0].strip())
                    if nombre and nombre not in IGNORAR_FUNCIONES and len(nombre) > 2:
                        funciones_filtradas.append(nombre)
                limpio[clave] = sorted(set(funciones_filtradas))
            elif clave in bloque:
                limpio[clave] = bloque.get(clave, "")

        if "derivada" not in limpio:
            limpio["derivada"] = ""

        limpio["n_funciones_utiles"] = len(limpio.get("funciones", []))
        bloques_limpios.append(limpio)

    return bloques_limpios


# B_EXP006: Diagnóstico SCANNER completo con sugerencias de acción
# # ∂B_EXP006/∂B0
def diagnosticar_bloques_sin_funcion(bloques: List[dict]) -> List[dict]:
    for bloque in bloques:
        if bloque.get("n_funciones_utiles", 0) == 0:
            desc = str(bloque.get("descripcion", "")).lower()
            if "import" in desc or "dependencia" in desc:
                bloque["accion_sugerida"] = "fusionar"
            elif "rutas" in desc or "rol" in desc:
                bloque["accion_sugerida"] = "declarar_semantico"
            elif "pivot" in desc or "buffer" in desc:
                bloque["accion_sugerida"] = "fortalecer_derivada"
            else:
                bloque["accion_sugerida"] = "eliminar"
        else:
            bloque["accion_sugerida"] = ""
    return bloques


# B_EXP012: Enriquecimiento de bloques con derivadas sugeridas y conflictos
# # ∂B_EXP012/∂B0
def enriquecer_bloques(bloques: List[dict]) -> List[dict]:
    for bloque in bloques:
        funciones_set = set(bloque.get("funciones", []))
        sugeridas = []
        for otro in bloques:
            if otro is bloque:
                continue
            if funciones_set & set(otro.get("funciones", [])):
                sugeridas.append(otro.get("bloque", ""))
        bloque["sugeridas"] = ", ".join(sorted(set(sugeridas)))
        bloque["conflictos"] = "SÍ" if not bloque.get("derivada") else "NO"
    return bloques


# B_EXP009: Enriquecimiento con modulo_base y orden incremental por bloque
# # ∂B_EXP009/∂B0
def optimizar_bloques(bloques: List[dict]) -> List[dict]:
    for b in bloques:
        b["modulo_base"] = os.path.basename(b["archivo"]) if b.get("archivo") else ""
    bloques = sorted(
        bloques, key=lambda b: (b.get("modulo_base", ""), b.get("bloque", ""))
    )
    for i, b in enumerate(bloques):
        b["orden"] = i + 1
    return bloques


# B_EXP010: Pipeline estructurado de limpieza y exportación de bloques SCANNER
# # ∂B_EXP010/∂B0
def pipeline_exportar_bloques(modulos: List[dict]) -> List[dict]:
    bloques = escanear_estructura_scanner(modulos)
    bloques = limpiar_bloques(bloques)
    bloques = diagnosticar_bloques_sin_funcion(bloques)
    bloques = enriquecer_bloques(bloques)
    bloques = optimizar_bloques(bloques)
    return bloques


# B_EXP021: Mapeo de bloques por archivo (para exportar dentro del TXT único)
# # ∂B_EXP021/∂B0
def mapear_bloques_por_archivo(bloques: Iterable[dict]) -> Dict[str, List[dict]]:
    por_archivo: Dict[str, List[dict]] = {}
    for b in bloques:
        ruta = b.get("archivo", "")
        por_archivo.setdefault(ruta, []).append(b)
    # ordenar interno por orden calculado
    for ruta in por_archivo:
        por_archivo[ruta].sort(key=lambda x: x.get("orden", 0))
    return por_archivo


# B_EXP011: Exportación de índice SCANNER plano (TXT)
# # ∂B_EXP011/∂B0
def exportar_index_global(bloques: List[dict], archivo_salida: str) -> None:
    """
    Exporta listado plano de bloques SCANNER con campos clave (TXT CSV-like).
    Nota: se mantiene por compatibilidad; no genera JSON p*. (Req 2)
    """
    os.makedirs(os.path.dirname(archivo_salida), exist_ok=True)
    with open(archivo_salida, "w", encoding="utf-8", newline="\n") as f:
        f.write("Archivo;Bloque;Descripción;Derivada;Funciones;Observaciones\n")
        for bloque in bloques:
            funciones = ", ".join(bloque.get("funciones", []))
            observaciones = ", ".join(bloque.get("observaciones", []))
            f.write(
                f"{bloque.get('archivo','')};{bloque.get('bloque','')};{bloque.get('descripcion','')};{bloque.get('derivada','')};{funciones};{observaciones}\n"
            )


# B_EXP018: Exportación de bloques SCANNER en formato JSON estructurado (opcional)
# # ∂B_EXP018/∂B0
def exportar_index_json(bloques: List[dict], ruta_salida: str) -> None:
    """
    Exporta los bloques SCANNER estructurados a un archivo JSON legible.
    Se conserva como utilidad, pero se eliminan funciones/archivos p*.json. (Req 2)
    """
    try:
        with open(ruta_salida, "w", encoding="utf-8", newline="\n") as f_out:
            json.dump(bloques, f_out, indent=2, ensure_ascii=False)
        print(f"✅ Index SCANNER JSON generado: {ruta_salida} ({len(bloques)} bloques)")
    except Exception as e:
        print(f"❌ Error al exportar JSON SCANNER: {e}")


# B_EXP016→B_EXP022: Exportación unificada ÚNICA con anexos SCANNER por archivo
# # ∂B_EXP022/∂B0
def exportar_unificado_unico(
    modulos: List[dict], bloques_map: Dict[str, List[dict]]
) -> None:
    """
    Genera **un solo** archivo de salida con:
    - Índice por categoría
    - Cuerpo con contenido completo de cada archivo
    - Anexo SCANNER por archivo (bloques/derivadas/funciones/observaciones)
    """
    os.makedirs(os.path.dirname(ARCHIVO_SALIDA), exist_ok=True)

    HEADER_IDX = (
        "=============================================\n"
        "ÍNDICE DE ARCHIVOS UNIFICADOS – SISTEMA SYMBIOS\n"
        "=============================================\n\n"
    )
    HEADER_BODY = (
        "=========================================\n"
        "ARCHIVOS CONSOLIDADOS – CONTENIDO COMPLETO\n"
        "=========================================\n\n"
    )

    with open(ARCHIVO_SALIDA, "w", encoding="utf-8", newline="\n") as salida:
        # Índice
        salida.write(HEADER_IDX)
        categorias = sorted({m["categoria"] for m in modulos})
        for cat in categorias:
            salida.write(f"\n## {cat}\n")
            for i, mod in enumerate([m for m in modulos if m["categoria"] == cat], 1):
                salida.write(f"{i}. {mod['nombre']}  ({mod['ruta']})\n")

        salida.write("\n\n")
        salida.write(HEADER_BODY)

        # Cuerpo por archivo con anexo SCANNER
        for i, mod in enumerate(modulos, 1):
            salida.write("\n\n")
            salida.write(
                "------------------------------------------------------------\n"
            )
            salida.write(f"#{i}: {mod['nombre']} | {mod['categoria']}\n")
            salida.write(f"Ruta relativa: {mod['ruta']}\n")
            salida.write(f"Última modificación: {mod['modificado']}\n")
            salida.write(f"SHA256: {mod['hash']}\n")
            salida.write(
                "------------------------------------------------------------\n\n"
            )

            # Contenido del archivo
            salida.write(mod["contenido"])  # asume UTF-8
            salida.write("\n")

            # --- Anexo SCANNER por archivo ---
            rel = mod["ruta"]
            bloques_locales = bloques_map.get(rel, [])
            if bloques_locales:
                salida.write("\n[SCANNER] Bloques detectados (limpios)\n")
                for b in bloques_locales:
                    funcs = ", ".join(b.get("funciones", []))
                    obs = ", ".join(b.get("observaciones", []))
                    der = b.get("derivada", "")
                    salida.write(
                        f" - {b.get('bloque','')} — {b.get('descripcion','')}"
                        f" | derivada: {der} | funciones: {funcs}"
                        f" | obs: {obs}\n"
                    )

    print(f"✅ Unificado exportado: {ARCHIVO_SALIDA} ({len(modulos)} archivos)")


# B_EXP020: Ejecución principal de exportación SCANNER y consolidado
# # ∂B_EXP020/∂B0
if __name__ == "__main__":
    # Modo opcional: sólo JSON del SCANNER (sin p*.json)
    if "--json-only" in sys.argv:
        modulos_estructura = recolectar_archivos(RUTA_BASE, cargar_contenido=False)
        bloques = pipeline_exportar_bloques(modulos_estructura)
        salida_scanner_json = os.path.join(RUTA_INST, "scanner_index_global.json")
        exportar_index_json(bloques, salida_scanner_json)
        sys.exit(0)

    try:
        # 1) Consolidado completo con contenido (archivos .py)
        modulos_contenido = recolectar_archivos(RUTA_BASE, cargar_contenido=True)

        # 2) SCANNER sólo estructura (para anexos en el TXT único)
        modulos_estructura = recolectar_archivos(RUTA_BASE, cargar_contenido=False)
        bloques = pipeline_exportar_bloques(modulos_estructura)
        bloques_map = mapear_bloques_por_archivo(bloques)

        # 3) Exportación única con anexos SCANNER por archivo (Req 3 y 4)
        exportar_unificado_unico(modulos_contenido, bloques_map)

    except Exception as e:
        print(f"❌ Error en ejecución principal: {e}")
