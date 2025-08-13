# B_EXP001: Importaciones y configuración global de exportación SCANNER
# # ∂B_EXP001/∂B0
import os
import re
import sys
import hashlib
import json
from typing import List, Dict
from datetime import datetime

RUTA_BASE = os.path.abspath(".")
EXTENSIONES_VALIDAS = [".py"]
ARCHIVOS_EXCLUIDOS = {"backup_diario.py"}
RUTA_INST = os.path.abspath(os.path.join(RUTA_BASE, "..", "..", "Inst"))
ARCHIVO_SALIDA = os.path.join(RUTA_INST, "8. Plan_unificado.txt")


# B_EXP002: Extracción estructural SCANNER por archivo y derivadas
# # ∂B_EXP002/∂B0
def extraer_bloques_y_derivadas(ruta_archivo: str, ruta_base: str) -> List[dict]:
    """
    Extrae bloques SCANNER desde un archivo `.py`:
    - Bloques Bᵢ con descripción (acepta B19, B_v005, B_(1W), etc.)
    - Derivada ∂Bᵢ/∂Bⱼ (si existe)
    - Funciones vivas
    - Observaciones estructurales
    """
    bloques = []
    bloque_actual = None
    descripcion = ""
    derivada = ""
    funciones = []
    observaciones = []
    leyendo_derivada = False

    try:
        with open(ruta_archivo, "r", encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()

                match_bloque = re.match(r"#\s*(B[\w\(\)_]+):\s*(.*)", linea)
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
                    match_derivada = re.match(r"#\s*∂[^/]+/∂[^\s]+", linea)
                    if match_derivada:
                        derivada = linea.replace("#", "").strip()
                    leyendo_derivada = False

                funciones_en_linea = re.findall(
                    r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", linea
                )
                funciones.extend(funciones_en_linea)

                if linea.startswith("import ") or linea.startswith("from "):
                    funciones.append(linea)
                    if "*" in linea:
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
    """
    Aplica extracción SCANNER a cada módulo recolectado.
    Devuelve todos los bloques SCANNER encontrados en el sistema.
    """
    bloques = []
    for modulo in modulos:
        ruta_relativa = os.path.join(RUTA_BASE, modulo["ruta"])
        bloques_modulo = extraer_bloques_y_derivadas(ruta_relativa, RUTA_BASE)
        bloques.extend(bloques_modulo)
    return bloques


# B_EXP004: Limpieza de bloques SCANNER antes de exportar
# # ∂B_EXP004/∂B0
def limpiar_bloques(bloques: List[dict]) -> List[dict]:
    """
    Versión extendida que filtra funciones basura sintáctica, SQL o pandas
    desde bloques SCANNER. Mejora la trazabilidad funcional.
    """
    import re

    IGNORAR_FUNCIONES = {
        # Sintácticas y primitivas
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
        "cursor",
        "connect",
        "set_index",
        "reset_index",
        "sort_index",
        "astype",
        "fillna",
        "apply",
        "groupby",
        "merge",
        "sum",
        "drop",
        "unique",
        "tolist",
        "now",
        "stop",
        "strip",
        "warning",
        "sorted",
        "markdown",
        "importpandasaspd",
        "importsqlite3",
        "importstreamlitasst",
        "fromdatetimeimportdatetime",
        "get_key_buffer",
        "B_",
        # SQL / pandas DSL
        "SUM",
        "CAST",
        "JOIN",
        "SELECT",
        "INSERT",
        "VALUES",
        "AS",
        "IN",
        "ON",
        "AND",
        "strftime",
        "pivot_table",
        "DataFrame",
        "read_sql",
        "read_sql_query",
        "execute",
    }

    bloques_limpios = []
    for bloque in bloques:
        limpio = {}
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
                funciones_filtradas = []
                for f in bloque["funciones"]:
                    nombre = re.sub(r"[^\w]", "", f.split("(")[0].strip())
                    if nombre and nombre not in IGNORAR_FUNCIONES and len(nombre) > 2:
                        funciones_filtradas.append(nombre)
                limpio[clave] = sorted(set(funciones_filtradas))
            elif clave in bloque:
                limpio[clave] = bloque.get(clave, "")

        # 🔒 Asegura campo derivada presente, aunque esté vacío
        if "derivada" not in limpio:
            limpio["derivada"] = ""

        limpio["n_funciones_utiles"] = len(limpio.get("funciones", []))
        bloques_limpios.append(limpio)

    return bloques_limpios


# B_EXP005: Agrupamiento de bloques por función conceptual
# # ∂B_EXP005/∂B0
def agrupar_bloques_por_concepto(bloques: List[dict], concepto: str) -> List[str]:
    return [
        b["bloque"]
        for b in bloques
        if concepto in str(b.get("funciones")) or concepto in b.get("descripcion", "")
    ]


# B_EXP006: Diagnóstico SCANNER completo con sugerencias de acción
# # ∂B_EXP006/∂B0
def diagnosticar_bloques_sin_funcion(bloques: List[dict]) -> List[dict]:
    for bloque in bloques:
        if bloque.get("n_funciones_utiles", 0) == 0:
            desc = bloque.get("descripcion", "").lower()
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


# B_EXP007: Clasificación semántica extendida por nombre de función
# # ∂B_EXP007/∂B0
def clasificar_semantica_funcion(nombre: str) -> str:
    if nombre in {"markdown", "title", "subheader", "stop", "info", "warning"}:
        return "UI"
    if nombre in {"len", "str", "int", "float", "type", "sorted", "print", "get"}:
        return "UTILITARIA"
    if nombre in {"SUM", "CAST", "JOIN", "SELECT", "VALUES"}:
        return "SQL_DSL"
    if nombre in {"B_", "fromdatetimeimportdatetime", "importpandasaspd"}:
        return "DECORATIVA"
    if nombre in {"astype", "reset_index", "pivot_table", "fillna", "groupby"}:
        return "PANDAS_BASICAS"
    return "FUNCIONAL"


# B_EXP009: Enriquecimiento con modulo_base y orden incremental por bloque
# # ∂B_EXP009/∂B0
def optimizar_bloques(bloques: List[dict]) -> List[dict]:
    """
    Aplica mejoras estructurales:
    1. Campo `modulo_base` simplificado
    2. Ordenamiento por archivo y bloque
    3. Campo `orden` incremental
    """
    for b in bloques:
        b["modulo_base"] = os.path.basename(b["archivo"])

    bloques = sorted(bloques, key=lambda b: (b["modulo_base"], b["bloque"]))
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


# B_EXP011: Exportación de índice SCANNER plano (TXT)
# # ∂B_EXP011/∂B0
def exportar_index_global(bloques: List[dict], archivo_salida: str):
    """
    Exporta listado plano de bloques SCANNER con campos clave:
    - Archivo, Bloque, Descripción, Derivada, Funciones, Observaciones
    """
    os.makedirs(os.path.dirname(archivo_salida), exist_ok=True)
    with open(archivo_salida, "w", encoding="utf-8") as f:
        f.write(
            "Archivo;Bloque;Descripción;Derivada;Funciones e Import;Observaciones\n"
        )
        for bloque in bloques:
            funciones = ", ".join(sorted(set(bloque["funciones"])))
            observaciones = ", ".join(sorted(set(bloque["observaciones"])))
            f.write(
                f"{bloque['archivo']};{bloque['bloque']};{bloque['descripcion']};{bloque['derivada']};{funciones};{observaciones}\n"
            )


# B_EXP012: Enriquecimiento de bloques con derivadas sugeridas y conflictos
# # ∂B_EXP012/∂B0
def enriquecer_bloques(bloques: List[dict]) -> List[dict]:
    """
    Añade ∂Sugeridas y Conflictos a cada bloque SCANNER.
    """
    for bloque in bloques:
        funciones_set = set(bloque["funciones"])
        sugeridas = []
        for otro in bloques:
            if otro == bloque:
                continue
            if funciones_set & set(otro["funciones"]):
                sugeridas.append(otro["bloque"])
        bloque["sugeridas"] = ", ".join(sorted(set(sugeridas)))
        bloque["conflictos"] = "SÍ" if not bloque["derivada"] else "NO"
    return bloques


# B_EXP013: Lectura e indexación de bloques vivos desde texto fuente
# # ∂B_EXP013/∂B0
def leer_archivo_py(ruta: str) -> str:
    with open(ruta, "r", encoding="utf-8") as f:
        return f.read()


def indexar_bloques(contenido: str) -> Dict[str, List[str]]:
    """
    Extrae pares (Bloque, ∂Bᵢ/∂Bⱼ) desde contenido en texto.
    Compatible con sintaxis extendida B_vXXX, B_(1Z), etc.
    """
    bloques = {}
    patron = re.compile(r"#\s*(B[\w\(\)_]+):.*?\n#\s*∂([^\n]+)")
    for match in patron.finditer(contenido):
        bloque = match.group(1).strip()
        derivada = f"∂{match.group(2).strip()}"
        bloques.setdefault(bloque, []).append(derivada)
    return bloques


# B_EXP014: Generación de hash SHA256 por archivo
# # ∂B_EXP014/∂B0
def obtener_hash(filepath):
    """Devuelve hash SHA256 de un archivo dado."""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for bloque in iter(lambda: f.read(4096), b""):
            sha256.update(bloque)
    return sha256.hexdigest()


# B_EXP015: Recolección recursiva de archivos válidos para escaneo/exportación
# # ∂B_EXP015/∂B0
def recolectar_archivos(carpeta, cargar_contenido: bool = True):
    """
    Escanea directorio base recursivamente:
    - Ignora carpetas de backup
    - Lee contenido, fecha, hash y categoría (si se indica)
    """
    modulos = []
    for dirpath, dirnames, archivos in os.walk(carpeta):
        if "backups" in dirpath or "motor" in dirpath:
            continue
        for archivo in sorted(archivos):
            if (
                archivo.endswith(tuple(EXTENSIONES_VALIDAS))
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
    return modulos


# B_EXP016: Exportación de unificado consolidado de archivos por partes
# # ∂B_EXP016/∂B0
def exportar_unificado(modulos):
    """
    Genera 3 archivos de salida:
    - 8. Plan_unificado_p1.txt → primera tercera parte
    - 8. Plan_unificado_p2.txt → segunda tercera parte
    - 8. Plan_unificado_p3.txt → tercera parte restante
    """
    os.makedirs(os.path.dirname(ARCHIVO_SALIDA), exist_ok=True)

    total = len(modulos)
    tercio = total // 3
    partes = [
        ("8. Plan_unificado_p1.txt", modulos[:tercio]),
        ("8. Plan_unificado_p2.txt", modulos[tercio : 2 * tercio]),
        ("8. Plan_unificado_p3.txt", modulos[2 * tercio :]),
    ]

    for nombre_archivo, subset in partes:
        ruta_salida = os.path.join(RUTA_INST, nombre_archivo)
        with open(ruta_salida, "w", encoding="utf-8") as salida:
            salida.write("=============================================\n")
            salida.write("ÍNDICE DE ARCHIVOS UNIFICADOS – SISTEMA SYMBIOS\n")
            salida.write("=============================================\n\n")

            categorias = sorted(set(m["categoria"] for m in subset))
            for categoria in categorias:
                salida.write(f"\n## {categoria}\n")
                for i, mod in enumerate(subset, 1):
                    if mod["categoria"] == categoria:
                        salida.write(f"{i}. {mod['nombre']}  ({mod['ruta']})\n")

            salida.write("\n\n")
            salida.write("=========================================\n")
            salida.write("ARCHIVOS CONSOLIDADOS – CONTENIDO COMPLETO\n")
            salida.write("=========================================\n\n")

            for i, mod in enumerate(subset, 1):
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
                salida.write(mod["contenido"])
                salida.write("\n")

        print(f"✅ Parte exportada: {ruta_salida} ({len(subset)} archivos)")


# B_EXP017: Clasificación de módulo según ruta relativa
# # ∂B_EXP017/∂B0
def clasificar_modulo(ruta_relativa):
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


# B_EXP018: Exportación de bloques SCANNER en formato JSON estructurado
# # ∂B_EXP018/∂B0
def exportar_index_json(bloques: List[dict], ruta_salida: str):
    """
    Exporta los bloques SCANNER estructurados a un archivo JSON legible.
    """
    try:
        with open(ruta_salida, "w", encoding="utf-8") as f_out:
            json.dump(bloques, f_out, indent=2, ensure_ascii=False)
        print(f"✅ Index SCANNER JSON generado: {ruta_salida} ({len(bloques)} bloques)")
    except Exception as e:
        print(f"❌ Error al exportar JSON SCANNER: {e}")


# B_EXP019: División estructural del JSON SCANNER en tres partes
# # ∂B_EXP019/∂B0
def exportar_index_json_en_partes(bloques: List[dict], carpeta_salida: str):
    """
    Divide `scanner_index_global.json` en tres partes:
    - scanner_index_global_p1.json
    - scanner_index_global_p2.json
    - scanner_index_global_p3.json
    """
    total = len(bloques)
    tercio = total // 3
    partes = [
        ("scanner_index_global_p1.json", bloques[:tercio]),
        ("scanner_index_global_p2.json", bloques[tercio : 2 * tercio]),
        ("scanner_index_global_p3.json", bloques[2 * tercio :]),
    ]

    for nombre_archivo, subset in partes:
        ruta = os.path.join(carpeta_salida, nombre_archivo)
        try:
            with open(ruta, "w", encoding="utf-8") as f_out:
                json.dump(subset, f_out, indent=2, ensure_ascii=False)
            print(f"✅ Fragmento JSON exportado: {ruta} ({len(subset)} bloques)")
        except Exception as e:
            print(f"❌ Error al exportar {nombre_archivo}: {e}")


# B_EXP020: Ejecución principal de exportación SCANNER y consolidado
# # ∂B_EXP020/∂B0
if __name__ == "__main__":

    # ∂B_EXP020/∂B_EXP018
    if "--json-only" in sys.argv:
        modulos_estructura = recolectar_archivos(RUTA_BASE, cargar_contenido=False)
        bloques = pipeline_exportar_bloques(modulos_estructura)
        salida_scanner_json = os.path.join(RUTA_INST, "scanner_index_global.json")
        exportar_index_json(bloques, salida_scanner_json)
        sys.exit()

    try:
        # Consolidado completo con contenido
        modulos_contenido = recolectar_archivos(RUTA_BASE, cargar_contenido=True)
        exportar_unificado(modulos_contenido)

        # Index SCANNER con solo estructura
        modulos_estructura = recolectar_archivos(RUTA_BASE, cargar_contenido=False)
        bloques = pipeline_exportar_bloques(modulos_estructura)

        # Exportación en formato JSON estructurado
        salida_scanner_json = os.path.join(RUTA_INST, "scanner_index_global.json")
        exportar_index_json_en_partes(bloques, RUTA_INST)

    except Exception as e:
        print(f"❌ Error en ejecución principal: {e}")
