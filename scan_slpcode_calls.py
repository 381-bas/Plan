# tools/scan_slpcode_calls.py
# Escáner de referencias no canónicas a SlpCode en st.session_state
# - NO modifica archivos (solo reporte)
# - Detecta: st.session_state.slpcode / ['slpcode'] / ['Slpcode'] / .get('slpcode', ...)
# - Opcional: registra ocurrencias en strings y comentarios (separadas como "texto")

import os
import io
import re
import tokenize
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple

# --- Rutas (ajústalas si tu proyecto está en otra carpeta) ---
RUTA_BASE = r"C:\Users\qmkbantiman\Documents\Plan_qmk"
RUTA_INST = r"C:\Users\qmkbantiman\Documents"
REPORTE_SALIDA = os.path.join(RUTA_INST, "reporte_slpcode_calls.txt")

# --- Filtros de recorrido (mismos criterios de tu exportador) ---
EXTENSIONES_VALIDAS = {".py"}
ARCHIVOS_EXCLUIDOS = {"backup_diario.py"}
DIRS_EXCLUIDAS = {".git", ".venv", "__pycache__", "audits", "Inst", "backups", "motor"}

# --- Tipos de hallazgo ---
ATTR = "CODE_ATTR   (st.session_state.slpcode)"
SUBS = "CODE_SUBS   (st.session_state['slpcode'|'Slpcode'])"
GET = "CODE_GET    (st.session_state.get('slpcode'|'Slpcode', ...))"
TEXT = "TEXT        (string/comment con 'slpcode'|'Slpcode')"


def iter_py_files(root: Path):
    for p in root.rglob("*.py"):
        parts = set(p.parts)
        if parts & DIRS_EXCLUIDAS:
            continue
        if p.name in ARCHIVOS_EXCLUIDOS:
            continue
        yield p


def load_lines(path: Path) -> List[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1").splitlines()


def tok(tokens: List[tokenize.TokenInfo], i: int):
    return tokens[i] if 0 <= i < len(tokens) else None


def scan_tokens(code: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Devuelve (hits_codigo, hits_texto)
    - hits_codigo: referencias reales en código ejecutable (ATTR, SUBS, GET)
    - hits_texto : menciones en strings o comentarios (TEXT)
    """
    hits_code: List[Dict] = []
    hits_text: List[Dict] = []

    g = tokenize.generate_tokens(io.StringIO(code).readline)
    tokens = list(g)
    i = 0

    while i < len(tokens):
        t0 = tok(tokens, i)

        # --- TEXT: strings / comentarios que mencionan slpcode/Slpcode ---
        if t0 and t0.type in (tokenize.STRING, tokenize.COMMENT):
            s = t0.string
            # Strip comillas en STRING
            if (
                t0.type == tokenize.STRING
                and len(s) >= 2
                and s[0] in "\"'"
                and s[-1] == s[0]
            ):
                s_clean = s[1:-1]
            else:
                s_clean = s
            if re.search(r"\bslpcode\b", s_clean, flags=re.IGNORECASE):
                hits_text.append(
                    {
                        "type": TEXT,
                        "line": t0.start[0],
                        "col": t0.start[1],
                        "snippet": s_clean[:200].replace("\n", "\\n"),
                        "suggest": "Opcional: estandarizar texto a 'SlpCode' (cosmético)",
                    }
                )

        # --- CODE_ATTR: st.session_state.slpcode
        if (
            t0
            and t0.type == tokenize.NAME
            and t0.string == "st"
            and tok(tokens, i + 1)
            and tok(tokens, i + 1).string == "."
            and tok(tokens, i + 2)
            and tok(tokens, i + 2).type == tokenize.NAME
            and tok(tokens, i + 2).string == "session_state"
            and tok(tokens, i + 3)
            and tok(tokens, i + 3).string == "."
            and tok(tokens, i + 4)
            and tok(tokens, i + 4).type == tokenize.NAME
            and tok(tokens, i + 4).string == "slpcode"
        ):
            hits_code.append(
                {
                    "type": ATTR,
                    "line": t0.start[0],
                    "col": t0.start[1],
                    "snippet": "st.session_state.slpcode",
                    "suggest": "Usar st.session_state['SlpCode']",
                }
            )
            i += 5
            continue

        # --- CODE_SUBS: st.session_state['slpcode'|'Slpcode']
        if (
            t0
            and t0.type == tokenize.NAME
            and t0.string == "st"
            and tok(tokens, i + 1)
            and tok(tokens, i + 1).string == "."
            and tok(tokens, i + 2)
            and tok(tokens, i + 2).type == tokenize.NAME
            and tok(tokens, i + 2).string == "session_state"
            and tok(tokens, i + 3)
            and tok(tokens, i + 3).string == "["
            and tok(tokens, i + 4)
            and tok(tokens, i + 4).type == tokenize.STRING
            and tok(tokens, i + 5)
            and tok(tokens, i + 5).string == "]"
        ):
            lit = tok(tokens, i + 4).string.strip("\"'")
            if lit in ("slpcode", "Slpcode"):
                hits_code.append(
                    {
                        "type": SUBS,
                        "line": t0.start[0],
                        "col": t0.start[1],
                        "snippet": f"st.session_state['{lit}']",
                        "suggest": "Usar st.session_state['SlpCode']",
                    }
                )
            i += 6
            continue

        # --- CODE_GET: st.session_state.get('slpcode'|'Slpcode', ...)
        if (
            t0
            and t0.type == tokenize.NAME
            and t0.string == "st"
            and tok(tokens, i + 1)
            and tok(tokens, i + 1).string == "."
            and tok(tokens, i + 2)
            and tok(tokens, i + 2).type == tokenize.NAME
            and tok(tokens, i + 2).string == "session_state"
            and tok(tokens, i + 3)
            and tok(tokens, i + 3).string == "."
            and tok(tokens, i + 4)
            and tok(tokens, i + 4).type == tokenize.NAME
            and tok(tokens, i + 4).string == "get"
            and tok(tokens, i + 5)
            and tok(tokens, i + 5).string == "("
            and tok(tokens, i + 6)
            and tok(tokens, i + 6).type == tokenize.STRING
        ):
            lit = tok(tokens, i + 6).string.strip("\"'")
            if lit in ("slpcode", "Slpcode"):
                hits_code.append(
                    {
                        "type": GET,
                        "line": t0.start[0],
                        "col": t0.start[1],
                        "snippet": f"st.session_state.get('{lit}', ...)",
                        "suggest": "Usar st.session_state.get('SlpCode', ...)",
                    }
                )
            # avanzar hasta cerrar paréntesis para evitar falsos positivos encadenados
            j = i + 7
            depth = 1
            while tok(tokens, j) and depth > 0:
                if tok(tokens, j).string == "(":
                    depth += 1
                elif tok(tokens, j).string == ")":
                    depth -= 1
                j += 1
            i = j
            continue

        i += 1

    return hits_code, hits_text


def clasificar_categoria(ruta_relativa: str) -> str:
    ruta = ruta_relativa.replace("\\", "/")
    if "/modulos/" in ruta:
        return "MÓDULOS FUNCIONALES (/modulos/)"
    if "/utils/" in ruta:
        return "UTILIDADES Y LÓGICA (/utils/)"
    if "/core/" in ruta:
        return "NÚCLEO CENTRAL (/core/)"
    if "/forecast/" in ruta:
        return "FORECAST (/forecast/)"
    if "/service/" in ruta:
        return "SERVICIOS (/service/)"
    if "/motor/" in ruta:
        return "EXCLUIDOS (/motor/)"
    return "NÚCLEO / OTROS (/root/)"


def run_scan() -> Dict[str, Dict[str, List[Dict]]]:
    base = Path(RUTA_BASE)
    resultados: Dict[str, Dict[str, List[Dict]]] = {}
    for path in iter_py_files(base):
        rel = os.path.relpath(str(path), RUTA_BASE)
        lines = load_lines(path)
        code = "\n".join(lines)
        hits_code, hits_text = scan_tokens(code)
        if not hits_code and not hits_text:
            continue
        resultados[rel] = {
            "categoria": clasificar_categoria(rel),
            "code": hits_code,
            "text": hits_text,
        }
    return resultados


def export_report(resultados: Dict[str, Dict[str, List[Dict]]], ruta_salida: str):
    total_code = sum(len(v["code"]) for v in resultados.values())
    total_text = sum(len(v["text"]) for v in resultados.values())
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
    with open(ruta_salida, "w", encoding="utf-8", newline="\n") as f:
        f.write("=============================================\n")
        f.write("REPORTE DE LLAMADAS NO CANÓNICAS A SlpCode\n")
        f.write("=============================================\n")
        f.write(f"Generado: {ahora}\n")
        f.write(f"Base: {RUTA_BASE}\n\n")
        f.write("Resumen:\n")
        f.write(f"  • Archivos con hallazgos: {len(resultados)}\n")
        f.write(f"  • Hallazgos en código   : {total_code}\n")
        f.write(f"  • Hallazgos en texto    : {total_text}\n\n")

        # Orden por categoría y ruta
        items = sorted(resultados.items(), key=lambda kv: (kv[1]["categoria"], kv[0]))
        current_cat = None
        for rel, data in items:
            if data["categoria"] != current_cat:
                current_cat = data["categoria"]
                f.write(f"\n## {current_cat}\n")

            f.write(f"\n--- {rel} ---\n")

            if data["code"]:
                f.write(">> Código (revisión obligatoria)\n")
                for h in data["code"]:
                    f.write(
                        f"  - [{h['type']}] L{h['line']:>4} C{h['col']:>3}  {h['snippet']}  → Sugerencia: {h['suggest']}\n"
                    )
            else:
                f.write(">> Código (sin hallazgos)\n")

            if data["text"]:
                f.write(">> Texto (opcional estandarizar)\n")
                for h in data["text"]:
                    clipped = (
                        (h["snippet"][:120] + "…")
                        if len(h["snippet"]) > 120
                        else h["snippet"]
                    )
                    f.write(
                        f"  - [{h['type']}] L{h['line']:>4} C{h['col']:>3}  {clipped}  → {h['suggest']}\n"
                    )
            else:
                f.write(">> Texto (sin hallazgos)\n")

    print(f"✅ Reporte generado: {ruta_salida}")
    print(
        f"   Archivos con hallazgos: {len(resultados)} | Código: {total_code} | Texto: {total_text}"
    )


if __name__ == "__main__":
    resultados = run_scan()
    export_report(resultados, REPORTE_SALIDA)
