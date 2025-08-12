#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SYMBIOS Fixers (codemod seguro, v2)
- AUTO:
  * SESSION_READS:  st.session_state.slpcode (lectura) -> get_slpcode()
                    st.session_state["slpcode"] (lectura) -> get_slpcode()
    >>> inserta 'from session_utils import get_slpcode' si falta
  * BARE_EXCEPT:    'except:' -> 'except Exception as e:'
  * YAML_SAFE:      yaml.load(…) -> yaml.safe_load(…)
- REPORT (manual): .append/.ix/%Y-%d-%m/paths/pickle.load/pd.to_datetime ambiguo
"""

from __future__ import annotations
import re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
APPLY = "--apply" in sys.argv

AUTO_RULES = [
    # Lecturas estilo atributo (evita asignaciones con lookahead)
    ("SESSION_READS_ATTR",
     re.compile(r"st\.session_state\.slpcode(?!\s*=\s*)"), "get_slpcode()"),
    # Lecturas estilo índice ["slpcode"] (evita asignaciones)
    ("SESSION_READS_INDEX",
     re.compile(r"st\.session_state\[\s*['\"]slpcode['\"]\s*\](?!\s*=\s*)"), "get_slpcode()"),
    ("BARE_EXCEPT",
     re.compile(r"\bexcept\s*:\s*(#.*)?$"), "except Exception as e:"),
    ("YAML_SAFE",
     re.compile(r"\byaml\.load\s*\("), "yaml.safe_load("),
]

REPORT_RULES = [
    ("PANDAS_APPEND", re.compile(r"\.append\s*\(")),
    ("PANDAS_IX",     re.compile(r"\.ix\[")),
    ("DATE_FMT_YDM",  re.compile(r"%Y-%d-%m")),
    ("PATH_ABS",      re.compile(r"[\"']([A-Za-z]:\\|/home/|/Users/)[^\"']+[\"']")),
    ("PICKLE_LOAD",   re.compile(r"\bpickle\.load\s*\(")),
    ("PANDAS_TODT",   re.compile(r"pd\.to_datetime\s*\([^)]*\)")),
]

IMPORT_RX = re.compile(r"^\s*from\s+session_utils\s+import\s+get_slpcode\b", re.MULTILINE)

def _ensure_import(text: str) -> str:
    """Inserta 'from session_utils import get_slpcode' si no existe.
       Lo coloca tras el docstring inicial (si hay), o tras shebang/encoding."""
    if IMPORT_RX.search(text):
        return text
    # Detectar docstring al inicio
    m = re.match(r'^\s*(?:#.*\n|"""[\s\S]*?"""\n|\'\'\'[\s\S]*?\'\'\'\n)*', text)
    insert_at = m.end() if m else 0
    import_line = "from session_utils import get_slpcode\n"
    return text[:insert_at] + import_line + text[insert_at:]

def process_file(p: Path) -> dict:
    text = p.read_text(encoding="utf-8", errors="ignore")
    original = text
    changes = []
    replaced_any_session = False

    # AUTO replacements
    for name, rx, repl in AUTO_RULES:
        def _sub(m):
            nonlocal replaced_any_session
            if name.startswith("SESSION_READS"):
                replaced_any_session = True
            changes.append((name, m.group(0), repl))
            return repl
        text = rx.sub(_sub, text)

    # Si tocamos lecturas de sesión, asegurar import
    if replaced_any_session:
        new_text = _ensure_import(text)
        if new_text != text:
            changes.append(("INSERT_IMPORT", "from session_utils import get_slpcode", "added"))
            text = new_text

    # Guardar si APPLY
    if APPLY and text != original:
        p.write_text(text, encoding="utf-8")

    # REPORT matches
    reports = []
    for name, rx in REPORT_RULES:
        for m in rx.finditer(text):
            line = text[:m.start()].count("\n") + 1
            snippet = text.splitlines()[line-1].strip()
            if name == "PANDAS_TODT":
                ln = re.sub(r"\s+", "", snippet)
                if "format=" in ln or "dayfirst=" in ln:
                    continue
            reports.append((name, line, snippet))

    return {"file": str(p), "applied": changes, "reports": reports}

def main() -> int:
    py_files = [p for p in ROOT.rglob("*.py") if p.is_file() and p.name != "symbios_fixers.py"]
    total_applied = total_reports = 0
    results = []
    for p in py_files:
        r = process_file(p)
        results.append(r)
        total_applied += len(r["applied"])
        total_reports += len(r["reports"])

    print(f"[SYMBIOS][AUTO] cambios {'aplicados' if APPLY else 'propuestos'}: {total_applied}")
    for r in results:
        for name, old, new in r["applied"]:
            if name == "INSERT_IMPORT":
                print(f"  {name}: {r['file']}: {old} -> {new}")
            else:
                print(f"  {name}: {r['file']}: '{old}' -> '{new}'")
    print(f"[SYMBIOS][REPORT] hallazgos que requieren criterio: {total_reports}")
    for r in results:
        for name, line, snip in r["reports"]:
            print(f"  {name}: {r['file']}:{line}: {snip}")
    print("\nSugerencias:")
    print("- .append -> pd.concat([...], ignore_index=True)")
    print("- .ix -> .loc (revisar índices)")
    print("- pd.to_datetime: agrega format= o dayfirst=")
    print("- Evita rutas absolutas; usa config/ENV")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
