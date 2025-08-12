#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SYMBIOS Risk-Lint (solo stdlib)
Escanea .py y detecta riesgos:
- Fechas: formatos sospechosos (%Y-%d-%m), to_datetime ambiguo (multilÃ­nea)
- SesiÃ³n: claves 'SlpCode' vs 'slpcode' (inconsistencias)
- Excepciones: 'except:' pelado
- Seguridad: eval/exec, pickle, yaml.load inseguro
- Paths: rutas absolutas (C:\, /home/â€¦), IO duro
- Pandas: DataFrame.append/Series.append (deprecado), asignaciones encadenadas, inplace=True

Genera: risklint_report.md
"""

from __future__ import annotations
import re
import os
from pathlib import Path
from typing import List, Tuple, Dict

# --- SYMBIOS: raÃ­z del repo y filtros de escaneo (sin rutas absolutas) ---
REPO_ROOT = Path(__file__).resolve().parent
ROOT = Path(os.getenv("SYMBIOS_ROOT", REPO_ROOT))

ALLOW_DIRS = {"components", "config", "core", "modulos", "motor", "services", "utils"}
EXCLUDE_DIRS = {
    "temp_ediciones",
    "__pycache__",
    "logs",
    "utils\\logs",
    "Informacion",
    ".github",
    ".venv",
    "backups",
    "gpt_engine",
}


def should_skip(p: Path) -> bool:
    parts_lower = [s.lower() for s in p.parts]
    if any(ex.lower() in parts_lower for ex in EXCLUDE_DIRS):
        return True
    if p.suffix != ".py":
        return True
    # si estÃ¡ en subcarpeta, exigir que sea una de las permitidas
    if len(p.parts) > 1 and p.parts[1].lower() not in {d.lower() for d in ALLOW_DIRS}:
        return True
    # no lint a los propios utilitarios symbios_* (opcional)
    if p.name.startswith("symbios_"):
        return True
    return False


# ---------- Define reglas ----------
Rule = Tuple[str, str, str]  # (id, severidad, regex)

RULES: List[Rule] = [
    # Fechas
    ("DATE_FMT_YDM", "HIGH", r"%Y-%d-%m"),
    (
        "DATE_FMT_DMY_STRPTIME",
        "MED",
        r"strptime\s*\([^,]+,\s*[\"']%d[-/ ]%m[-/ ]%Y[\"']",
    ),
    # Pandas to_datetime (detector base por lÃ­nea; se afina en escaneo multilÃ­nea)
    ("DATE_PANDAS_AMBIG", "MED", r"pd\.to_datetime\s*\([^)]*\)"),
    # SesiÃ³n
    ("SESSION_SlpCode_mixed", "HIGH", r"st\.session_state\.(SlpCode|slpcode)"),
    # Excepciones
    ("EXCEPT_BARE", "HIGH", r"except\s*:\s*(#.*)?$"),
    # Seguridad
    ("EVAL_EXEC", "CRITICAL", r"\b(eval|exec)\s*\("),
    ("PICKLE_LOAD", "HIGH", r"\bpickle\.load\s*\("),
    ("YAML_UNSAFE", "HIGH", r"\byaml\.load\s*\("),
    # Paths e IO
    ("PATH_ABS_WIN", "MED", r"[\"']([A-Za-z]:\\)[^\"']+[\"']"),
    ("PATH_ABS_UNIX", "MED", r"[\"'](/home/|/Users/|/var/)[^\"']+[\"']"),
    (
        "IO_HARDCODE",
        "MED",
        r"\b(open|pd\.read_(csv|excel|parquet)|to_csv|to_excel)\s*\([^)]*[\"'](C:\\|/home/|/Users/)[^\"']+[\"']",
    ),
    # Pandas deprecated afinado (evita list.append())
    ("PANDAS_APPEND", "HIGH", r"(?:\bdf\b|DataFrame|Series)\.append\s*\("),
    ("PANDAS_CHAIN_ASSIGN", "MED", r"\]\s*\[\s*['\"][^'\"]+['\"]\s*\]\s*="),
    ("PANDAS_INPLACE", "LOW", r"inplace\s*=\s*True"),
    # Mutables por defecto
    (
        "MUTABLE_DEFAULT",
        "HIGH",
        r"def\s+\w+\s*\([^)]*(=\s*\{\}|=\s*\[\]|=\s*dict\(\)|=\s*list\(\))",
    ),
]


# --- helpers para inspecciÃ³n multilÃ­nea de to_datetime ---
def _gather_call(lines: List[str], start_idx: int) -> str:
    """Devuelve el texto completo de la llamada que inicia en lines[start_idx] hasta balancear parÃ©ntesis."""
    buf, depth, started = [], 0, False
    for i in range(start_idx, len(lines)):
        line = lines[i]
        buf.append(line)
        for ch in line:
            if ch == "(":
                depth += 1
                started = True
            elif ch == ")":
                depth -= 1
        if started and depth <= 0:
            break
    return "\n".join(buf)


def _to_datetime_is_ambiguous(call_text: str) -> bool:
    lt = re.sub(r"\s+", "", call_text)
    # Si especifica format= o dayfirst=, NO es ambiguo
    return not ("format=" in lt or "dayfirst=" in lt)


# --- escaneo por archivo ---
def scan_file(path: Path) -> List[Dict]:
    findings = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return findings
    lines = text.splitlines()

    for i, line in enumerate(lines, 1):
        for rid, sev, rx in RULES:
            if rid == "DATE_PANDAS_AMBIG":
                if "pd.to_datetime" in line:
                    call = _gather_call(lines, i - 1)
                    if _to_datetime_is_ambiguous(call):
                        findings.append(
                            {
                                "rule": rid,
                                "sev": sev,
                                "file": str(path),
                                "line": i,
                                "code": line.strip(),
                            }
                        )
                continue
            if re.search(rx, line):
                findings.append(
                    {
                        "rule": rid,
                        "sev": sev,
                        "file": str(path),
                        "line": i,
                        "code": line.strip(),
                    }
                )
    return findings


def main() -> int:
    # Excluir utilitarios propios para no contaminar el conteo
    py_files = [p for p in ROOT.rglob("*.py") if p.is_file() and not should_skip(p)]
    all_findings: List[Dict] = []
    for f in py_files:
        all_findings.extend(scan_file(f))

    sev_order = {"CRITICAL": 0, "HIGH": 1, "MED": 2, "LOW": 3}
    all_findings.sort(key=lambda x: (sev_order.get(x["sev"], 9), x["file"], x["line"]))

    counts: Dict[str, int] = {}
    for it in all_findings:
        counts[it["sev"]] = counts.get(it["sev"], 0) + 1

    out_md = []
    out_md.append("# SYMBIOS Risk-Lint Report\n")
    out_md.append(f"Root: `{ROOT}`\n")
    out_md.append("## Resumen por severidad\n")
    for sev in ("CRITICAL", "HIGH", "MED", "LOW"):
        out_md.append(f"- {sev}: {counts.get(sev,0)}")
    out_md.append("\n---\n")

    def block(sev: str, title: str):
        items = [it for it in all_findings if it["sev"] == sev]
        if not items:
            out_md.append(f"## {title}\n_Sin hallazgos._\n")
            return
        out_md.append(f"## {title}\n")
        for it in items:
            out_md.append(f"- **{it['rule']}** Â· `{it['file']}:{it['line']}`")
            out_md.append(f"  ```py\n  {it['code']}\n  ```")
        out_md.append("")

    for sev, title in [
        ("CRITICAL", "CRITICAL"),
        ("HIGH", "HIGH"),
        ("MED", "MEDIUM"),
        ("LOW", "LOW"),
    ]:
        block(sev, title)

    report_path = ROOT / "risklint_report.md"
    report_path.write_text("\n".join(out_md), encoding="utf-8")
    print("[SYMBIOS] Risk-lint listo:", report_path)
    print("Resumen:", counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
