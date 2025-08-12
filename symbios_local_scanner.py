#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SYMBIOS Local Scanner (auto-config)
- Si se ejecuta SIN argumentos, usa rutas por defecto (CONFIG).
- Si se ejecuta CON argumentos, respeta --root y --out.
- Solo usa librerías estándar.

Salidas:
- summary.txt, tree.txt, inventory.json, duplicates_report.md, imports.csv
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# =========================
# CONFIG (ajusta si quieres)
# =========================
CONFIG = {
    "DEFAULT_ROOT": r"C:\Users\qmkbantiman\OneDrive - QMK SPA\GG\Python\Plan_Forecast",
    "DEFAULT_OUT_BASE": r"C:\Users\qmkbantiman\OneDrive - QMK SPA\GG\Python\DocS",
    "INCLUDE_TXT": True,  # escanear "8. Plan_unificado_p*" y otros .txt en busca de defs
    "STAMP_SUBFOLDER": True,  # crea subcarpeta "scan_YYYYmmdd_HHMMSS" dentro de OUT_BASE
}

# ==============
# Data classes
# ==============
@dataclass
class FunctionInfo:
    name: str
    lineno: int
    end_lineno: Optional[int]
    args: List[str]
    file: str
    is_method: bool = False
    class_name: Optional[str] = None
    source_hash: Optional[str] = None


@dataclass
class ClassInfo:
    name: str
    lineno: int
    end_lineno: Optional[int]
    methods: List[FunctionInfo]
    file: str


@dataclass
class FileInfo:
    path: str
    size: int
    mtime: float
    sha256: str
    type: str  # "py" | "txt" | "other"
    line_count: Optional[int] = None
    functions: List[FunctionInfo] = None  # for .py
    classes: List[ClassInfo] = None       # for .py
    imports: List[str] = None             # for .py
    txt_defs: List[str] = None            # for .txt


# ==============
# Helpers
# ==============
def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def normalize_source(text: str) -> str:
    # elimina espacios, tabs y líneas vacías para comparar fuentes similares
    return re.sub(r"\s+", "", text or "")


def read_file_text(p: Path, max_bytes: int = 5_000_000) -> str:
    data = p.read_bytes()
    if len(data) > max_bytes:
        data = data[:max_bytes]
    try:
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return data.decode("latin-1", errors="ignore")


def extract_python_info(p: Path) -> Tuple[List[FunctionInfo], List[ClassInfo], List[str]]:
    text = read_file_text(p)
    try:
        tree = ast.parse(text, filename=str(p))
    except Exception:
        return [], [], []
    functions: List[FunctionInfo] = []
    classes: List[ClassInfo] = []
    imports: List[str] = []
    lines = text.splitlines()

    class FuncVisitor(ast.NodeVisitor):
        def __init__(self, module_text: str, module_lines: List[str], file_path: str):
            self.module_text = module_text
            self.lines = module_lines
            self.file_path = file_path
            self.functions: List[FunctionInfo] = []
            self.classes: List[ClassInfo] = []
            self.imports: List[str] = []

        def visit_Import(self, node: ast.Import):
            for alias in node.names:
                self.imports.append(alias.name)

        def visit_ImportFrom(self, node: ast.ImportFrom):
            if node.module:
                self.imports.append(node.module)

        def visit_FunctionDef(self, node: ast.FunctionDef):
            args = [a.arg for a in node.args.args]
            src = self._get_src(node.lineno, getattr(node, "end_lineno", None))
            self.functions.append(
                FunctionInfo(
                    name=node.name,
                    lineno=node.lineno,
                    end_lineno=getattr(node, "end_lineno", None),
                    args=args,
                    file=self.file_path,
                    is_method=False,
                    class_name=None,
                    source_hash=sha256_bytes(normalize_source(src).encode("utf-8")) if src else None,
                )
            )

        def visit_ClassDef(self, node: ast.ClassDef):
            methods: List[FunctionInfo] = []
            for b in node.body:
                if isinstance(b, ast.FunctionDef):
                    args = [a.arg for a in b.args.args]
                    src = self._get_src(b.lineno, getattr(b, "end_lineno", None))
                    methods.append(
                        FunctionInfo(
                            name=b.name,
                            lineno=b.lineno,
                            end_lineno=getattr(b, "end_lineno", None),
                            args=args,
                            file=self.file_path,
                            is_method=True,
                            class_name=node.name,
                            source_hash=sha256_bytes(normalize_source(src).encode("utf-8")) if src else None,
                        )
                    )
            self.classes.append(
                ClassInfo(
                    name=node.name,
                    lineno=node.lineno,
                    end_lineno=getattr(node, "end_lineno", None),
                    methods=methods,
                    file=self.file_path,
                )
            )

        def _get_src(self, start: int, end: Optional[int]) -> str:
            if start is None:
                return ""
            if end is None:
                # intenta inferir hasta la próxima def/class o EOF
                end_guess = start
                for i in range(start, len(self.lines)):
                    line = self.lines[i - 1]
                    if i > start and re.match(r"^\s*(def|class)\s+", line):
                        break
                    end_guess = i
                end = end_guess
            try:
                return "\n".join(self.lines[start - 1 : end])
            except Exception:
                return ""

    v = FuncVisitor(text, lines, str(p))
    v.visit(tree)
    return v.functions, v.classes, v.imports


def extract_txt_defs(p: Path) -> List[str]:
    # busca firmas "def nombre(" en .txt (para Plan_unificado dumps)
    text = read_file_text(p)
    defs = re.findall(r"^\s*def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", text, flags=re.MULTILINE)
    return sorted(set(defs))


def build_inventory(root: Path, include_txt: bool = True) -> Dict:
    all_files: List[FileInfo] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        ftype = "other"
        if suffix == ".py":
            ftype = "py"
        elif suffix == ".txt" and include_txt:
            ftype = "txt"

        size = path.stat().st_size
        mtime = path.stat().st_mtime
        sha = sha256_bytes(path.read_bytes() if size <= 5_000_000 else b"")  # evita leer archivos enormes
        info = FileInfo(
            path=str(path),
            size=size,
            mtime=mtime,
            sha256=sha,
            type=ftype,
            line_count=None,
            functions=[],
            classes=[],
            imports=[],
            txt_defs=[],
        )
        if ftype in ("py", "txt"):
            text = read_file_text(path)
            info.line_count = len(text.splitlines())
        if ftype == "py":
            funcs, classes, imports = extract_python_info(path)
            info.functions = funcs
            info.classes = classes
            info.imports = imports
        elif ftype == "txt":
            info.txt_defs = extract_txt_defs(path)
        all_files.append(info)

    # Duplicados por hash de función
    dup_map: Dict[str, List[FunctionInfo]] = {}
    for fi in all_files:
        if fi.type != "py":
            continue
        for fn in fi.functions:
            if not fn.source_hash:
                continue
            dup_map.setdefault(fn.source_hash, []).append(fn)
        for cl in fi.classes:
            for m in cl.methods:
                if not m.source_hash:
                    continue
                dup_map.setdefault(m.source_hash, []).append(m)

    duplicates = {h: [asdict(f) for f in fns] for h, fns in dup_map.items() if len(fns) > 1}

    # Grafo de imports
    import_edges = []
    for fi in all_files:
        if fi.type != "py":
            continue
        src_mod = Path(fi.path).name
        for imp in fi.imports or []:
            import_edges.append((src_mod, imp))

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "root": str(root),
        "files": [asdict(f) for f in all_files],
        "duplicates": duplicates,
        "import_edges": import_edges,
        "stats": {
            "files_total": len(all_files),
            "py_files": sum(1 for f in all_files if f.type == "py"),
            "txt_files": sum(1 for f in all_files if f.type == "txt"),
            "other_files": sum(1 for f in all_files if f.type == "other"),
            "functions_total": sum(len(f.functions or []) for f in all_files if f.type == "py"),
            "classes_total": sum(len(f.classes or []) for f in all_files if f.type == "py"),
            "duplicates_groups": len(duplicates),
        },
    }


def write_reports(data: Dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    # JSON principal
    (out_dir / "inventory.json").write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    # Árbol simple (texto)
    root = Path(data["root"])
    lines = []
    for f in data["files"]:
        rel = str(Path(f["path"]).resolve())
        try:
            rel = str(Path(f["path"]).relative_to(root))
        except Exception:
            pass
        lines.append(f'{rel}  [{f["type"]}]  {f.get("line_count","-")} líneas')
    (out_dir / "tree.txt").write_text("\n".join(lines), encoding="utf-8")

    # Import edges CSV
    with open(out_dir / "imports.csv", "w", newline="", encoding="utf-8") as fp:
        w = csv.writer(fp)
        w.writerow(["source_file", "import_target"])
        w.writerows(data["import_edges"])

    # Duplicados (markdown)
    md = ["# Duplicados de funciones (por hash normalizado)\n"]
    if not data["duplicates"]:
        md.append("_No se detectaron duplicados exactos de funciones._\n")
    else:
        for h, items in data["duplicates"].items():
            md.append(f"## hash: `{h[:12]}...`\n")
            for it in items:
                where = f'{it["file"]}:{it["lineno"]}'
                owner = f'{it.get("class_name")+"." if it.get("class_name") else ""}{it["name"]}'
                md.append(f"- **{owner}** @ {where}")
            md.append("")
    (out_dir / "duplicates_report.md").write_text("\n".join(md), encoding="utf-8")

    # Resumen
    s = data["stats"]
    summary = f"""SYMBIOS Local Scanner — Resumen
==================================
Root: {data["root"]}
Generado: {data["generated_at"]}

Archivos: total={s["files_total"]} | py={s["py_files"]} | txt={s["txt_files"]} | other={s["other_files"]}
Funciones totales (py): {s["functions_total"]}
Clases totales (py):    {s["classes_total"]}
Grupos de duplicados:   {s["duplicates_groups"]}

Siguientes pasos sugeridos:
- Revisar 'duplicates_report.md' (eliminar o unificar funciones repetidas).
- Priorizar módulos con más funciones/líneas para pruebas.
- Usar 'imports.csv' para ver dependencias y hotspots.
"""
    (out_dir / "summary.txt").write_text(summary, encoding="utf-8")


# =========================
# API pública programática
# =========================
def run_scan(root_dir: str, out_base: str, include_txt: bool = True, stamp_subfolder: bool = True) -> Path:
    root = Path(root_dir).expanduser()
    if not root.exists():
        raise FileNotFoundError(f"No existe la carpeta raíz: {root}")
    out_dir = Path(out_base).expanduser()
    if stamp_subfolder:
        out_dir = out_dir / f"scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    data = build_inventory(root, include_txt=include_txt)
    write_reports(data, out_dir)
    return out_dir


# ==========
# Entrypoint
# ==========
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="SYMBIOS Local Scanner (auto-config)")
    ap.add_argument("--root", help="Carpeta raíz a escanear (ej. C:\\...\\Plan_Forecast)")
    ap.add_argument("--out", help="Carpeta base donde guardar reportes")
    ap.add_argument("--no-txt", action="store_true", help="No escanear .txt en busca de 'def ...'")
    ap.add_argument("--no-stamp", action="store_true", help="No crear subcarpeta con timestamp")
    args = ap.parse_args(argv)

    # Si faltan args, usar CONFIG por defecto
    root = args.root or CONFIG["DEFAULT_ROOT"]
    out_base = args.out or CONFIG["DEFAULT_OUT_BASE"]
    include_txt = CONFIG["INCLUDE_TXT"] if args.no_txt is False else False
    stamp = CONFIG["STAMP_SUBFOLDER"] if args.no_stamp is False else False

    print(f"[SYMBIOS] ROOT = {root}")
    print(f"[SYMBIOS] OUT_BASE = {out_base}  (stamp_subfolder={stamp})")
    print(f"[SYMBIOS] INCLUDE_TXT = {include_txt}")

    try:
        out_dir = run_scan(root, out_base, include_txt=include_txt, stamp_subfolder=stamp)
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        return 2

    print(f"[SYMBIOS] Listo. Reportes en: {out_dir}")
    for n in ["summary.txt", "tree.txt", "inventory.json", "duplicates_report.md", "imports.csv"]:
        print(f" - {out_dir / n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
