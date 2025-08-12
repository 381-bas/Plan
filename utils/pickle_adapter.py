# utils/pickle_adapter.py
"""
Parquet adapter para pandas pickle, activado por BACKUP_FMT=parquet.
- DataFrame/Series → guarda/lee .parquet (pyarrow).
- Otros objetos → pickle normal (compat).
- Si pides leer X.pkl pero existe X.parquet, usa el parquet.
"""
from __future__ import annotations
import os
from pathlib import Path


def _repo_root_from_here() -> Path:
    p = Path(__file__).resolve()
    for parent in [p] + list(p.parents):
        if (parent / ".git").exists():
            return parent
    return Path(__file__).resolve().parent


def _load_dotenv_into_environ() -> None:
    root = _repo_root_from_here()
    env_file = root / ".env"
    if not env_file.exists():
        return
    try:
        for line in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip()
            os.environ.setdefault(k, v)
    except Exception:
        pass


def _get_backup_fmt() -> str:
    _load_dotenv_into_environ()
    val = (os.getenv("BACKUP_FMT") or "").strip().lower()
    return val or "parquet"  # por defecto parquet


def _enable_parquet_adapter() -> None:
    if _get_backup_fmt() != "parquet":
        return
    import pandas as pd
    from pandas import DataFrame, Series

    _real_to_pickle = pd.to_pickle
    _real_read_pickle = pd.read_pickle

    def _resolve_parquet_path(path_like) -> Path:
        return Path(path_like).with_suffix(".parquet")

    def _to_parquet_compat(obj, filepath, **kwargs):
        if isinstance(obj, DataFrame):
            p2 = _resolve_parquet_path(filepath)
            p2.parent.mkdir(parents=True, exist_ok=True)
            obj.to_parquet(p2, index=True)
            return
        if isinstance(obj, Series):
            p2 = _resolve_parquet_path(filepath)
            p2.parent.mkdir(parents=True, exist_ok=True)
            obj.to_frame(name=obj.name if obj.name is not None else "value").to_parquet(
                p2, index=True
            )
            return
        return _real_to_pickle(obj, filepath, **kwargs)

    def _read_parquet_compat(filepath, **kwargs):
        p2 = _resolve_parquet_path(filepath)
        if p2.exists():
            import pandas as pd

            return pd.read_parquet(p2)
        return _real_read_pickle(filepath, **kwargs)

    pd.to_pickle = _to_parquet_compat
    pd.read_pickle = _read_parquet_compat


_enable_parquet_adapter()
