# session_utils.py
from __future__ import annotations
from pathlib import Path
from typing import Any
import io
import pickle
import pandas as pd
import numpy as np
import os
import time


def save_df_parquet_atomic(df: pd.DataFrame, path: str | Path) -> None:
    """
    Escritura atómica en Parquet: escribe a .tmp y luego replace.
    Logs: [PARQ.INFO]/[PARQ.ERROR] en una sola línea, sin emojis.
    """
    t0 = time.perf_counter()
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")

    print(f"[PARQ.INFO] write.start path={p} tmp={tmp}")
    try:
        # sin índice para que sea 100% tabular portable
        df.to_parquet(tmp, index=False)
        tmp_bytes = tmp.stat().st_size if tmp.exists() else None
        os.replace(tmp, p)  # atómico en mismo FS
        final_bytes = p.stat().st_size if p.exists() else None
        print(
            f"[PARQ.INFO] write.ok path={p} bytes={final_bytes} tmp_bytes={tmp_bytes} elapsed={time.perf_counter()-t0:.3f}s"
        )
    except Exception as e:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception as e2:
            print(
                f"[PARQ.ERROR] tmp_cleanup.fail path={tmp} err={e2.__class__.__name__}: {e2}"
            )
        print(
            f"[PARQ.ERROR] write.fail path={p} err={e.__class__.__name__}: {e} elapsed={time.perf_counter()-t0:.3f}s"
        )
        raise


def load_df_parquet_safe(path: str | Path, allowed_dir: str | Path) -> pd.DataFrame:
    """
    Lee Parquet sólo si `path` cae dentro de `allowed_dir`.
    Si el archivo no existe, retorna DataFrame vacío.
    Logs: [PARQ.INFO]/[PARQ.ERROR].
    """
    t0 = time.perf_counter()
    p = Path(path).resolve()
    base = Path(allowed_dir).resolve()
    print(f"[PARQ.INFO] load.start path={p} allowed_dir={base}")

    # Chequeo de contención robusto
    try:
        p.relative_to(base)
    except Exception:
        msg = f"Ruta no permitida para parquet: {p}"
        print(
            f"[PARQ.ERROR] disallowed_path path={p} base={base} msg={msg} elapsed={time.perf_counter()-t0:.3f}s"
        )
        raise ValueError(msg)

    try:
        if not p.exists():
            print(
                f"[PARQ.INFO] load.miss path={p} reason=not_found elapsed={time.perf_counter()-t0:.3f}s"
            )
            return pd.DataFrame()

        df = pd.read_parquet(p)
        rows, cols = df.shape
        print(
            f"[PARQ.INFO] load.ok path={p} shape=({rows},{cols}) elapsed={time.perf_counter()-t0:.3f}s"
        )
        return df
    except Exception as e:
        print(
            f"[PARQ.ERROR] load.fail path={p} err={e.__class__.__name__}: {e} elapsed={time.perf_counter()-t0:.3f}s"
        )
        raise


try:
    import streamlit as st
except Exception:  # permite importar sin Streamlit en tests

    class _Dummy:
        session_state = {}

    st = _Dummy()


def set_slpcode(value: int) -> None:
    """Set SlpCode de forma canónica (sin escribir la key legada)."""
    v = int(value)
    st.session_state["SlpCode"] = v


def get_slpcode(default: int = 999) -> int:
    """Lee SlpCode; permite fallback temporal a 'slpcode' (solo lectura)."""
    return int(
        st.session_state.get("SlpCode", st.session_state.get("slpcode", default))
    )


def normalize_slpcode_session(default: int = 999) -> None:
    """
    Asegura que 'SlpCode' exista en session_state:
    - si solo está 'slpcode' (legacy), lo copia a 'SlpCode'
    - si ninguna existe, setea default
    """
    ss = st.session_state
    if "SlpCode" not in ss:
        if "slpcode" in ss and ss.get("slpcode") not in (None, "", 0, "0"):
            try:
                ss["SlpCode"] = int(ss["slpcode"])
            except Exception:
                ss["SlpCode"] = default
        else:
            ss["SlpCode"] = default


class _RestrictedUnpickler(pickle.Unpickler):
    """
    Unpickler con lista blanca para deserializar objetos seguros (DF/Series/ndarray y builtins).
    Evita code execution vía pickle.
    """

    _ALLOWED = {
        ("builtins", "set"),
        ("builtins", "dict"),
        ("builtins", "list"),
        ("builtins", "tuple"),
        ("builtins", "str"),
        ("builtins", "int"),
        ("builtins", "float"),
        ("builtins", "bool"),
        ("builtins", "NoneType"),
        ("pandas.core.frame", "DataFrame"),
        ("pandas.core.series", "Series"),
        ("numpy", "ndarray"),
    }

    def find_class(self, module, name):
        if (module, name) in self._ALLOWED:
            return super().find_class(module, name)
        raise ValueError(f"Objeto no permitido al unpickle: {module}.{name}")


def restricted_unpickle(fp: io.BufferedReader) -> Any:
    """Carga ‘pickle’ con lista blanca de tipos permitidos."""
    return _RestrictedUnpickler(fp).load()


def normalize_df_for_hash(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Normaliza DF para hashing estable: columnas→str, orden filas/cols, ‘float64’ donde aplique.
    """

    df2 = df.copy()
    df2.columns = df2.columns.astype(str)
    df2 = df2.sort_index(axis=1).sort_index()
    df2 = df2.astype("float64", errors="ignore")
    return df2


DETALLE_KEYS: list[str] = ["ItemCode", "TipoForecast", "OcrCode3", "Mes", "CardCode"]

DETALLE_OBLIGATORIAS: list[str] = [
    "ItemCode",
    "TipoForecast",
    "OcrCode3",
    "Linea",
    "DocCur",
    "Mes",
    "FechEntr",
    "Cant",
    "PrecioUN",
    "CardCode",
    "SlpCode",
]


def ensure_mes_str2(s: pd.Series) -> pd.Series:
    """Mes en texto 2 dígitos ('01'..'12')."""
    return s.astype(str).str.zfill(2)


def fechentr_from_anio_mes(anio: int, mes):
    """
    Construye FechEntr como primer día del mes (date) a partir de `anio` y `mes`.
    `mes` puede ser escalar, lista, tupla o pd.Series; valores fuera de [1,12] o no numéricos -> mes=1.
    """
    # Llevar a Series para vectorizar
    s = mes if isinstance(mes, pd.Series) else pd.Series(mes)

    # Normalizar a 1..12 (coerce -> NaN -> 1)
    s_num = pd.to_numeric(s, errors="coerce")
    s_num = s_num.where((s_num >= 1) & (s_num <= 12))  # fuera de rango -> NaN
    s_num = s_num.fillna(1).astype(int)

    ym = (
        pd.Series(int(anio)).astype(str)
        + "-"
        + s_num.astype(int).astype(str).str.zfill(2)
    )
    dt = pd.to_datetime(ym + "-01", format="%Y-%m-%d", errors="coerce")
    return dt.dt.date


def attach_campos_largo(
    df_cambios: pd.DataFrame, df_largo: pd.DataFrame, anio: int
) -> pd.DataFrame:
    """
    Reinyecta campos base desde df_largo al df de cambios:
    - Merge por claves (ItemCode, TipoForecast, OcrCode3, Mes, CardCode) si están presentes,
      si no, por subset disponible y luego complementa.
    - Si FechEntr faltara tras el merge, la reconstruye desde anio+Mes.
    """
    df_c = df_cambios.copy()
    df_c["Mes"] = ensure_mes_str2(df_c["Mes"])
    # Determinar claves de merge según disponibilidad
    keys = [k for k in DETALLE_KEYS if k in df_c.columns and k in df_largo.columns]
    if not keys:
        # mínimo razonable
        keys = [
            k
            for k in ["ItemCode", "TipoForecast", "OcrCode3", "Mes"]
            if k in df_c.columns and k in df_largo.columns
        ]
    df_m = df_c.merge(
        df_largo[
            [c for c in df_largo.columns if c in (DETALLE_OBLIGATORIAS + DETALLE_KEYS)]
        ].drop_duplicates(),
        on=keys,
        how="left",
        suffixes=("", "_largo"),
    )

    # Si no viene FechEntr del merge, reconstruirla
    if "FechEntr" not in df_m.columns or df_m["FechEntr"].isna().all():
        df_m["FechEntr"] = fechentr_from_anio_mes(anio, df_m["Mes"])

    # Completar columnas faltantes con defaults seguros
    if "DocCur" not in df_m.columns:
        df_m["DocCur"] = "CLP"
    if "Linea" not in df_m.columns and "OcrCode3" in df_m.columns:
        # si no tienes mapeo global, mantén OcrCode3 como Linea
        df_m["Linea"] = df_m["OcrCode3"]

    if "PrecioUN" not in df_m.columns:
        df_m["PrecioUN"] = 0.0
    df_m["PrecioUN"] = pd.to_numeric(df_m["PrecioUN"], errors="coerce").fillna(0.0)

    # Mes 2 dígitos
    df_m["Mes"] = ensure_mes_str2(df_m["Mes"])

    return df_m


def ensure_detalle_schema(df: pd.DataFrame, anio: int) -> pd.DataFrame:
    """
    Asegura esquema requerido por inserción de detalle.
    - Tipos/campos mínimos.
    - Sin NaN en claves.
    """
    out = df.copy()
    # Campos faltantes -> añadir
    for col in DETALLE_OBLIGATORIAS:
        if col not in out.columns:
            if col == "FechEntr":
                out[col] = fechentr_from_anio_mes(anio, out["Mes"])
            elif col == "DocCur":
                out[col] = "CLP"
            elif col == "PrecioUN":
                out[col] = 0.0
            else:
                out[col] = np.nan

    # Normalizar tipos
    out["Mes"] = ensure_mes_str2(out["Mes"])
    out["Cant"] = pd.to_numeric(out["Cant"], errors="coerce").fillna(0.0)
    out["PrecioUN"] = pd.to_numeric(out["PrecioUN"], errors="coerce").fillna(0.0)
    # FechEntr ya es date; si viene string, forzar
    if out["FechEntr"].dtype == object:
        # Si ya viene en 'YYYY-MM' o 'YYYY-MM-DD', normalizamos a YYYY-MM-01
        fe = out["FechEntr"].astype(str).str.slice(0, 7)  # YYYY-MM
        out["FechEntr"] = pd.to_datetime(
            fe + "-01", format="%Y-%m-%d", errors="coerce"
        ).dt.date

    # Validar claves no nulas
    for k in ["ItemCode", "TipoForecast", "OcrCode3", "Mes", "CardCode"]:
        if out[k].isna().any():
            raise ValueError(f"[ensure_detalle_schema] Valores nulos en clave '{k}'")

    # Mantener sólo columnas necesarias + extras útiles si existen
    cols = [c for c in DETALLE_OBLIGATORIAS if c in out.columns]
    extras = [c for c in out.columns if c not in cols]  # se permiten extras
    return out[cols + extras]
