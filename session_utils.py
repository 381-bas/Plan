# session_utils.py
from __future__ import annotations
from pathlib import Path
from typing import Any
import io
import pickle
import pandas as pd
import numpy as np

# B_TMP001: Importaciones y configuraciÃ³n de base temporal para backups de ediciÃ³n
# # âˆ‚B_TMP001/âˆ‚B0


try:
    import streamlit as st
except Exception:  # permite importar sin Streamlit en tests

    class _Dummy:
        session_state = {}

    st = _Dummy()


def set_slpcode(value: int) -> None:
    """Set SlpCode de forma canÃ³nica y mantiene back-compat 'slpcode'."""
    v = int(value)
    st.session_state["SlpCode"] = v
    st.session_state["slpcode"] = v  # compat temporal


def get_slpcode(default: int = 999) -> int:
    """Lee SlpCode de forma robusta."""
    return int(
        st.session_state.get("SlpCode", st.session_state.get("slpcode", default))
    )


class _RestrictedUnpickler(pickle.Unpickler):
    """
    Unpickler con lista blanca para deserializar objetos seguros (DF/Series/ndarray y builtins).
    Evita code execution vÃ­a pickle.
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
    """Carga â€˜pickleâ€™ con lista blanca de tipos permitidos."""
    return _RestrictedUnpickler(fp).load()


def safe_pickle_load(path: str | Path, allowed_dir: str | Path) -> Any:
    """
    Carga un pickle sÃ³lo si â€˜pathâ€™ cae dentro de â€˜allowed_dirâ€™ y usando RestrictedUnpickler.
    """
    p = Path(path).resolve()
    base = Path(allowed_dir).resolve()
    if not str(p).startswith(str(base)):
        raise ValueError(f"Ruta no permitida para pickle: {p}")
    with open(p, "rb") as f:
        return restricted_unpickle(f)


def atomic_pickle_dump(obj: Any, path: str | Path) -> None:
    """
    Escritura atÃ³mica: dump a .tmp y luego replace -> sin archivos corruptos si hay fallo.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with open(tmp, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    tmp.replace(p)


def normalize_df_for_hash(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Normaliza DF para hashing estable: columnasâ†’str, orden filas/cols, â€˜float64â€™ donde aplique.
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
    """Mes en texto 2 dÃ­gitos ('01'..'12')."""
    return s.astype(str).str.zfill(2)


def fechentr_from_anio_mes(anio: int, mes):
    """
    Construye FechEntr como primer dÃ­a del mes (date) a partir de `anio` y `mes`.
    `mes` puede ser escalar, lista, tupla o pd.Series; valores fuera de [1,12] o no numÃ©ricos -> mes=1.
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
    - Merge por claves (ItemCode, TipoForecast, OcrCode3, Mes, CardCode) si estÃ¡n presentes,
      si no, por subset disponible y luego complementa.
    - Si FechEntr faltara tras el merge, la reconstruye desde anio+Mes.
    """
    df_c = df_cambios.copy()
    df_c["Mes"] = ensure_mes_str2(df_c["Mes"])
    # Determinar claves de merge segÃºn disponibilidad
    keys = [k for k in DETALLE_KEYS if k in df_c.columns and k in df_largo.columns]
    if not keys:
        # mÃ­nimo razonable
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
        # si no tienes mapeo global, mantÃ©n OcrCode3 como Linea
        df_m["Linea"] = df_m["OcrCode3"]

    if "PrecioUN" not in df_m.columns:
        df_m["PrecioUN"] = 0.0
    df_m["PrecioUN"] = pd.to_numeric(df_m["PrecioUN"], errors="coerce").fillna(0.0)

    # Mes 2 dÃ­gitos
    df_m["Mes"] = ensure_mes_str2(df_m["Mes"])

    return df_m


def ensure_detalle_schema(df: pd.DataFrame, anio: int) -> pd.DataFrame:
    """
    Asegura esquema requerido por inserciÃ³n de detalle.
    - Tipos/campos mÃ­nimos.
    - Sin NaN en claves.
    """
    out = df.copy()
    # Campos faltantes -> aÃ±adir
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

    # Mantener sÃ³lo columnas necesarias + extras Ãºtiles si existen
    cols = [c for c in DETALLE_OBLIGATORIAS if c in out.columns]
    extras = [c for c in out.columns if c not in cols]  # se permiten extras
    return out[cols + extras]
