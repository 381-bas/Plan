# B_FCS001: Importaciones y configuración de base de datos para consultas forecast
# # ∂B_FCS001/∂B0
from __future__ import annotations
import pandas as pd
import time
import numpy as np
import streamlit as st
from pathlib import Path
from typing import Tuple, Optional, Any, Dict, Literal, Set
from datetime import datetime
from contextlib import nullcontext
import unicodedata as _ud
from hashlib import sha256
from pandas.util import hash_pandas_object
from session_utils import (
    normalize_df_for_hash,
    load_df_parquet_safe,
    save_df_parquet_atomic,
)

from utils.db import (
    _run_forecast_insert_get_id,
    _run_log_to_sql,
    DB_PATH,
    run_query,
    _read_sql,
    add_year_filter,
    tx,
    deduplicar_detalle_por_clave,
)

with tx(DB_PATH, immediate=True) as conn:
    deduplicar_detalle_por_clave(conn)

from utils.repositorio_forecast.repositorio_forecast_editor import get_key_buffer


# modulos/editor_forecast.py (arriba)
BASE_IDX = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
MESES = [f"{m:02d}" for m in range(1, 13)]
EXTRAS_FIJOS = ["ItemName", "DocCur"]  # puedes agregar "Linea" si la usas
COLS_REQUERIDAS = BASE_IDX + EXTRAS_FIJOS + MESES


# B_INS001: Preparación de payload de guardado desde UI 4x12
# # ∂B_INS001/∂B0
def preparar_payload_guardado(
    df_ui: pd.DataFrame,
    *,
    slpcode: int,
    cardcode: str,
    anio: int,
    db_path: str,
    persist_mode: Literal["ui_state", "delta"] = "ui_state",
) -> Dict[str, Any]:
    if persist_mode != "ui_state":
        print(
            f"[PREP.WARN] persist_mode='{persist_mode}' no soportado; forzando 'ui_state'"
        )
        persist_mode = "ui_state"

    df_long = _melt_ui_4x12(df_ui)
    # Normalizaciones duras
    df_long["Cant"] = (
        pd.to_numeric(df_long["Cant"], errors="coerce").fillna(0).astype(int)
    )
    df_long["PrecioUN"] = pd.to_numeric(df_long["PrecioUN"], errors="coerce")
    df_long["DocCur"] = df_long["DocCur"].fillna("CLP")
    df_long["Linea"] = df_long["Linea"].fillna("Trader")

    # Solo lo visible >0
    to_insert = df_long[df_long["Cant"] > 0].copy()
    to_insert["SlpCode"] = int(slpcode)
    to_insert["CardCode"] = str(cardcode)
    to_insert["FechEntr"] = to_insert["Mes"].map(lambda m: f"{anio}-{m}-01")

    # Histórico previo para Δ/bajadas
    f_prev = _get_forecast_id_prev(slpcode, cardcode, anio, db_path=db_path)
    if f_prev is not None:
        hist = _leer_detalle_forecast(f_prev, db_path)
        # último por FechEntr (política "last")
        if not hist.empty and "FechEntr" in hist.columns:
            hist = hist.sort_values("FechEntr").drop_duplicates(
                subset=["ItemCode", "TipoForecast", "OcrCode3", "Mes"], keep="last"
            )
        hist = hist[["ItemCode", "TipoForecast", "OcrCode3", "Mes", "Cant"]].copy()
        hist = hist.rename(columns={"Cant": "Cant_Anterior"})
    else:
        hist = pd.DataFrame(
            columns=["ItemCode", "TipoForecast", "OcrCode3", "Mes", "Cant_Anterior"]
        )

    # diag para log (outer merge)
    diag = to_insert.merge(
        hist,
        how="outer",
        on=["ItemCode", "TipoForecast", "OcrCode3", "Mes"],
        indicator=True,
    )
    diag["Cant"] = pd.to_numeric(diag["Cant"], errors="coerce").fillna(0).astype(int)
    diag["Cant_Anterior"] = (
        pd.to_numeric(diag["Cant_Anterior"], errors="coerce").fillna(0).astype(int)
    )
    diag["Delta"] = diag["Cant"] - diag["Cant_Anterior"]

    # Bajadas >0→0: presentes en hist y ausentes en to_insert, o Δ<0 hasta 0
    left = hist.merge(
        to_insert[["ItemCode", "TipoForecast", "OcrCode3", "Mes"]],
        how="left",
        on=["ItemCode", "TipoForecast", "OcrCode3", "Mes"],
        indicator=True,
    )
    bajadas = left[(left["_merge"] == "left_only") & (left["Cant_Anterior"] > 0)].copy()
    bajadas = bajadas.drop(columns=["_merge"])

    n_ui_gt_zero = int(len(to_insert))
    n_bajadas = int(len(bajadas))
    print(
        f"[PREP.INFO] ui_gt_zero={n_ui_gt_zero} | bajadas={n_bajadas} | prev_id={f_prev}"
    )

    return {
        "to_insert": to_insert,
        "diag": diag,
        "bajadas": bajadas,
        "meta": {
            "slpcode": slpcode,
            "cardcode": cardcode,
            "anio": anio,
            "n_ui_gt_zero": n_ui_gt_zero,
            "n_bajadas": n_bajadas,
            "forecast_id_prev": f_prev,
        },
    }


# B_SYN001: Persistencia transaccional de cambios del cliente
# # ∂B_SYN001/∂B0
def persistir_cambios_cliente(
    slpcode: int,
    cardcode: str,
    anio: int,
    df_ui_editado: pd.DataFrame,
    db_path: str,
    key_buffer: str,
) -> dict:
    """
    Persiste cambios del cliente en TX, refresca buffer UI y devuelve payload consistente.
    Retorno:
      { "ok": True,  "forecast_id": int, "insertadas": int, "rows": int, "warnings": list }
      { "ok": False, "error": str,      "warnings": list }
    """
    warnings: list[str] = []

    # --- helper interno: sanity 4×12 permitiendo múltiples bases (grupos) ---
    def _is_4x12_multibase(df: pd.DataFrame) -> tuple[bool, int, str]:
        if df is None or df.empty:
            return True, 0, "empty"
        meses = [f"{m:02d}" for m in range(1, 13)]
        if not set(meses).issubset(df.columns):
            return False, 0, "missing_month_cols"

        # Estimar cantidad de “bases” (grupos). Preferimos _GKEY si viene del rebuild.
        bases = 1
        if "_GKEY" in df.columns:
            try:
                g = int(pd.Series(df["_GKEY"]).nunique())
                bases = max(1, g)
            except Exception:
                pass
        else:
            # Fallback robusto: si las filas son múltiplo de 4, asumimos N bases
            rows = int(df.shape[0])
            if rows % 4 == 0:
                bases = max(1, rows // 4)

        # Condición mínima: filas = 4 × bases
        ok = df.shape[0] == 4 * bases
        return ok, bases, "ok" if ok else "rows_not_4xbases"

    try:
        # 1) Validación estructural del 4×12 (con tus reglas actuales)
        validar_forecast_dataframe(df_ui_editado)

        # 2) Preparar payload largo con reglas de negocio
        payload = preparar_payload_guardado(
            df_ui=df_ui_editado,
            slpcode=slpcode,
            cardcode=cardcode,
            anio=anio,
            db_path=db_path,
        )
        warnings = payload.get("warnings", []) or []
        df_largo = payload.get("to_insert")
        if df_largo is None:
            # Asegurar DataFrame vacío válido si no hay filas
            df_largo = pd.DataFrame(
                columns=[
                    "CardCode",
                    "ItemCode",
                    "TipoForecast",
                    "Métrica",
                    "OcrCode3",
                    "FechEntr",
                    "Valor",
                    "DocCur",
                ]
            )

        forecast_id_prev = payload.get("meta", {}).get("forecast_id_prev")

        # 3) Crear SIEMPRE nueva cabecera activa (firma compatible)
        #    Intento 1: firma rica (slpcode, cardcode, anio, db_path, force_new)
        #    Intento 2: firma corta (slpcode, db_path [,+ force_new])
        #    Intento 3: registrar_forecast_cabecera(slpcode, db_path)
        try:
            forecast_id = obtener_forecast_activo(  # type: ignore
                slpcode=slpcode,
                cardcode=cardcode,
                anio=anio,
                db_path=db_path,
                force_new=True,
            )
        except TypeError:
            try:
                # posición: evita kwargs no soportados
                try:
                    forecast_id = obtener_forecast_activo(slpcode, db_path)  # type: ignore
                except TypeError:
                    forecast_id = obtener_forecast_activo(slpcode, db_path, True)  # type: ignore
            except Exception:
                # último recurso
                forecast_id = registrar_forecast_cabecera(
                    slpcode=slpcode, db_path=db_path
                )

        # 4) Auditoría de cambios
        registrar_log_detalle_cambios(
            slpcode=slpcode,
            cardcode=cardcode,
            anio=anio,
            df_largo=payload.get("diag"),
            db_path=db_path,
            forecast_id=forecast_id,
            forecast_id_anterior=forecast_id_prev,
        )

        # 5) Insert detalle (replace=True)
        n_ins = insertar_forecast_detalle(
            forecast_id=forecast_id,
            anio=anio,
            df_in=df_largo,
            db_path=db_path,
            replace=True,
        )

        # 6) Refrescar buffers UI y sanity 4×12 tolerante a múltiples bases
        df_ref = _refrescar_buffer_ui(
            forecast_id=forecast_id, key_buffer=key_buffer, db_path=db_path
        )

        ok_multi, bases_est, motivo = _is_4x12_multibase(df_ref)
        try:
            verify_4x12(df_ref, bases=max(1, bases_est))
        except Exception:
            if ok_multi:
                print(
                    f"[UI.VERIFY.INFO] Rebuild multi-base aceptado — bases={bases_est} motivo='{motivo}'"
                )
                warnings.append("UI_REBUILD_NOT_4X12")  # notificación suave
            else:
                print(f"[UI.VERIFY.FAIL] bases_est={bases_est} motivo='{motivo}'")
                # En este caso el rebuild no cumple ni el mínimo; lo marcamos como warning pero NO rompemos TX,
                # pues la escritura en BD ya fue correcta.
                warnings.append("UI_REBUILD_NOT_4X12")

        # 7) Limpieza suave de marca en sesión (best-effort)
        try:
            pend = st.session_state.get("clientes_editados", set())
            if isinstance(pend, set) and cardcode in pend:
                pend.discard(cardcode)
                st.session_state["clientes_editados"] = pend
        except Exception:
            pass

        # 8) Retorno consistente
        return {
            "ok": True,
            "forecast_id": int(forecast_id),
            "insertadas": int(n_ins) if n_ins is not None else 0,
            "rows": int(n_ins) if n_ins is not None else 0,  # compat
            "warnings": warnings,
        }

    except Exception as e:
        print(f"[TX.CLIENT.ERROR] cc={cardcode} err={e}")
        try:
            # Si usas conexiones manuales, realiza ROLLBACK aquí
            pass
        except Exception as ee:
            print(f"[TX.ROLLBACK.WARN] {ee}")

        return {
            "ok": False,
            "error": str(e),
            "warnings": warnings,
        }


# B_INS002: Inserción de detalle forecast (largo) en BD con replace opcional
# # ∂B_INS002/∂B0
def insertar_forecast_detalle(
    forecast_id: int,
    anio: int,
    df_in: pd.DataFrame,
    *,
    db_path: str,
    replace: bool = True,
) -> int:

    print("[PFD-007] formato detectado -> largo=True")
    if df_in is None or df_in.empty:
        # Detalle vacío es válido (todo quedó en 0)
        if replace:
            with tx(db_path, immediate=True) as con:
                con.execute(
                    "DELETE FROM Forecast_Detalle WHERE ForecastID = ?", (forecast_id,)
                )
        print("[PFD-007] detalle vacío (Cant>0 = 0) | filas_insertadas=0")
        return 0

    df = df_in.copy()
    # Normalizaciones y defensas
    df["Cant"] = pd.to_numeric(df["Cant"], errors="coerce").fillna(0).astype(int)
    df = df[df["Cant"] > 0].copy()  # SOLO >0
    if df.empty:
        if replace:
            with tx(db_path, immediate=True) as con:
                con.execute(
                    "DELETE FROM Forecast_Detalle WHERE ForecastID = ?", (forecast_id,)
                )
        print("[PFD-007] SOLO ceros tras filtro | filas_insertadas=0")
        return 0

    df["Mes"] = df["Mes"].astype(str).str.zfill(2)
    # Normaliza SIEMPRE FechEntr a cadena ISO 'YYYY-MM-01' para SQLite.
    # 1) Intentamos parsear lo que venga; 2) rellenamos con Mes cuando falte; 3) formateamos a str.
    if "FechEntr" not in df.columns:
        df["FechEntr"] = np.nan
    _dt = pd.to_datetime(df["FechEntr"], errors="coerce")
    _fallback = pd.to_datetime(df["Mes"].radd(f"{anio}-") + "-01", errors="coerce")
    df["FechEntr"] = _dt.fillna(_fallback).dt.strftime("%Y-%m-%d")
    df["Linea"] = df.get("Linea", "Trader")
    if isinstance(df["Linea"], pd.Series):
        df["Linea"] = df["Linea"].fillna("Trader")
    df["DocCur"] = df.get("DocCur", "CLP")
    if isinstance(df["DocCur"], pd.Series):
        df["DocCur"] = df["DocCur"].fillna("CLP")
    df["PrecioUN"] = pd.to_numeric(df.get("PrecioUN", 0), errors="coerce").fillna(0.0)

    cols = [
        "ForecastID",
        "SlpCode",
        "CardCode",
        "ItemCode",
        "TipoForecast",
        "OcrCode3",
        "Linea",
        "FechEntr",
        "Cant",
        "PrecioUN",
        "DocCur",
    ]
    df["ForecastID"] = int(forecast_id)
    df = df.assign(
        SlpCode=df["SlpCode"].astype(int),
        CardCode=df["CardCode"].astype(str),
        ItemCode=df["ItemCode"].astype(str),
        TipoForecast=df["TipoForecast"].astype(str),
        OcrCode3=df["OcrCode3"].astype(str),
        DocCur=df["DocCur"].astype(str),
        Linea=df["Linea"].astype(str),
        PrecioUN=df["PrecioUN"].astype(float),
        Cant=df["Cant"].astype(int),
    )
    rows = [tuple(x) for x in df[cols].itertuples(index=False, name=None)]

    # Delete-replace por ForecastID
    with tx(db_path, immediate=True) as con:
        cur = con.cursor()
        if replace:
            cur.execute(
                "DELETE FROM Forecast_Detalle WHERE ForecastID = ?", (forecast_id,)
            )
            print("[PFD-007] detalle previo eliminado (replace=True)")
        cur.executemany(
            """
            INSERT INTO Forecast_Detalle
            (ForecastID, SlpCode, CardCode, ItemCode, TipoForecast, OcrCode3, Linea, FechEntr, Cant, PrecioUN, DocCur)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
            rows,
        )

    print(f"[PFD-007] ✅ insert completado | filas_insertadas={len(rows)}")
    return len(rows)


# B_BUF001: Reconstrucción/refresco del buffer 4x12 desde BD
# # ∂B_BUF001/∂B0
def _refrescar_buffer_ui(
    forecast_id: int,
    *,
    key_buffer: str,
    db_path: str,
) -> pd.DataFrame:
    """
    Reconstruye el buffer 4×12 desde BD (solo >0) y completa faltantes en memoria.
    Si el detalle viene vacío, fuerza 4×12 con meses en 0 tomando la estructura del buffer previo.
    """
    print(f"[RFE-005] refresh from forecast_id={forecast_id}")
    df_db = _leer_detalle_forecast(forecast_id, db_path)
    print(f"[RFE-005] raw result shape={df_db.shape}")

    # Intentar recuperar el buffer previo para preservar columnas extra y estructura
    has_prev = key_buffer in st.session_state and isinstance(
        st.session_state.get(key_buffer), pd.DataFrame
    )
    df_prev = st.session_state.get(key_buffer).copy() if has_prev else None

    # 1) Reconstrucción principal con make_4x12_from (espera largo escaso >0)
    df4 = None
    try:
        df4 = make_4x12_from(
            df_db
        )  # debe devolver 4×12 completo (Cantidad/Precio × Firme/Proyectado)
    except Exception as e:
        print(f"[RFE-005.WARN] make_4x12_from falló: {e} -> se intentará fallback")

    # 2) Fallback si make_4x12_from no devolvió nada válido o df_db está vacío
    if df4 is None or not isinstance(df4, pd.DataFrame) or df4.empty:
        print(
            "[RFE-005.INFO] no hay detalle >0 en BD para este forecast_id -> fallback 4×12"
        )
        if has_prev:
            # Usar la misma estructura del buffer previo, pero con meses en 0
            df4 = df_prev.copy()
            meses = [f"{i:02d}" for i in range(1, 13) if f"{i:02d}" in df4.columns]
            if meses:
                df4.loc[:, meses] = 0
            # Garantizar columnas clave presentes
            for col in ("DocCur", "OcrCode3"):
                if col not in df4.columns:
                    df4[col] = "CLP" if col == "DocCur" else "Trader"
        else:
            # Crear esqueleto mínimo 4×12 (sin inflar BD). Defaults seguros.
            meses = [f"{i:02d}" for i in range(1, 13)]
            df4 = pd.DataFrame(
                [
                    {
                        "TipoForecast": "Firme",
                        "Métrica": "Cantidad",
                        "OcrCode3": "Trader",
                        "DocCur": "CLP",
                        **{m: 0 for m in meses},
                    },
                    {
                        "TipoForecast": "Firme",
                        "Métrica": "Precio",
                        "OcrCode3": "Trader",
                        "DocCur": "CLP",
                        **{m: 0 for m in meses},
                    },
                    {
                        "TipoForecast": "Proyectado",
                        "Métrica": "Cantidad",
                        "OcrCode3": "Trader",
                        "DocCur": "CLP",
                        **{m: 0 for m in meses},
                    },
                    {
                        "TipoForecast": "Proyectado",
                        "Métrica": "Precio",
                        "OcrCode3": "Trader",
                        "DocCur": "CLP",
                        **{m: 0 for m in meses},
                    },
                ]
            )
            # Orden amable si existen estas columnas
            col_orden = [
                "ItemCode",
                "ItemName",
                "TipoForecast",
                "OcrCode3",
                "DocCur",
                "Métrica",
            ] + meses
            df4 = df4.reindex(columns=[c for c in col_orden if c in df4.columns])

    # 3) Preservar columnas extra del buffer previo (si existían) sin pisar los meses
    if has_prev and isinstance(df_prev, pd.DataFrame):
        extras = [
            c
            for c in df_prev.columns
            if c not in df4.columns and not (isinstance(c, str) and c.isdigit())
        ]
        if extras:
            try:
                for col in extras:
                    df4[col] = (
                        df_prev[col].iloc[: len(df4)].values
                        if len(df_prev) >= len(df4)
                        else df_prev[col].values.tolist()
                        + [None] * (len(df4) - len(df_prev))
                    )
                print(f"[RFE-005.INFO] extras_preservadas={extras}")
            except Exception as e:
                print(f"[RFE-005.WARN] no se pudieron preservar extras: {e}")

    # 4) Orden estable y tipado mínimo
    if {"ItemCode", "TipoForecast", "Métrica"}.issubset(df4.columns):
        df4 = df4.sort_values(
            ["ItemCode", "TipoForecast", "Métrica"], na_position="last"
        ).reset_index(drop=True)
    meses = [f"{i:02d}" for i in range(1, 13) if f"{i:02d}" in df4.columns]
    for m in meses:
        df4[m] = pd.to_numeric(df4[m], errors="coerce").fillna(0.0)

    # 5) Persistir en sesión (buffer y espejo) + basehash/edithash estables
    try:
        # Guarda buffer y espejo inicial (estado recién reconstruido)
        st.session_state[key_buffer] = df4.copy()
        st.session_state[f"{key_buffer}_editado"] = df4.copy()

        # Canon para hashing: solo columnas mensuales "01".."12", normalizadas.
        # Usamos tu normalize_df_for_hash para asegurar orden/dtypes/NaN consistentes.
        def _canon_for_hash(df: pd.DataFrame) -> pd.DataFrame:
            meses = [
                c
                for c in df.columns
                if isinstance(c, str) and c.isdigit() and len(c) == 2
            ]
            base = df.loc[:, meses].copy()
            return normalize_df_for_hash(base)

        # Hash estable como entero (coherente con backups temporales)
        basehash = int(
            hash_pandas_object(
                _canon_for_hash(st.session_state[key_buffer]), index=True
            ).sum()
        )
        edithash = int(
            hash_pandas_object(
                _canon_for_hash(st.session_state[f"{key_buffer}_editado"]), index=True
            ).sum()
        )

        st.session_state[f"{key_buffer}_basehash"] = basehash
        st.session_state[f"{key_buffer}_edithash"] = edithash
        st.session_state[f"{key_buffer}_changed"] = edithash != basehash  # parte False

    except Exception as e:
        print(f"[RFE-005.WARN] no session available / hash calc: {e}")

    # 6) Verificación 4×12 estricta (soporta múltiples bases)
    try:
        if VERIFY_4X12_STRICT:  # type: ignore[name-defined]
            meses_ok = all(f"{i:02d}" in df4.columns for i in range(1, 13))
            filas_ok = (len(df4) % 4) == 0
            if not (meses_ok and filas_ok):
                raise RuntimeError("UI_REBUILD_NOT_4X12")
    except NameError:
        # si la flag no existe, no bloquear
        pass

    # --- trazas post (clave real usada)
    try:
        in_session = key_buffer in st.session_state
        in_mirror = f"{key_buffer}_editado" in st.session_state
        print(
            f"[RFE-005.POST] key_buffer='{key_buffer}' in_session={in_session} mirror_in_session={in_mirror}"
        )
    except Exception:
        pass

    return df4


# B_DBQ001: Consulta consolidada del histórico forecast por día
# # ∂B_DBQ001/∂B0
def forecast_prev(
    slpcode: int,
    cardcode: str,
    anio: int,
    *,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Histórico del año: consolida por día y tipo, usando límites [anio-01-01, (anio+1)-01-01).
    Sin JOIN innecesario a Forecast; todo lo que necesitamos está en Forecast_Detalle.
    """
    anio = int(anio)
    ini = f"{anio:04d}-01-01"
    fin = f"{anio + 1:04d}-01-01"

    sql = """
    SELECT
        d.ForecastID,
        d.ItemCode,
        d.CardCode,
        d.TipoForecast,
        d.OcrCode3,
        date(d.FechEntr) AS FechEntrDate,
        SUM(CASE WHEN d.TipoForecast='Firme'      THEN COALESCE(d.Cant,0)    ELSE 0 END) AS Cant_Firme,
        SUM(CASE WHEN d.TipoForecast='Proyectado' THEN COALESCE(d.Cant,0)    ELSE 0 END) AS Cant_Proy,
        AVG(CASE WHEN d.TipoForecast='Firme'      THEN NULLIF(d.PrecioUN,0)  END)        AS Precio_Firme,
        AVG(CASE WHEN d.TipoForecast='Proyectado' THEN NULLIF(d.PrecioUN,0)  END)        AS Precio_Proy,
        MAX(COALESCE(d.DocCur,'CLP')) AS DocCur
    FROM Forecast_Detalle d
    WHERE
        d.SlpCode  = ? AND
        d.CardCode = ? AND
        date(d.FechEntr) >= date(?) AND
        date(d.FechEntr)  < date(?)
    GROUP BY
        d.ForecastID, d.ItemCode, d.CardCode, d.TipoForecast, d.OcrCode3, date(d.FechEntr)
    ORDER BY FechEntrDate ASC;
    """
    return run_query(sql, db_path, (slpcode, cardcode, ini, fin))


# Función 5
MESES = [f"{i:02d}" for i in range(1, 13)]
MONEDAS_VALIDAS_DEFAULT: Set[str] = {
    "CLP",
    "USD",
    "EUR",
    "MXN",
    "COP",
    "PEN",
    "BRL",
    "ARS",
    "UYU",
    "BOB",
}


# B_VFD001: Validación dura del buffer UI 4x12
# # ∂B_VFD001/∂B0
def validar_buffer_ui(
    df_ui_editado: pd.DataFrame, *, monedas_validas: Optional[Set[str]] = None
) -> Dict[str, Any]:
    """
    Valida el 4×12 proveniente de la UI y SIEMPRE retorna un dict:
    {"ok": bool, "errores": list[str], "warnings": list[str]}
    """
    print("[VAL.UI.INFO] inicio validación 4×12")

    errores: list[str] = []
    warnings: list[str] = []

    # Monedas válidas por defecto
    if monedas_validas is None:
        try:
            monedas_validas = MONEDAS_VALIDAS_DEFAULT
        except NameError:
            monedas_validas = {"CLP", "USD", "EUR"}

    # 0) DF válido
    if df_ui_editado is None or not isinstance(df_ui_editado, pd.DataFrame):
        errores.append("DataFrame de UI inválido o None")
        print("[VAL.UI.ERROR] DataFrame de UI inválido o None")
        return {"ok": False, "errores": errores, "warnings": warnings}

    # Copia segura + reset de índice si es MultiIndex
    df = df_ui_editado.copy()
    if isinstance(df.index, pd.MultiIndex):
        print("[VAL.UI.INFO] MultiIndex detectado → reset_index()")
        df = df.reset_index()

    # 1) Validación estructural/dominio usando tu validador base
    try:
        res = validar_forecast_dataframe(df)
        if isinstance(res, list) and len(res) > 0:
            errores.extend(res)
    except Exception as e:
        # Si el validador base lanza, lo capturamos como error bloqueante
        msg = f"validar_forecast_dataframe lanzó excepción: {e}"
        errores.append(msg)
        print(f"[VAL.UI.ERROR] {msg}")

    # 2) Reglas adicionales: DocCur permitido
    if "DocCur" in df.columns:
        cur = df["DocCur"].astype("string").str.upper()
        no_permit = cur[~cur.isin(sorted(monedas_validas))]
        if not no_permit.empty:
            unq = sorted(set(no_permit.dropna().tolist()))
            errores.append(
                f"DocCur contiene monedas no permitidas: {unq} (permitidas: {sorted(monedas_validas)})"
            )
            print(f"[VAL.UI.ERROR] DocCur fuera de catálogo: {unq}")

    # 3) Resultado
    ok = len(errores) == 0
    print(f"[VAL.UI.RESULT] ok={ok} errores={len(errores)} warnings={len(warnings)}")
    return {"ok": ok, "errores": errores, "warnings": warnings}


# B_VFD002: Verificación estricta 4x12 posterior al refresco
# # ∂B_VFD002/∂B0
def verify_4x12(df_ui: pd.DataFrame, bases: Optional[int] = None) -> None:
    """
    Verificación flexible post-refresh. NO lee ni escribe session_state.
    Requisitos mínimos:
      - df_ui es DataFrame
      - 12 columnas mes "01".."12"
      - número de filas múltiplo de 4 (4×N)
      - BaseIdx presente como columnas o como índice (no muta)
    Semántica:
      - Si `bases` es un int > 0, además exige N == bases (estricto).
      - Errores de layout que se pueden degradar a warning aguas arriba usan: "UI_REBUILD_NOT_4X12".
    """
    if df_ui is None:
        raise RuntimeError("E.STATE.001: buffer inconsistente (None)")
    if not isinstance(df_ui, pd.DataFrame):
        raise RuntimeError(f"E.STATE.001: tipo inesperado: {type(df_ui)}")

    # 1) Meses obligatorios
    meses = [f"{i:02d}" for i in range(1, 13)]
    faltantes = [m for m in meses if m not in df_ui.columns]
    if faltantes:
        raise RuntimeError(
            f"UI_REBUILD_NOT_4X12: faltan columnas mensuales: {faltantes}"
        )

    # 2) Filas: múltiplo de 4 (4×N)
    nfilas = int(getattr(df_ui, "shape", (0, 0))[0])
    if nfilas % 4 != 0:
        raise RuntimeError(
            f"UI_REBUILD_NOT_4X12: filas no múltiplo de 4 (len={nfilas})"
        )

    # 3) Si bases>0, validar N exacto
    ngrupos = 0 if nfilas == 0 else (nfilas // 4)
    if isinstance(bases, int) and bases > 0 and ngrupos != bases:
        raise RuntimeError(
            f"UI_REBUILD_NOT_4X12: bases_esperadas={bases}, bases_obtenidas={ngrupos}"
        )

    # 4) Presencia de claves lógicas (en columnas o en índice)
    base_cols = {"ItemCode", "TipoForecast", "Métrica", "OcrCode3"}
    cols_ok = base_cols.issubset(df_ui.columns)

    if not cols_ok and isinstance(df_ui.index, pd.MultiIndex):
        idx_names = {n for n in (df_ui.index.names or []) if n is not None}
        if not base_cols.issubset(idx_names):
            raise RuntimeError(
                "E.VAL.001: BaseIdx ausente (ni en columnas ni en índice)"
            )
    elif not cols_ok and not isinstance(df_ui.index, pd.MultiIndex):
        raise RuntimeError("E.VAL.001: BaseIdx ausente (ni en columnas ni en índice)")

    # 5) NaN en columnas mensuales no permitido en verificación post-refresh
    if df_ui[meses].isna().any().any():
        raise RuntimeError("E.VAL.001: valores NaN en columnas mensuales")

    # OK (sin return)


# B_BUF002: Invalidación de caches y buffers por cliente/año
# # ∂B_BUF002/∂B0
def invalidar_cache(
    slpcode: int,
    cardcode: str,
    anio: int,
    *,
    deep: bool = False,
    verbose: bool = True,
) -> None:
    """
    Invalida caches/buffers asociados a (slpcode, cardcode, anio) para forzar
    una recarga limpia tras guardar.

    - Limpia buffers de sesión: buffer editable, espejo, hashes y filtro por producto.
    - Mantiene la selección de cliente (no borra 'cliente_selectbox').
    - Si deep=True, limpia st.cache_data / st.cache_resource (global).
    """

    try:
        import streamlit as st  # por si esta función vive en otro módulo
    except Exception:
        pass

    # Claves conocidas por cliente
    key_buffer = f"forecast_buffer_{cardcode}"
    key_mirror = f"{key_buffer}_editado"
    key_precio_map = f"{key_buffer}__precio_ref_map"
    key_hash_base = f"{key_buffer}_basehash"
    key_hash_edit = f"{key_buffer}_edithash"
    key_filtro = f"filtro_producto_{cardcode}"
    key_editor = f"editor_forecast_{cardcode}"  # data_editor key

    # (Opcionales) claves de resultados/consultas por cliente/año
    key_prev = f"forecast_prev_{slpcode}_{cardcode}_{anio}"
    key_editable = f"forecast_editable_{slpcode}_{cardcode}_{anio}"
    key_snapshot = f"snapshot_{slpcode}_{cardcode}_{anio}"

    known_keys = [
        key_buffer,
        key_mirror,
        key_precio_map,
        key_hash_base,
        key_hash_edit,
        key_filtro,
        key_editor,
        key_prev,
        key_editable,
        key_snapshot,
    ]

    removed = []
    for k in known_keys:
        if k in st.session_state:
            st.session_state.pop(k, None)
            removed.append(k)

    # No vaciamos 'clientes_editados' aquí: persistir_cambios_cliente ya descarta solo el actual.

    # Limpieza profunda opcional (afecta a TODA la app)
    if deep:
        try:
            st.cache_data.clear()
        except Exception:
            pass
        try:
            st.cache_resource.clear()
        except Exception:
            pass

    if verbose:
        print(
            f"[CACHE.INVALIDATE] slp={slpcode} card={cardcode} año={anio} "
            f"removed={len(removed)} deep={deep}"
        )
        if removed:
            print(f"[CACHE.INVALIDATE.DETAIL] {removed}")


# B_CTX001: Flags de configuración global del editor/forecast
# # ∂B_CTX001/∂B0
# Flags (puedes moverlos a tu config)
PERSIST_MODE = "ui_state"  # "ui_state" (default) / "delta" (solo diagnóstico)
VERIFY_4X12_STRICT = True
HIST_LAST_POLICY = "last_by_FechEntr"

MONTHS = [f"{i:02d}" for i in range(1, 13)]
TIPOS = ("Firme", "Proyectado")
METRICAS = ("Cantidad", "Precio")


# B_DBQ002: Lectura de detalle forecast por ForecastID desde BD
# # ∂B_DBQ002/∂B0
def _leer_detalle_forecast(forecast_id: int, db_path: str) -> pd.DataFrame:
    sql = """
      SELECT ForecastID, SlpCode, CardCode, ItemCode, TipoForecast, OcrCode3, Linea,
             FechEntr, strftime('%m', FechEntr) AS Mes, Cant, PrecioUN, DocCur
      FROM Forecast_Detalle
      WHERE ForecastID = ?
    """
    try:
        df = _read_sql(db_path, sql, (forecast_id,))
        df["Mes"] = df["Mes"].astype(str).str.zfill(2)
        return df
    except Exception as e:
        print(f"[PREP.WARN] leer_detalle_forecast failed id={forecast_id} err={e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _periodo_anual(anio: int) -> tuple[str, str]:
    """Devuelve (desde, hasta_exclusivo) para el año dado."""
    desde = f"{anio:04d}-01-01"
    hasta = f"{anio+1:04d}-01-01"
    return desde, hasta


# B_DBQ003: Obtención del último ForecastID por vendedor/cliente/año
# # ∂B_DBQ003/∂B0
def _get_forecast_id_prev(
    slp_code: int | None = None,
    card_code: str | None = None,
    anio: int | None = None,
    *,
    db_path: str,
    # alias por compatibilidad con call-sites que envían slpcode
    slpcode: int | None = None,
) -> int | None:
    """
    Busca el ForecastID más reciente (por Fecha_Carga, luego ForecastID) que tenga
    líneas para (slp_code, card_code) dentro del año 'anio'.

    Esquema (ver 9. Tipo_Datos):
      - Forecast(ForecastID, Fecha_Carga, SlpCode)
      - Forecast_Detalle(ForecastID, CardCode, FechEntr, ...)
    """
    # Normalizar alias
    if slp_code is None and slpcode is not None:
        slp_code = int(slpcode)
    if slp_code is None:
        raise ValueError("slp_code requerido")
    if not card_code:
        raise ValueError("card_code requerido")
    if anio is None:
        raise ValueError("anio requerido")

    fi, ff = _periodo_anual(anio)

    sql = """
    SELECT f.ForecastID
    FROM Forecast_Detalle AS fd
    INNER JOIN Forecast AS f ON f.ForecastID = fd.ForecastID
    WHERE f.SlpCode = ?
      AND fd.CardCode = ?
      AND fd.FechEntr >= ?
      AND fd.FechEntr < ?
    GROUP BY f.ForecastID, f.Fecha_Carga
    ORDER BY f.Fecha_Carga DESC, f.ForecastID DESC
    LIMIT 1;
    """
    df = _read_sql(db_path, sql, (slp_code, card_code, fi, ff))
    if df is not None and not df.empty:
        return int(df.iloc[0, 0])
    return None


# B_HDF002: Transformación del buffer UI 4x12 a formato largo
# # ∂B_HDF002/∂B0
def _melt_ui_4x12(df_ui: pd.DataFrame) -> pd.DataFrame:
    """
    Espera df_ui 4×12 con índice o columnas que incluyan:
    ItemCode, TipoForecast, Métrica ('Cantidad'|'Precio'), OcrCode3, Linea?, PrecioUN?, DocCur?
    y columnas '01'..'12' con valores.
    """
    base = df_ui.copy()
    if not isinstance(base.index, pd.MultiIndex):
        # si vienen como columnas, no pasa nada
        pass
    else:
        base = base.reset_index()

    # Normalizar columnas clave
    for col in ("ItemCode", "TipoForecast", "Métrica"):
        if col not in base.columns:
            raise ValueError("UI_SCHEMA_INVALID: faltan columnas clave en buffer UI")

    # Melt Cantidad
    qty = base[base["Métrica"] == "Cantidad"].copy()
    qty = qty.melt(
        id_vars=[c for c in qty.columns if c not in MONTHS],
        value_vars=MONTHS,
        var_name="Mes",
        value_name="Cant",
    )

    # Melt Precio (si existe precio mensual por fila)
    price_rows = base[base["Métrica"] == "Precio"].copy()
    if not price_rows.empty and price_rows[MONTHS].notna().any(axis=None):
        price_long = price_rows.melt(
            id_vars=[c for c in price_rows.columns if c not in MONTHS],
            value_vars=MONTHS,
            var_name="Mes",
            value_name="PrecioMes",
        )
    else:
        price_long = pd.DataFrame(columns=list(qty.columns) + ["PrecioMes"])

    # Armar precio/moneda por mes:
    # 1) Preferimos Precio mensual si existe
    out = qty.merge(
        price_long[["ItemCode", "TipoForecast", "OcrCode3", "Mes", "PrecioMes"]],
        how="left",
        on=["ItemCode", "TipoForecast", "OcrCode3", "Mes"],
    )
    # 2) Sino, usamos columna PrecioUN plana si existe
    if "PrecioUN" in base.columns and "PrecioMes" in out.columns:
        out["PrecioUN"] = out["PrecioMes"].fillna(out.get("PrecioUN", np.nan))
    elif "PrecioMes" in out.columns:
        out["PrecioUN"] = out["PrecioMes"]
    else:
        out["PrecioUN"] = np.nan

    # DocCur & Linea
    if "DocCur" in base.columns and "DocCur" not in out.columns:
        out["DocCur"] = (
            base["DocCur"].dropna().head(1).tolist()[0]
            if base["DocCur"].notna().any()
            else np.nan
        )
    if "Linea" not in out.columns:
        out["Linea"] = (
            base.get("Linea", "Trader") if "Linea" in base.columns else "Trader"
        )

    cols_keep = [
        "ItemCode",
        "TipoForecast",
        "OcrCode3",
        "Linea",
        "Mes",
        "Cant",
        "PrecioUN",
        "DocCur",
    ]
    out = out[cols_keep].copy()
    return out


# B_HDF003: Construcción de vista 4x12 desde datos en largo
# # ∂B_HDF003/∂B0
def make_4x12_from(
    df_db_largo: pd.DataFrame,
    df_catalogo: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Construye un 4×12 (2 TF × 2 Métricas) por cada combinación presente de
    (ItemCode, OcrCode3, DocCur), con comportamiento retrocompatible y robusto.

    Entradas toleradas (largo):
      - TipoForecast, Mes (1..12, "01".."12" o derivable desde FechEntr),
        Cant, PrecioUN, FechEntr, DocCur, y opcionalmente ItemCode, OcrCode3, ItemName.

    Salida:
      Columnas: ["ItemCode","ItemName","TipoForecast","Métrica","OcrCode3", 01..12, "DocCur","PrecioUN_ref","PrecioUN"].
      Meses como float64; 4 filas por cada combinación (Firme/Proyectado × Cantidad/Precio).

    Cambios clave vs. versión previa:
      - Evita FutureWarning de pandas al sustituir factorize(list(zip(...))) por
        groupby(...).ngroup() con sort=False y dropna=False cuando esté disponible.
      - Normalización de mes más tolerante (strings/ints/fechas), y DocCur en mayúsculas.
      - Cálculo de precio mensual: último no-nulo por mes (ordenado por FechEntr si existe).
      - PrecioUN_ref: primer valor mensual no-cero por fila de Precio (bfill left→right).
      - Orden y dtypes estables, sin dependencias de Streamlit.
    """
    print("[MAKE4x12.INFO] start")

    # ──────────────────────── Constantes tolerantes ────────────────────────
    try:
        _TIPOS = TIPOS
    except NameError:
        _TIPOS = ["Firme", "Proyectado"]

    try:
        _MONTHS = MONTHS
    except NameError:
        _MONTHS = [f"{m:02d}" for m in range(1, 13)]

    def _norm_month_series(s: pd.Series) -> pd.Series:
        """Convierte a '01'..'12' desde ints/strs; si viene fecha, toma el mes."""
        s2 = s.copy()
        try:
            # Si el dtype ya es datetime64
            if np.issubdtype(s2.dtype, np.datetime64):
                return s2.dt.month.astype(int).astype(str).str.zfill(2)
        except Exception:
            pass
        # Intento de parseo a fecha para strings
        try:
            s_dt = pd.to_datetime(s2, errors="coerce")
            if s_dt.notna().any():
                s2 = s_dt
                return s2.dt.month.astype(int).astype(str).str.zfill(2)
        except Exception:
            pass
        # Fallback: extraer números 1..12 y clamp
        s2 = s2.astype(str).str.extract(r"(\d{1,2})", expand=False)
        s2 = s2.fillna("0").astype(int).clip(1, 12).astype(str).str.zfill(2)
        return s2

    def _safe_name_map(df_cat: Optional[pd.DataFrame]) -> dict:
        if df_cat is None or not isinstance(df_cat, pd.DataFrame):
            return {}
        c = df_cat.copy()
        c.columns = c.columns.astype(str)
        if not {"ItemCode", "ItemName"}.issubset(c.columns):
            return {}
        c["ItemCode"] = c["ItemCode"].astype("string").str.strip()
        c["ItemName"] = c["ItemName"].astype("string").str.strip()
        return c.drop_duplicates("ItemCode").set_index("ItemCode")["ItemName"].to_dict()

    name_map = _safe_name_map(df_catalogo)

    # ──────────────────────── Caso vacío (devuelve 4×12 estándar) ────────────────────────
    if df_db_largo is None or getattr(df_db_largo, "empty", True):
        base = []
        for t in _TIPOS:
            base.append({"TipoForecast": t, "Métrica": "Cantidad"})
            base.append({"TipoForecast": t, "Métrica": "Precio"})
        df = pd.DataFrame(base)
        for m in _MONTHS:
            df[m] = 0.0
        df["PrecioUN_ref"] = 0.0
        df["PrecioUN"] = 0.0
        df["DocCur"] = "CLP"
        df["ItemCode"] = ""
        df["ItemName"] = ""
        df["OcrCode3"] = ""
        print("[MAKE4x12.INFO] empty input → returned template 4×12")
        return df[
            [
                "ItemCode",
                "ItemName",
                "TipoForecast",
                "Métrica",
                "OcrCode3",
                *_MONTHS,
                "DocCur",
                "PrecioUN_ref",
                "PrecioUN",
            ]
        ]

    # ──────────────────────── Normalización columnas de entrada ────────────────────────
    df = df_db_largo.copy()
    df.columns = df.columns.astype(str)

    # Aliases mínimos
    aliases = {
        "cantidad": "Cant",
        "cant": "Cant",
        "precio": "PrecioUN",
        "precioun": "PrecioUN",
        "fechentr": "FechEntr",
        "fecha": "FechEntr",
        "meses": "Mes",
    }
    for c in list(df.columns):
        low = c.lower()
        if low in aliases and aliases[low] not in df.columns:
            df = df.rename(columns={c: aliases[low]})

    # Mes
    if "Mes" not in df.columns:
        if "FechEntr" in df.columns:
            try:
                df["FechEntr"] = pd.to_datetime(df["FechEntr"], errors="coerce")
                df["Mes"] = (
                    df["FechEntr"].dt.month.astype("Int64").astype(str).str.zfill(2)
                )
            except Exception:
                df["Mes"] = "01"
        else:
            df["Mes"] = "01"
    else:
        df["Mes"] = _norm_month_series(df["Mes"])  # '01'..'12'

    # TipoForecast
    if "TipoForecast" not in df.columns:
        df["TipoForecast"] = "Firme"
    df["TipoForecast"] = df["TipoForecast"].astype(str).str.strip().str.title()
    df.loc[~df["TipoForecast"].isin(_TIPOS), "TipoForecast"] = "Firme"

    # Cantidad/Precio
    if "Cant" not in df.columns:
        df["Cant"] = 0.0
    df["Cant"] = (
        pd.to_numeric(df["Cant"], errors="coerce").fillna(0.0).astype("float64")
    )

    if "PrecioUN" in df.columns:
        df["PrecioUN"] = pd.to_numeric(df["PrecioUN"], errors="coerce").astype(float)
    else:
        df["PrecioUN"] = np.nan

    # Claves complementarias (+ normalización)
    for k, default, cast in [
        ("ItemCode", "", "string"),
        ("OcrCode3", "", "string"),
        ("DocCur", "CLP", "string"),
        ("ItemName", None, "string"),
    ]:
        if k not in df.columns:
            df[k] = default
        df[k] = df[k].astype(cast)
    df["DocCur"] = df["DocCur"].fillna("CLP").str.upper()

    # Si hay catálogo, prioriza su ItemName
    if name_map:
        df["ItemName"] = df.apply(
            lambda r: name_map.get(
                str(r["ItemCode"]).strip(),
                (r["ItemName"] or str(r["ItemCode"]).strip()),
            ),
            axis=1,
        ).astype("string")
    else:
        # Asegurar string limpio
        df["ItemName"] = (
            df["ItemName"].fillna(df["ItemCode"]).astype("string").str.strip()
        )

    # ──────────────────────── Agrupar por combinación (ItemCode, OcrCode3, DocCur) ────────────────────────
    group_keys = ["ItemCode", "OcrCode3", "DocCur"]
    if (not set(group_keys).issubset(df.columns)) or df.empty:
        df["_GKEY"] = 0
    else:
        # Robusto, sin FutureWarning; tolera NaN con dropna=False si existe en tu versión de pandas
        try:
            df["_GKEY"] = df.groupby(group_keys, sort=False, dropna=False).ngroup()
        except TypeError:
            df["_GKEY"] = df.groupby(group_keys, sort=False).ngroup()

    blocks = []

    # Orden estable por aparición de grupos (_GKEY ya respeta sort=False)
    for gkey, gdf in df.groupby("_GKEY", sort=False):
        gdf = gdf.copy()
        item_code = gdf["ItemCode"].iloc[0]
        ocr3 = gdf["OcrCode3"].iloc[0]
        doccur = gdf["DocCur"].iloc[0]
        item_name_series = gdf["ItemName"].dropna().astype(str)
        item_name = (
            item_name_series.iloc[0] if not item_name_series.empty else str(item_code)
        )

        # Pivot de cantidades: sum por mes/TF
        qty = (
            gdf.groupby(["TipoForecast", "Mes"], as_index=False)["Cant"]
            .sum()
            .pivot(index="TipoForecast", columns="Mes", values="Cant")
        )

        # Pivot de precios: último no nulo por FechEntr si existe, si no, último por orden de aparición
        tmp = gdf.copy()
        if "FechEntr" in tmp.columns:
            try:
                tmp["_Fech"] = pd.to_datetime(tmp["FechEntr"], errors="coerce")
            except Exception:
                tmp["_Fech"] = pd.NaT
        else:
            tmp["_Fech"] = pd.NaT
        tmp = tmp.sort_values(["TipoForecast", "Mes", "_Fech"], na_position="last")

        price = (
            tmp.dropna(subset=["PrecioUN"])
            .groupby(["TipoForecast", "Mes"], as_index=False)["PrecioUN"]
            .last()
            .pivot(index="TipoForecast", columns="Mes", values="PrecioUN")
            if tmp["PrecioUN"].notna().any()
            else pd.DataFrame(index=pd.Index([], name="TipoForecast"))
        )

        # Asegurar índice (TIPOS) y columnas (MONTHS)
        def _ensure_frame(frame: pd.DataFrame, fill: float) -> pd.DataFrame:
            if frame is None or frame.empty:
                frame = pd.DataFrame(
                    0.0, index=pd.Index(_TIPOS, name="TipoForecast"), columns=_MONTHS
                )
            else:
                frame = frame.reindex(_TIPOS).fillna(0.0)
                for m in _MONTHS:
                    if m not in frame.columns:
                        frame[m] = fill
                frame = frame[_MONTHS]
            # dtype consistente
            for m in _MONTHS:
                frame[m] = (
                    pd.to_numeric(frame[m], errors="coerce")
                    .fillna(0.0)
                    .astype("float64")
                )
            return frame

        qty = _ensure_frame(qty, 0.0)
        price = _ensure_frame(price, 0.0)

        # Construir 4 filas (2 TF × 2 métricas)
        filas = []
        for t in _TIPOS:
            row_q = {
                "ItemCode": item_code,
                "ItemName": item_name,
                "TipoForecast": t,
                "Métrica": "Cantidad",
                "OcrCode3": ocr3,
                "DocCur": doccur,
            }
            row_q.update({m: float(qty.loc[t, m]) for m in _MONTHS})
            filas.append(row_q)

            row_p = {
                "ItemCode": item_code,
                "ItemName": item_name,
                "TipoForecast": t,
                "Métrica": "Precio",
                "OcrCode3": ocr3,
                "DocCur": doccur,
            }
            row_p.update({m: float(price.loc[t, m]) for m in _MONTHS})
            filas.append(row_p)

        df4 = pd.DataFrame(filas)

        # PrecioUN_ref/PrecioUN: primer valor mensual no-cero (bfill desde izquierda)
        price_rows = df4["Métrica"].eq("Precio")
        if price_rows.any():
            tmpv = df4.loc[price_rows, _MONTHS].replace(0, np.nan)
            ref = tmpv.bfill(axis=1).iloc[:, 0].fillna(0.0).astype(float).to_numpy()
            df4.loc[price_rows, "PrecioUN_ref"] = ref
            df4.loc[price_rows, "PrecioUN"] = (
                ref  # compat: algunos flujos esperan 'PrecioUN'
            )
        df4.loc[~price_rows, "PrecioUN_ref"] = 0.0
        df4.loc[~price_rows, "PrecioUN"] = 0.0

        # Orden y dtypes
        ordered = [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            *_MONTHS,
            "DocCur",
            "PrecioUN_ref",
            "PrecioUN",
        ]
        df4 = df4[ordered].copy()
        df4["DocCur"] = df4["DocCur"].astype("string").str.upper()
        df4["OcrCode3"] = df4["OcrCode3"].astype("string")
        df4["ItemName"] = df4["ItemName"].astype("string")

        blocks.append(df4)

    out = (
        pd.concat(blocks, ignore_index=True)
        if blocks
        else pd.DataFrame(
            columns=[
                "ItemCode",
                "ItemName",
                "TipoForecast",
                "Métrica",
                "OcrCode3",
                *_MONTHS,
                "DocCur",
                "PrecioUN_ref",
                "PrecioUN",
            ]
        )
    )

    # Orden estable por claves lógicas (para reproducibilidad visual)
    if not out.empty:
        out = out.sort_values(
            ["ItemCode", "OcrCode3", "TipoForecast", "Métrica"],
            ascending=[True, True, True, True],
        ).reset_index(drop=True)

    print(f"[MAKE4x12.INFO] rows={len(out)} completed=True groups={len(blocks)}")
    return out


# B_DBQ004: Asegurar índice único correcto en Forecast_Detalle
# # ∂B_DBQ004/∂B0
def asegurar_indice_unico_forecast_detalle(
    db_path: Optional[str] = None, conn: Optional[Any] = None
):
    """
    Crea (si no existe) el índice ÚNICO correcto para Forecast_Detalle SIN usar 'Métrica'.
    Clave única: (ForecastID, CardCode, ItemCode, TipoForecast, OcrCode3, FechEntr)
    """
    # Usar la tx() central si no se provee una conexión abierta
    if conn is not None:
        cm = nullcontext(conn)
    else:
        if not db_path:
            raise ValueError("db_path requerido si no se entrega 'conn'")
        cm = tx(db_path, immediate=True)  # BEGIN IMMEDIATE dentro de tx()

    idx_name_ok = "ux_ForecastDet_FID_Card_Item_Tipo_Linea_Mes"
    try:
        with cm as c:
            cur = c.cursor()

            # Validar columnas requeridas (Métrica NO aplica a BD)
            cols = [
                r[1]
                for r in cur.execute("PRAGMA table_info('Forecast_Detalle')").fetchall()
            ]
            required = [
                "ForecastID",
                "CardCode",
                "ItemCode",
                "TipoForecast",
                "OcrCode3",
                "FechEntr",
            ]
            missing = [col for col in required if col not in cols]
            if missing:
                raise RuntimeError(
                    f"Faltan columnas requeridas en Forecast_Detalle: {missing}"
                )

            # Crear índice único correcto (idempotente)
            idxs = cur.execute("PRAGMA index_list('Forecast_Detalle');").fetchall()
            names = {r[1] for r in idxs}
            print(f"[IDX.CHECK] existentes={names}")
            if idx_name_ok in names:
                print(f"[IDX.SKIP] {idx_name_ok} ya existe")
                return

            sql = f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {idx_name_ok}
            ON Forecast_Detalle(ForecastID, CardCode, ItemCode, TipoForecast, OcrCode3, FechEntr);
            """
            print(f"[IDX.CREATE.SQL] {sql.strip()}")
            cur.execute(sql)
            print(f"[IDX.OK] creado {idx_name_ok}")
    except Exception as e:
        print(f"[IDX.ERROR] asegurar_indice: {e}")
        raise


# B_FCS005: Obtener forecast editable para edición directa por cliente
# # ∂B_FCS005/∂B0
# ∂B_FCS005/∂B0
# # ∂B_FCS005/∂B0
def obtener_forecast_editable(
    slp_code: int,
    card_code: str,
    anio: Optional[int] = None,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Devuelve forecast en 4x12 por ItemCode/TipoForecast/OcrCode3/DocCur:
      - Métrica = 'Cantidad'  → sumas mensuales
      - Métrica = 'Precio'    → precio mensual (último no-nulo del mes)
    NOTA: 'PrecioUN_ref' queda solo como referencia (no se usa en la UI).
    """
    print("[CFE-002] ▶ start obtener_forecast_editable")
    print(
        f"[CFE-002] args: slp_code={slp_code}({type(slp_code).__name__}), card_code={card_code}({type(card_code).__name__}), anio={anio}, db_path={db_path}"
    )

    # ─────────── Guards
    if not isinstance(slp_code, (int, np.integer)):
        raise TypeError("[CFE-002] slp_code debe ser int")
    if not isinstance(card_code, str):
        raise TypeError("[CFE-002] card_code debe ser str")
    if anio is not None and not isinstance(anio, (int, np.integer)):
        raise TypeError("[CFE-002] anio debe ser int o None")

    MONTHS = [f"{m:02d}" for m in range(1, 13)]
    idx_cols = ["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur"]

    # 1) último ForecastID del cliente/año (usa helper de rango)
    q_last_id_base = """
        SELECT MAX(ForecastID) AS ForecastID
          FROM Forecast_Detalle
         WHERE SlpCode = ? AND CardCode = ?
    """.strip()
    q_last_id, p_rango_id = add_year_filter(q_last_id_base, anio, col="FechEntr")
    params_last = (slp_code, card_code, *p_rango_id)

    df_last = run_query(q_last_id, db_path, params_last)
    last_id = None if df_last.empty else df_last["ForecastID"].iloc[0]
    print(f"[CFE-002] último ForecastID detectado → {last_id}")

    # DataFrame vacío (tipado) si no hay ID
    if last_id is None or (isinstance(last_id, float) and np.isnan(last_id)):
        cols = [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            *MONTHS,
            "DocCur",
        ]
        empty = pd.DataFrame({c: [] for c in cols})
        for c in [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            "DocCur",
        ]:
            empty[c] = empty[c].astype("string")
        print("[CFE-002] ✅ done obtener_forecast_editable (vacío)")
        return empty

    # 2) Traer detalle (sin agregación de precio) + helper de rango
    query_base = """
        WITH name_pick AS (
          SELECT
            o.ItemCode,
            COALESCE(
              (SELECT i2.ItemName
                 FROM OITM i2
                WHERE i2.ItemCode = o.ItemCode AND UPPER(COALESCE(i2.validFor,'Y'))='Y'
                ORDER BY i2.ItemName ASC
                LIMIT 1),
              (SELECT i3.ItemName
                 FROM OITM i3
                WHERE i3.ItemCode = o.ItemCode
                ORDER BY i3.ItemName ASC
                LIMIT 1)
            ) AS ItemName
          FROM OITM o
          GROUP BY o.ItemCode
        )
        SELECT
            fd.ItemCode,
            np.ItemName,
            fd.TipoForecast,
            COALESCE(fd.OcrCode3, '') AS OcrCode3,
            UPPER(fd.DocCur) AS DocCur,
            fd.FechEntr            AS Fecha,
            CAST(strftime('%m', fd.FechEntr) AS INTEGER) AS Mes,
            fd.Cant                AS Cantidad,
            fd.PrecioUN            AS PrecioUN
        FROM Forecast_Detalle fd
        LEFT JOIN name_pick np
               ON np.ItemCode = fd.ItemCode
        WHERE fd.ForecastID = ?
          AND fd.SlpCode    = ?
          AND fd.CardCode   = ?
    """.strip()
    query, p_rango_det = add_year_filter(query_base, anio, col="fd.FechEntr")

    params_main = (int(last_id), slp_code, card_code, *p_rango_det)
    print(f"[CFE-002] sql params(main): {params_main}")

    df = run_query(query, db_path, params_main)
    print(f"[CFE-002] raw result shape: {df.shape}")

    # Vacío con esquema estándar si no hay filas
    if df.empty:
        cols = [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            *MONTHS,
            "DocCur",
        ]
        empty = pd.DataFrame({c: [] for c in cols})
        for c in [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            "DocCur",
        ]:
            empty[c] = empty[c].astype("string")
        print("[CFE-002] ✅ done obtener_forecast_editable (vacío tras query)")
        return empty

    # ─────────── Normalizaciones
    df["Mes"] = pd.to_numeric(df["Mes"], errors="coerce").clip(1, 12).astype("Int64")
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0.0)
    df["PrecioUN"] = pd.to_numeric(df["PrecioUN"], errors="coerce")
    for c in ["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur"]:
        df[c] = df[c].astype("string")
    df["TipoForecast"] = df["TipoForecast"].str.strip().str.title()
    df["DocCur"] = df["DocCur"].str.strip().str.upper()
    df["OcrCode3"] = df["OcrCode3"].fillna("").astype("string")

    # ===== Cantidad por mes (sum)
    qty = (
        df.groupby(idx_cols + ["Mes"], as_index=False)["Cantidad"]
        .sum()
        .pivot(index=idx_cols, columns="Mes", values="Cantidad")
        .fillna(0.0)
        .reset_index()
    )
    # Renombrar columnas Mes → "01".."12"
    qty.columns = [
        f"{int(c):02d}" if isinstance(c, (int, np.integer)) else c for c in qty.columns
    ]
    for m in MONTHS:
        if m not in qty.columns:
            qty[m] = 0.0
    qty["Métrica"] = "Cantidad"

    # ===== Precio por mes (último no-nulo del mes, por fecha)
    df_prc = df.dropna(subset=["PrecioUN"]).sort_values(idx_cols + ["Mes", "Fecha"])
    if not df_prc.empty:
        prc = (
            df_prc.groupby(idx_cols + ["Mes"], as_index=False)["PrecioUN"]
            .last()
            .pivot(index=idx_cols, columns="Mes", values="PrecioUN")
            .fillna(0.0)
            .reset_index()
        )
        prc.columns = [
            f"{int(c):02d}" if isinstance(c, (int, np.integer)) else c
            for c in prc.columns
        ]
        for m in MONTHS:
            if m not in prc.columns:
                prc[m] = 0.0
        prc["Métrica"] = "Precio"
    else:
        # Fallback: misma granularidad que qty pero con 0.0
        base = qty[idx_cols].drop_duplicates().copy()
        for m in MONTHS:
            base[m] = 0.0
        prc = base
        prc["Métrica"] = "Precio"

    # Unificar y ordenar columnas
    keep = [
        "ItemCode",
        "ItemName",
        "TipoForecast",
        "Métrica",
        "OcrCode3",
        *MONTHS,
        "DocCur",
    ]
    qty = qty[
        [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "OcrCode3",
            *MONTHS,
            "DocCur",
            "Métrica",
        ]
    ].reindex(columns=keep)
    prc = prc[
        [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "OcrCode3",
            *MONTHS,
            "DocCur",
            "Métrica",
        ]
    ].reindex(columns=keep)
    out = pd.concat([qty, prc], ignore_index=True)

    # (Opcional) referencia de precio promedio anual
    out["PrecioUN_ref"] = 0.0
    mask_pr = out["Métrica"].eq("Precio")
    if mask_pr.any():
        out.loc[mask_pr, "PrecioUN_ref"] = (
            out.loc[mask_pr, MONTHS].mean(axis=1).astype("float64")
        )

    # Tipos finales para meses
    for m in MONTHS:
        out[m] = pd.to_numeric(out[m], errors="coerce").fillna(0.0).astype("float64")

    print(f"[CFE-002] editable shape: {out.shape}")
    print(
        f"[CFE-002] meses present → {', '.join([c for c in MONTHS if c in out.columns])}"
    )
    try:
        with pd.option_context("display.max_columns", None):
            print("[CFE-002] head preview:")
            print(out.head().to_string(index=False))
    except Exception as e:
        print(f"[CFE-002] head preview error: {e}")

    print("[CFE-002] ✅ done obtener_forecast_editable")
    return out[keep]  # la UI no usa PrecioUN_ref


# B_BUF003: Inicialización extendida del buffer forecast con estructura y Métrica
# # ∂B_BUF003/∂B0
def inicializar_buffer_cliente(
    cliente: str,
    df_base: pd.DataFrame,
    moneda_default: str = "CLP",
    precio_autofill_mode: str = "strict",  # "strict" | "spread"
    force_reinit: bool = False,
    oitm_df: "pd.DataFrame | None" = None,
) -> None:
    """
    Inicializa/rehidrata el buffer editable del cliente en sesión.
    - Sin st.stop()/st.rerun().
    - Índice canónico: ["ItemCode","TipoForecast","Métrica","OcrCode3"].
    - Columnas de meses aseguradas y en orden 01..12 (numéricas).
    - No toca *_edithash (la UI lo gestiona).
    """

    print("[RFE-004] ▶ start inicializar_buffer_cliente")
    print(
        f"[RFE-004] args: cliente={cliente}({type(cliente).__name__}), "
        f"df_base.shape={getattr(df_base,'shape',None)}, moneda_default={moneda_default}, "
        f"precio_autofill_mode={precio_autofill_mode}, force_reinit={force_reinit}, "
        f"oitm_df.shape={getattr(oitm_df,'shape',None)}"
    )

    # ── Guards
    if not isinstance(cliente, str):
        raise TypeError("[RFE-004] cliente debe ser str")
    if not isinstance(df_base, pd.DataFrame):
        raise TypeError("[RFE-004] df_base debe ser DataFrame")
    if precio_autofill_mode not in ("strict", "spread"):
        raise ValueError("[RFE-004] precio_autofill_mode debe ser 'strict' o 'spread'")

    # ── Normalizar id de cliente
    _cli = (
        cliente.replace("forecast_buffer_", "", 1)
        if cliente.startswith("forecast_buffer_")
        else cliente
    )
    if _cli != cliente:
        print(f"[BUFKEY.INFO] input='{cliente}' → normalized='{_cli}'")

    # ── Clave de sesión (tolerante si no existe get_key_buffer)
    try:
        key = get_key_buffer(_cli)  # type: ignore[name-defined]
    except Exception:
        key = f"forecast_buffer_{_cli}"

    print(f"[RFE-004] key de buffer -> {key}")
    if key in st.session_state and not force_reinit:
        print(
            f"[RFE-004] buffer ya existe en sesión: {key} -> skip (usa force_reinit=True para recalc)"
        )
        return

    # ── Copia / normalización base
    df = df_base.copy()
    df.columns = df.columns.astype(str)

    # Asegurar columnas base mínimas
    if "ItemCode" not in df.columns:
        raise ValueError("[RFE-004] Faltante columna 'ItemCode' en df_base")
    df["ItemCode"] = df["ItemCode"].astype("string").str.strip()

    # TipoForecast / Métrica
    if "TipoForecast" not in df.columns:
        df["TipoForecast"] = "Firme"
    df["TipoForecast"] = df["TipoForecast"].astype("string").str.strip().str.title()
    if "Métrica" not in df.columns:
        df["Métrica"] = "Cantidad"
    df["Métrica"] = df["Métrica"].astype("string").str.strip().str.title()
    df.loc[~df["TipoForecast"].isin(["Firme", "Proyectado"]), "TipoForecast"] = "Firme"
    df.loc[~df["Métrica"].isin(["Cantidad", "Precio"]), "Métrica"] = "Cantidad"

    # OcrCode3 / DocCur / ItemName
    if "OcrCode3" not in df.columns:
        df["OcrCode3"] = ""
    df["OcrCode3"] = df["OcrCode3"].astype("string")

    if "DocCur" not in df.columns:
        df["DocCur"] = moneda_default
    df["DocCur"] = df["DocCur"].astype("string")

    if "ItemName" not in df.columns:
        df["ItemName"] = df["ItemCode"]
    df["ItemName"] = df["ItemName"].astype("string").fillna(df["ItemCode"]).str.strip()

    # Meses 01..12 (numéricos, ordenados)
    months = [f"{m:02d}" for m in range(1, 13)]
    for m in months:
        if m not in df.columns:
            df[m] = 0.0
        df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0.0).astype("float64")

    # ── Resolver nombre canónico (preferente OITM)
    def _resolver_nombres_por_oitm(oitm_df: "pd.DataFrame | None") -> dict:
        if oitm_df is None or not isinstance(oitm_df, pd.DataFrame):
            return {}
        odf = oitm_df.copy()
        odf.columns = odf.columns.astype(str)
        if not {"ItemCode", "ItemName"}.issubset(odf.columns):
            return {}
        odf["ItemCode"] = odf["ItemCode"].astype("string").str.strip()
        odf["ItemName"] = odf["ItemName"].astype("string").str.strip()
        if "validFor" in odf.columns:
            odf["_valid"] = (
                odf["validFor"].astype("string").str.upper() == "Y"
            ).astype(int)
        else:
            odf["_valid"] = 1
        odf = odf.sort_values(
            ["ItemCode", "_valid", "ItemName"], ascending=[True, False, True]
        )
        name_map = (
            odf.drop_duplicates(subset=["ItemCode"], keep="first")
            .set_index("ItemCode")["ItemName"]
            .to_dict()
        )
        print(f"[RFE-004.NOMBRE] ItemName canónico via OITM; únicos={len(name_map)}")
        return name_map

    name_map = _resolver_nombres_por_oitm(oitm_df)
    if not name_map:
        tmp = df[["ItemCode"]].copy()
        tmp["ItemName"] = (
            df.get("ItemName", df["ItemCode"])
            .astype("string")
            .fillna(df["ItemCode"])
            .str.strip()
        )
        tmp = tmp.sort_values(["ItemCode", "ItemName"], ascending=[True, True])
        name_map = (
            tmp.drop_duplicates(subset=["ItemCode"], keep="first")
            .set_index("ItemCode")["ItemName"]
            .to_dict()
        )
        print(f"[RFE-004.NOMBRE] ItemName canónico via df_base; únicos={len(name_map)}")

    def _canon_name(itemcode: str) -> str:
        code = pd.Series([itemcode]).astype("string").iloc[0]
        return name_map.get(code, code)

    # ── Ref map de PrecioUN (soporta PrecioUN o PrecioUN_ref)
    base_keys = ["ItemCode", "OcrCode3", "DocCur"]
    origen_precio = "cero"
    ref_map: dict[tuple, float] = {}

    # Preferir PrecioUN, si no existe, usar PrecioUN_ref si está disponible
    precio_un_col = (
        "PrecioUN"
        if "PrecioUN" in df.columns
        else ("PrecioUN_ref" if "PrecioUN_ref" in df.columns else None)
    )
    if precio_un_col:
        ref = df[["ItemCode", "OcrCode3", precio_un_col]].copy()
        ref[precio_un_col] = pd.to_numeric(ref[precio_un_col], errors="coerce")
        ref.loc[ref[precio_un_col] < 0, precio_un_col] = np.nan
        ref.loc[ref[precio_un_col] > 1e9, precio_un_col] = np.nan
        ref_map = (
            ref.groupby(["ItemCode", "OcrCode3"], dropna=False)[precio_un_col]
            .median()
            .fillna(0.0)
            .to_dict()
        )
        origen_precio = f"{precio_un_col}_median"

    # ── Agregación CANTIDAD
    tipos_obj = ["Firme", "Proyectado"]
    tiene_cant = df["Métrica"].eq("Cantidad").any()
    tiene_precio = df["Métrica"].eq("Precio").any()

    rows_cant: list[dict] = []
    if not tiene_precio or tiene_cant:  # siempre generar capa Cantidad
        df_c = df[df["Métrica"].eq("Cantidad")] if tiene_cant else df.copy()
        g_c = (
            df_c.groupby(base_keys + ["TipoForecast"], dropna=False)[months]
            .sum()
            .reset_index()
        )
        for _, sub in g_c.groupby(base_keys, dropna=False):
            presentes = set(sub["TipoForecast"].unique().tolist())
            for t in tipos_obj:
                if t in presentes:
                    row = sub[sub["TipoForecast"] == t].iloc[0].to_dict()
                else:
                    row = {k: sub.iloc[0][k] for k in base_keys}
                    row["TipoForecast"] = t
                    for m in months:
                        row[m] = 0.0
                row["ItemName"] = _canon_name(row["ItemCode"])
                rows_cant.append(row)
    cant_df = (
        pd.DataFrame(rows_cant)
        if rows_cant
        else pd.DataFrame(columns=base_keys + ["TipoForecast"] + months + ["ItemName"])
    )

    # ── Agregación PRECIO (si viene en base → respetar con FFill por mes)
    rows_prec: list[dict] = []
    if tiene_precio:
        df_p = df[df["Métrica"] == "Precio"].copy()

        def _agg_prec(gr: pd.DataFrame) -> pd.Series:
            out = {}
            for m in months:
                s = pd.to_numeric(gr[m], errors="coerce")
                s = s.replace(0, np.nan).ffill()  # hereda últimos precios conocidos
                val = s.iloc[-1] if not s.dropna().empty else float(0.0)
                out[m] = float(val) if pd.notna(val) else 0.0
            return pd.Series(out, dtype="float64")

        g_p = (
            df_p.groupby(base_keys + ["TipoForecast"], dropna=False)
            .apply(_agg_prec)
            .reset_index()
        )
        for _, sub in g_p.groupby(base_keys, dropna=False):
            presentes = set(sub["TipoForecast"].unique().tolist())
            for t in tipos_obj:
                if t in presentes:
                    row = sub[sub["TipoForecast"] == t].iloc[0].to_dict()
                else:
                    row = {k: sub.iloc[0][k] for k in base_keys}
                    row["TipoForecast"] = t
                    for m in months:
                        row[m] = 0.0
                row["ItemName"] = _canon_name(row["ItemCode"])
                rows_prec.append(row)

        # Si no teníamos ref_map y de los precios base se puede derivar, úsalo
        if not ref_map and rows_prec:
            tmp = pd.DataFrame(rows_prec)
            tmp["_ref"] = (
                tmp[months].replace(0, np.nan).bfill(axis=1).iloc[:, 0].fillna(0.0)
            )
            ref_map = tmp.set_index(["ItemCode", "OcrCode3"])["_ref"].to_dict()
            origen_precio = "precio_mensual_derivado"

    # ── Si NO vino Precio → autocompletar desde ref_map con strict/spread
    if not tiene_precio:

        def _pu(row: pd.Series) -> float:
            return float(ref_map.get((row["ItemCode"], row.get("OcrCode3", "")), 0.0))

        if not cant_df.empty:
            prec_df = cant_df.copy()
            pu_vec = prec_df.apply(_pu, axis=1).astype("float64")
            if precio_autofill_mode == "strict":
                for m in months:
                    prec_df[m] = np.where(prec_df[m] > 0, pu_vec, 0.0).astype("float64")
            else:  # spread
                any_qty = (prec_df[months].sum(axis=1) > 0).astype(bool)
                for m in months:
                    prec_df[m] = np.where(any_qty, pu_vec, 0.0).astype("float64")
            rows_prec = prec_df.assign(
                ItemName=prec_df["ItemCode"].map(_canon_name)
            ).to_dict("records")
            if ref_map and origen_precio == "cero":
                origen_precio = "ref_map_existente"

    print(
        f"[RFE-004.PRECIOUN] ref_map size={len(ref_map)} origen={origen_precio} "
        f"min={min(ref_map.values(), default=0):.4f} max={max(ref_map.values(), default=0):.4f}"
    )

    # ── DataFrame final (Cantidad + Precio)
    cant_df = (
        pd.DataFrame(rows_cant)
        if rows_cant
        else pd.DataFrame(columns=base_keys + ["TipoForecast"] + months + ["ItemName"])
    )
    prec_df = (
        pd.DataFrame(rows_prec)
        if rows_prec
        else pd.DataFrame(columns=base_keys + ["TipoForecast"] + months + ["ItemName"])
    )
    if not cant_df.empty:
        cant_df["Métrica"] = "Cantidad"
    if not prec_df.empty:
        prec_df["Métrica"] = "Precio"

    df_combo = pd.concat([cant_df, prec_df], ignore_index=True)

    # columna Linea (opcional)
    if "Linea" in df.columns and "Linea" not in df_combo.columns:
        try:
            linea_map = (
                df.drop_duplicates(subset=base_keys)[base_keys + ["Linea"]]
                .set_index(base_keys)["Linea"]
                .to_dict()
            )
            df_combo["Linea"] = df_combo.apply(
                lambda r: linea_map.get(
                    (r["ItemCode"], r["OcrCode3"], r["DocCur"]), None
                ),
                axis=1,
            )
        except Exception as e:
            print(f"[RFE-004.LINEA.WARN] {e}")

    # Orden de columnas UI (ItemName antes de Métrica como en la vista)
    ordered = (
        ["ItemCode", "ItemName", "TipoForecast", "Métrica", "OcrCode3"]
        + months
        + ["DocCur"]
        + ([c for c in ["Linea"] if c in df_combo.columns])
    )
    ordered = [c for c in ordered if c in df_combo.columns]
    df_combo = df_combo[ordered].copy()

    # ── Guardar en sesión: índice UNIFICADO y espejo editable
    st.session_state[key] = df_combo.set_index(
        ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
    ).sort_index()
    st.session_state[f"{key}_editado"] = st.session_state[key].copy()
    st.session_state[f"{key}__precio_ref_map"] = ref_map

    # Trazas
    print(
        f"[RFE-004] saved buffer_editado -> {key}_editado shape={st.session_state[f'{key}_editado'].shape}"
    )
    print(f"[RFE-004] saved buffer -> {key}")
    print(f"[RFE-004] buffer shape (rows x cols): {st.session_state[key].shape}")
    print(f"[RFE-004] columnas: {list(st.session_state[key].columns)}")
    try:
        with pd.option_context("display.max_columns", None):
            print("[RFE-004] head (indexed):")
            print(st.session_state[key].head().to_string())
    except Exception as e:
        print(f"[RFE-004] head preview error: {e}")

    print("[RFE-004] ✅ done inicializar_buffer_cliente")


# B_HDF001: Hash semántico para DataFrame con control de cambios estructurales
# # ∂B_HDF001/∂B0
def hash_semantico(df):
    return hash(pd.util.hash_pandas_object(df.sort_index(axis=1), index=True).sum())


# B_SYN002: Sincronización local del buffer con cambios del editor
# # ∂B_SYN002/∂B0
def sincronizar_buffer_local(
    df_buffer: pd.DataFrame, df_editado: pd.DataFrame
) -> Tuple[pd.DataFrame, bool]:
    """Fusiona cambios del editor con el buffer y devuelve (df_final, hay_cambios).

    Reglas:
      - Actualiza SOLO columnas de meses (01..12).
      - No elimina filas (une índices).
      - No toca 'PrecioUN' en filas existentes del buffer.
      - Preserva extras del buffer; para filas NUEVAS, toma extras del editado (excepto 'PrecioUN').
    """
    t0 = time.perf_counter()
    print("[SYNC.LOCAL.INFO] start")

    columnas_clave = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
    MESES_ALL = [f"{i:02d}" for i in range(1, 13)]
    EXTRAS_PROHIBIDAS = {"PrecioUN"}  # nunca se sobreescribe desde el editado

    def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
        idx_names = (
            list(df.index.names)
            if isinstance(df.index, pd.MultiIndex)
            else ([df.index.name] if df.index.name else [])
        )
        if any(k in idx_names for k in columnas_clave) and not set(
            columnas_clave
        ).issubset(df.columns):
            print("[SYNC.LOCAL.FIX] reset_index()")
            df = df.reset_index()
        return df

    def _rename_months(df: pd.DataFrame) -> pd.DataFrame:
        # Acepta columnas '1'..'12' o enteros y las normaliza a '01'..'12'
        ren = {}
        for c in df.columns:
            if isinstance(c, int) and 1 <= c <= 12:
                ren[c] = f"{c:02d}"
            elif isinstance(c, str) and c.isdigit():
                try:
                    n = int(c)
                    if 1 <= n <= 12:
                        ren[c] = f"{n:02d}"
                except Exception:
                    pass
        return df.rename(columns=ren) if ren else df

    # Normaliza entradas
    df_buffer = _ensure_cols(df_buffer.copy())
    df_editado = _ensure_cols(df_editado.copy())
    df_buffer = _rename_months(df_buffer)
    df_editado = _rename_months(df_editado)

    # Meses presentes en cualquiera de los dos (orden canónico 01..12)
    columnas_mes = [
        m for m in MESES_ALL if (m in df_buffer.columns) or (m in df_editado.columns)
    ]
    columnas_req = columnas_clave + columnas_mes

    # Validación mínima
    if not set(columnas_clave).issubset(df_editado.columns):
        missing = set(columnas_clave) - set(df_editado.columns)
        print(f"[SYNC.LOCAL.ERROR] missing_columns editado={missing}")
        raise ValueError(
            f"El DataFrame editado carece de columnas requeridas: {missing}"
        )

    print(f"[SYNC.LOCAL.INFO] meses_detectados={len(columnas_mes)}")

    # Garantiza columnas de meses en ambos DF (rellena con 0.0 si faltan)
    for m in columnas_mes:
        if m not in df_buffer.columns:
            df_buffer[m] = 0.0
        if m not in df_editado.columns:
            df_editado[m] = df_buffer[m]

        df_buffer[m] = pd.to_numeric(df_buffer[m], errors="coerce").fillna(0.0)
        df_editado[m] = pd.to_numeric(df_editado[m], errors="coerce").fillna(0.0)

    # Índices normalizados
    buf_idx = df_buffer.set_index(columnas_clave).sort_index()
    edi_idx = df_editado.set_index(columnas_clave).sort_index()

    idx_buf_original = buf_idx.index
    idx_union = idx_buf_original.union(edi_idx.index)

    buf_idx = buf_idx.reindex(idx_union)
    edi_idx = edi_idx.reindex(idx_union)

    print(
        f"[SYNC.LOCAL.INFO] shapes buffer={df_buffer.shape} "
        f"editado={df_editado.shape} union_idx={len(idx_union)}"
    )

    # Comparación tolerante a float solo en meses
    hay_cambios = False
    if columnas_mes:
        b = buf_idx[columnas_mes].astype("float64")
        e = edi_idx[columnas_mes].astype("float64")

        diff_array = ~np.isclose(b, e, atol=1e-6, equal_nan=True)
        dif_mask = pd.DataFrame(diff_array, index=idx_union, columns=columnas_mes)

        total_diff = int(dif_mask.to_numpy().sum())
        filas_diff = int(dif_mask.any(axis=1).sum())

        # Filas nuevas con datos (sum != 0.0) en meses
        idx_nuevas = edi_idx.index.difference(idx_buf_original)
        idx_nuevas_con_datos = [
            ix for ix in idx_nuevas if float(e.loc[ix, columnas_mes].sum()) != 0.0
        ]
        total_nuevas = len(idx_nuevas_con_datos)

        hay_cambios = (total_diff > 0) or (total_nuevas > 0)

        if hay_cambios:
            if total_diff > 0:
                print(f"[SYNC.CHANGE] cells={total_diff} rows={filas_diff}")
            if total_nuevas > 0:
                print(f"[SYNC.LOCAL.NEW] rows={total_nuevas}")
            # Aplica meses del editado al buffer (cubre cambios + nuevas)
            buf_idx.loc[:, columnas_mes] = e.values
        else:
            print("[SYNC.LOCAL.INFO] sin_diferencias")
    else:
        print("[SYNC.LOCAL.WARN] sin_columnas_mes -> no_changes")

    # Reconstrucción de extras:
    # - Para filas existentes: se preservan los extras del buffer (no se sobreescriben).
    # - Para filas nuevas: se completan desde el editado (excepto 'PrecioUN').
    extras_union_all = [
        c
        for c in set(df_buffer.columns).union(df_editado.columns)
        if (c not in columnas_clave) and (c not in columnas_mes)
    ]
    # Mantén orden estable de extras
    extras_union_all = [c for c in df_buffer.columns if c in extras_union_all] + [
        c
        for c in df_editado.columns
        if (c in extras_union_all and c not in df_buffer.columns)
    ]

    print(f"[SYNC.LOCAL.INFO] cols_extra={len(extras_union_all)}")

    if extras_union_all:
        # Semilla: extras desde el buffer
        extras_buf = df_buffer.set_index(columnas_clave)[extras_union_all].reindex(
            idx_union
        )
        buf_idx[extras_union_all] = extras_buf

        # Para filas nuevas, tomar extras del editado (excepto prohibidas)
        if len(idx_nuevas) > 0:
            extras_permitidas = [
                c for c in extras_union_all if c not in EXTRAS_PROHIBIDAS
            ]
            if extras_permitidas:
                extras_edi = df_editado.set_index(columnas_clave)[
                    extras_permitidas
                ].reindex(idx_union)
                # Solo asigna en las filas realmente nuevas
                buf_idx.loc[idx_nuevas, extras_permitidas] = extras_edi.loc[
                    idx_nuevas, extras_permitidas
                ]

    # Ensamblar salida con orden: claves + meses (detectados) + extras
    out_cols = columnas_req + [c for c in extras_union_all if c not in columnas_req]
    df_final = buf_idx.reset_index().reindex(columns=out_cols)

    # Mantener dtypes de columnas existentes según buffer
    dtype_map = {c: t for c, t in df_buffer.dtypes.items() if c in df_final.columns}
    df_final = df_final.astype(dtype_map, errors="ignore")

    print(
        f"[SYNC.LOCAL.INFO] end shape={df_final.shape} changes={hay_cambios} "
        f"elapsed={time.perf_counter()-t0:.3f}s"
    )
    return df_final, hay_cambios


BASE_TEMP = Path(__file__).resolve().parent.parent / "temp_ediciones"
BASE_TEMP.mkdir(parents=True, exist_ok=True)


# B_TMP002: Construcción de ruta de backup temporal para cliente
# # ∂B_TMP002/∂B0
def _ruta_temp(cliente: str) -> Path:
    return BASE_TEMP / f"{cliente}_forecast.parquet"


# B_TMP003: Guardado seguro de backup temporal (.parquet) del DataFrame de cliente
# # ∂B_TMP003/∂B0
def guardar_temp_local(cliente: str, df: pd.DataFrame):
    """
    Backup temporal (parquet) por cliente:
    - Hash estructural estable para evitar escrituras redundantes.
    - Lectura segura (lista blanca) confinada al directorio destino.
    - Escritura atómica (tmp + replace).
    Logs: [TMP.INFO]/[TMP.WARN]/[TMP.ERROR] en una sola línea, sin emojis.
    """
    t0 = time.perf_counter()
    ruta = _ruta_temp(cliente)  # p.ej. ".../temp_ediciones/<cliente>_forecast.parquet"
    ruta.parent.mkdir(parents=True, exist_ok=True)

    try:
        rows, cols = getattr(df, "shape", (0, 0))
        print(f"[TMP.INFO] start cliente={cliente} path={ruta} shape=({rows},{cols})")

        df_norm = normalize_df_for_hash(df)
        nuevo_hash = int(hash_pandas_object(df_norm, index=True).sum())

        hash_prev = None
        if ruta.exists():
            try:
                df_prev = load_df_parquet_safe(ruta, allowed_dir=BASE_TEMP)
                df_prev_norm = normalize_df_for_hash(df_prev)
                hash_prev = int(hash_pandas_object(df_prev_norm, index=True).sum())
            except Exception as _e:
                print(
                    f"[TMP.WARN] backup_unreadable — will_overwrite path={ruta} err={_e.__class__.__name__}: {str(_e)}"
                )

        if hash_prev is not None:
            iguales = nuevo_hash == hash_prev
            print(
                f"[TMP.INFO] hash_check new={nuevo_hash} prev={hash_prev} equal={iguales}"
            )
            if iguales:
                print(
                    f"[TMP.INFO] no_change — skip_write cliente={cliente} path={ruta} elapsed={time.perf_counter()-t0:.3f}s"
                )
                return

        save_df_parquet_atomic(df, ruta)
        size = ruta.stat().st_size if ruta.exists() else None
        print(
            f"[TMP.INFO] saved — cliente={cliente} path={ruta} bytes={size} elapsed={time.perf_counter()-t0:.3f}s"
        )

    except Exception as e:
        print(
            f"[TMP.ERROR] save_failed — cliente={cliente} path={ruta} err={e.__class__.__name__}: {e} elapsed={time.perf_counter()-t0:.3f}s"
        )


# B_VFD003: Validación integral de DataFrame forecast 4x12
# # ∂B_VFD003/∂B0
def validar_forecast_dataframe(df: pd.DataFrame) -> list[str]:

    def _strip_accents(s: str) -> str:
        s = _ud.normalize("NFKD", s)
        return "".join(ch for ch in s if not _ud.combining(ch))

    def _norm_col(s: str) -> str:
        """normaliza para matching flexible (minúsculas, sin tildes ni guiones/barras bajas)."""
        s2 = _strip_accents(str(s)).lower().replace("_", "").replace("-", "").strip()
        return s2

    print("🔍 [VALIDATION-START] Iniciando validación de DataFrame")

    errores: list[str] = []
    columnas_mes = [f"{m:02d}" for m in range(1, 13)]

    # --- Copia segura
    df = df.copy()
    df.columns = df.columns.astype(str)

    # --- 0) Si viene con MultiIndex (o índice con alguna clave lógica) → reset_index()
    baseidx_set = {"ItemCode", "TipoForecast", "Métrica", "OcrCode3"}
    try:
        if isinstance(df.index, pd.MultiIndex):
            print("ℹ️ [VALIDATION-INFO] MultiIndex detectado en índice → reset_index()")
            df = df.reset_index()
        elif df.index.name in baseidx_set:
            print(
                f"ℹ️ [VALIDATION-INFO] Índice nombrado '{df.index.name}' relevante → reset_index()"
            )
            df = df.reset_index()
    except Exception as _:
        # no crítico, seguimos
        pass

    # --- 1) Renombrado canónico de columnas (tolerante a variaciones)
    alias_map = {
        "itemcode": "ItemCode",
        "tipoforecast": "TipoForecast",
        "metrica": "Métrica",
        "métrica": "Métrica",
        "ocrcode3": "OcrCode3",
        "doccur": "DocCur",
        "itemname": "ItemName",
        "linea": "Linea",
        "precioun": "PrecioUN",
        "preciounref": "PrecioUN_ref",
        "precioun_ref": "PrecioUN_ref",
    }
    new_cols = {}
    for c in df.columns:
        key = _norm_col(c)
        if key in alias_map:
            new_cols[c] = alias_map[key]
    if new_cols:
        df = df.rename(columns=new_cols)

    # --- 2) Meses: aceptar 1..12 o "1".."12" → "01".."12"
    month_renames = {}
    for c in df.columns:
        if c.isdigit():
            n = int(c)
            if 1 <= n <= 12:
                month_renames[c] = f"{n:02d}"
    if month_renames:
        df = df.rename(columns=month_renames)

    # --- 2.1) Asegurar columna DocCur por política (default "CLP" si no existe)
    if "DocCur" not in df.columns:
        df["DocCur"] = "CLP"
        print(
            "ℹ️ [VALIDATION-INFO] DocCur no estaba presente → creado con valor por defecto 'CLP'"
        )

    # --- 3) Requeridos
    campos_requeridos = ["ItemCode", "TipoForecast", "Métrica", "DocCur"]
    for col in campos_requeridos:
        if col not in df.columns:
            errores.append(f"Falta la columna requerida: {col}")
            print(f"❌ [VALIDATION-ERROR] Falta columna requerida: {col}")
        else:
            print(f"✅ [VALIDATION-OK] Columna requerida presente: {col}")

    # Normalización de valores clave (si existen)
    if "ItemCode" in df.columns:
        # FIX: uso de .str.strip() en Series
        df["ItemCode"] = df["ItemCode"].astype(str).str.strip()
        print(
            f"✅ [VALIDATION-INFO] ItemCode normalizado - únicos: {df['ItemCode'].nunique()}"
        )

    if "TipoForecast" in df.columns:
        df["TipoForecast"] = df["TipoForecast"].astype(str).str.strip().str.title()
        print(
            f"✅ [VALIDATION-INFO] TipoForecast normalizado - valores: {df['TipoForecast'].unique().tolist()}"
        )

    if "Métrica" in df.columns:
        df["Métrica"] = df["Métrica"].astype(str).str.strip().str.title()
        print(
            f"✅ [VALIDATION-INFO] Métrica normalizada - valores: {df['Métrica'].unique().tolist()}"
        )

    if "DocCur" in df.columns:
        df["DocCur"] = (
            df["DocCur"].astype("string").str.strip().fillna("CLP").str.upper()
        )
        print(
            f"✅ [VALIDATION-INFO] DocCur normalizado - valores: {pd.Series(df['DocCur']).unique().tolist()}"
        )

    # --- 4) Validaciones de dominio
    if "Métrica" in df.columns:
        if not df["Métrica"].isin(["Cantidad", "Precio"]).all():
            errores.append(
                "La columna 'Métrica' contiene valores inválidos (solo 'Cantidad' o 'Precio')."
            )
            print(
                f"❌ [VALIDATION-ERROR] Valores inválidos en Métrica: {df['Métrica'].unique().tolist()}"
            )
        else:
            print("✅ [VALIDATION-OK] Métricas válidas")

    if "DocCur" in df.columns:
        if not df["DocCur"].str.match(r"^[A-Z]{3}$", na=False).all():
            errores.append(
                "La columna 'DocCur' debe contener códigos de moneda de 3 letras (ej. CLP, USD, EUR)."
            )
            print(
                f"❌ [VALIDATION-ERROR] DocCur inválidos: {df['DocCur'].unique().tolist()}"
            )
        else:
            print("✅ [VALIDATION-OK] DocCur válidos")

    # --- 5) Meses requeridos
    meses_faltantes = [col for col in columnas_mes if col not in df.columns]
    if meses_faltantes:
        errores.append(f"Faltan columnas de mes: {meses_faltantes}")
        print(f"❌ [VALIDATION-ERROR] Meses faltantes: {meses_faltantes}")
    else:
        print("✅ [VALIDATION-OK] Todas las columnas mensuales presentes")

    # --- 6) Duplicados por clave canónica (no incluye ItemName)
    clave_duplicado = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
    if set(clave_duplicado).issubset(df.columns):
        duplicados_mask = df.duplicated(subset=clave_duplicado, keep=False)
        if duplicados_mask.any():
            count_duplicados = int(duplicados_mask.sum())
            ejemplos = (
                df.loc[duplicados_mask, clave_duplicado]
                .head(5)
                .to_dict(orient="records")
            )
            errores.append(
                "Existen filas duplicadas por [ItemCode, TipoForecast, Métrica, OcrCode3]."
            )
            print(
                f"❌ [VALIDATION-ERROR] {count_duplicados} filas duplicadas encontradas. Ejemplos: {ejemplos}"
            )
        else:
            print("✅ [VALIDATION-OK] Sin duplicados en clave compuesta")

    # --- 7) Columnas inesperadas (tolerar ItemName/Linea/PrecioUN/PrecioUN_ref)
    permitidas_base = {
        "itemcode",
        "tipoforecast",
        "métrica",
        "metrica",
        "ocrcode3",
        "doccur",
        "itemname",
        "linea",
        "precioun",
        "precioun_ref",
        "preciounref",
    }
    col_extranas = [
        col
        for col in df.columns
        if (_norm_col(col) not in permitidas_base) and (col not in columnas_mes)
    ]
    if col_extranas:
        errores.append(f"Columnas inesperadas detectadas: {col_extranas}")
        print(f"❌ [VALIDATION-ERROR] Columnas inesperadas: {col_extranas}")
    else:
        print(
            "✅ [VALIDATION-OK] Sin columnas inesperadas (ItemName/Linea/PrecioUN(_ref) toleradas)"
        )

    # --- Si ya hay errores estructurales, devolvemos aquí
    if errores:
        print(f"❌ [VALIDATION-END] Validación fallida con {len(errores)} errores")
        return errores

    # --- 8) Forzar numéricos y chequear negativos
    df[columnas_mes] = (
        df[columnas_mes].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    )
    print("✅ [VALIDATION-INFO] Columnas mensuales convertidas a numéricas")

    for col in columnas_mes:
        negativos = df[df[col] < 0]
        if not negativos.empty:
            codigos = negativos["ItemCode"].astype(str).unique().tolist()[:5]
            errores.append(f"Valores negativos en mes {col} para: {codigos}")
            print(
                f"❌ [VALIDATION-ERROR] Valores negativos en {col}: {len(negativos)} registros (ej: {codigos})"
            )

    # --- 9) Dominio de TipoForecast (final)
    if (
        "TipoForecast" in df.columns
        and not df["TipoForecast"].isin(["Firme", "Proyectado"]).all()
    ):
        valores_invalidos = (
            df.loc[~df["TipoForecast"].isin(["Firme", "Proyectado"]), "TipoForecast"]
            .unique()
            .tolist()
        )
        errores.append(
            "TipoForecast contiene valores inválidos (solo 'Firme' o 'Proyectado')."
        )
        print(f"❌ [VALIDATION-ERROR] TipoForecast inválidos: {valores_invalidos}")
    else:
        print("✅ [VALIDATION-OK] TipoForecast válidos")

    if errores:
        print(f"❌ [VALIDATION-END] Validación fallida con {len(errores)} errores")
    else:
        print("✅ [VALIDATION-END] Validación exitosa - Sin errores encontrados")

    return errores


# B_INS003: Registro de cabecera Forecast (insert)
# # ∂B_INS003/∂B0
def registrar_forecast_cabecera(
    slpcode: int,
    db_path: str | None = None,
) -> int:
    ahora = datetime.now().isoformat(sep=" ", timespec="seconds")
    sql = "INSERT INTO Forecast (SlpCode, Fecha_Carga) VALUES (?, ?)"
    return _run_forecast_insert_get_id(sql, (slpcode, ahora))


# B_INS004: Obtiene o crea ForecastID reutilizable (cabecera)
# # ∂B_INS004/∂B0
def obtener_forecast_activo(
    slp_code: int | None = None,
    card_code: str | None = None,  # se conserva para compatibilidad con el call-site
    anio: int | None = None,
    *,
    db_path: str,
    force_new: bool = True,
    # alias por compatibilidad con call-sites que envían slpcode
    slpcode: int | None = None,
) -> int:
    """
    Devuelve un ForecastID "activo" para registrar cambios.

    - Con force_new=True: crea SIEMPRE un nuevo encabezado en Forecast (Fecha_Carga=CURRENT_TIMESTAMP).
    - Con force_new=False: intenta reutilizar el último encabezado del vendedor; si no existe, crea uno.

    Esquema (9. Tipo_Datos): Forecast(ForecastID, Fecha_Carga, SlpCode)
    No existen campos de cierre; la vigencia se define por el orden temporal de Fecha_Carga.
    """
    # Normalizar alias
    if slp_code is None and slpcode is not None:
        slp_code = int(slpcode)
    if slp_code is None:
        raise ValueError("slp_code requerido")

    if not force_new:
        reuse_sql = """
        SELECT ForecastID
        FROM Forecast
        WHERE SlpCode = ?
        ORDER BY Fecha_Carga DESC, ForecastID DESC
        LIMIT 1;
        """
        df = _read_sql(db_path, reuse_sql, (slp_code,))
        if df is not None and not df.empty:
            return int(df.iloc[0, 0])

    # Crear un nuevo encabezado limpio
    with tx(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO Forecast (Fecha_Carga, SlpCode) VALUES (CURRENT_TIMESTAMP, ?)",
            (slp_code,),
        )
        return int(cur.lastrowid)


# B_VFD004: Validación de esquema delta para logging de cambios
# # ∂B_VFD004/∂B0
def validate_delta_schema(
    df: pd.DataFrame, *, contexto: str = "[VALIDACIÓN DELTA]"
) -> None:
    """
    Valida que el DataFrame cumpla con el contrato `delta_schema_v3`, que estructura los cambios
    entre forecast actual y anterior. Este chequeo es obligatorio antes de insertar en logs o BD.

    ▸ Levanta ValueError si el esquema es inválido.
    ▸ El DataFrame debe contener exactamente las columnas esperadas.
    """
    expected_cols = {
        "ItemCode",
        "TipoForecast",
        "OcrCode3",
        "Mes",
        "CantidadAnterior",
        "CantidadNueva",
    }

    df_cols = set(df.columns)
    extra_cols = df_cols - expected_cols
    missing_cols = expected_cols - df_cols

    if missing_cols:
        raise ValueError(
            f"{contexto} ❌ Faltan columnas requeridas: {sorted(missing_cols)}"
        )

    if extra_cols:
        print(f"{contexto} ⚠️ Columnas adicionales no utilizadas: {sorted(extra_cols)}")

    if df.empty:
        print(f"{contexto} ⚠️ DataFrame vacío. Nada que validar.")
        return

    # Validaciones de tipo mínimo (pueden extenderse según reglas de negocio)
    for col in ["CantidadAnterior", "CantidadNueva"]:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise TypeError(f"{contexto} ❌ Columna '{col}' debe ser numérica")

    if df["Mes"].isnull().any():
        raise ValueError(f"{contexto} ❌ Hay valores nulos en la columna 'Mes'")

    if df["Mes"].str.len().max() != 2:
        raise ValueError(
            f"{contexto} ❌ Formato incorrecto de Mes: se espera string de 2 caracteres"
        )

    print(
        f"{contexto} ✅ Validación de esquema completada correctamente. Registros: {len(df)}"
    )


# B_FEN003: Registro de cambios reales en Forecast_LogDetalle desde historial
# # ∂B_FEN003/∂B0
def registrar_log_detalle_cambios(
    slpcode: int,
    cardcode: str,
    anio: int,
    df_largo: pd.DataFrame,
    db_path: str,
    *,
    forecast_id: int,
    forecast_id_anterior: int | None = None,
) -> pd.DataFrame:
    """Registra delta en Forecast_LogDetalle.
    Si `forecast_id_anterior` es None, asume que no existía versión previa."""

    if df_largo.empty:
        print(f"[DEBUG-B2] ⚠️ No hay filas para loggear (cliente {cardcode})")
        return pd.DataFrame()

    df_work = df_largo.copy()
    df_work["Timestamp"] = datetime.now().isoformat(timespec="seconds")
    df_work["SlpCode"] = slpcode
    df_work["CardCode"] = cardcode

    claves = ["ItemCode", "TipoForecast", "OcrCode3", "Mes"]

    if forecast_id_anterior is None:
        df_work["CantidadAnterior"] = 0
        print(
            f"[DEBUG-B2] No hay ForecastID anterior para {cardcode}, se asume CantidadAnterior = 0"
        )
    else:
        qry = """
            SELECT ItemCode, TipoForecast, OcrCode3,
                   CAST(strftime('%m', FechEntr) AS TEXT) AS MesTxt,
                   Cant AS CantidadAnterior
            FROM Forecast_Detalle
            WHERE ForecastID = ?
        """
        df_prev = run_query(qry, db_path, (forecast_id_anterior,))
        df_prev["Mes"] = df_prev["MesTxt"].astype(str).str.zfill(2)

        print(f"[DEBUG-B2] Histórico previo cargado: {len(df_prev)} registros")

        # 🧪 Detectar claves duplicadas antes del merge
        if df_prev.duplicated(subset=claves).any():
            print("[⚠️ DEBUG-B2] ¡Advertencia! Histórico con claves duplicadas:")
            print(
                df_prev[df_prev.duplicated(subset=claves, keep=False)][
                    claves + ["CantidadAnterior"]
                ]
            )

        df_work = df_work.merge(
            df_prev[claves + ["CantidadAnterior"]], on=claves, how="left"
        )
        df_work["CantidadAnterior"] = df_work["CantidadAnterior"].fillna(0)

    df_work["CantidadNueva"] = df_work["Cant"]
    df_log = df_work[df_work["CantidadAnterior"] != df_work["CantidadNueva"]].copy()
    print(f"[DEBUG-B2] Cambios detectados para log (cliente {cardcode}): {len(df_log)}")
    if df_log.empty:
        print("[DEBUG-B2] Sin cambios — se omite inserción en Forecast_LogDetalle")
        return df_log

    # 🧪 Validación estructural del log antes de continuar
    validate_delta_schema(df_log)

    # ▶️ Diagnóstico de impacto agregado
    df_log["Delta"] = df_log["CantidadNueva"] - df_log["CantidadAnterior"]
    delta_total = df_log["Delta"].sum()
    print(f"[DEBUG-B2] Delta total de unidades modificadas: {delta_total:,.2f}")

    nuevos = (df_log["CantidadAnterior"] == 0).sum()
    if nuevos > 0:
        print(f"[DEBUG-B2] Registros nuevos sin histórico previo: {nuevos}")

    resumen_tipo = df_log.groupby("TipoForecast")["Delta"].sum().reset_index()
    print("[DEBUG-B2] Delta por TipoForecast:")
    print(resumen_tipo.to_string(index=False))

    print("[DEBUG-B2] Cambios logueados (preview):")
    print(
        df_log[["ItemCode", "TipoForecast", "Mes", "CantidadAnterior", "CantidadNueva"]]
        .head(5)
        .to_string(index=False)
    )

    columnas = [
        "ForecastID",
        "SlpCode",
        "CardCode",
        "ItemCode",
        "TipoForecast",
        "OcrCode3",
        "Mes",
        "CantidadAnterior",
        "CantidadNueva",
        "Timestamp",
    ]
    df_log["ForecastID"] = forecast_id  # ✅ Cambiado: el log pertenece al nuevo ID

    # 🧬 Hash estructural para auditoría reversible
    hash_repr = sha256(df_log[columnas].to_string(index=False).encode()).hexdigest()
    print(f"[DEBUG-B2] Hash estructural del log ForecastID={forecast_id}: {hash_repr}")

    _run_log_to_sql(df_log[columnas], "Forecast_LogDetalle")
    print("[DEBUG-B2] ✅ Log insertado correctamente en Forecast_LogDetalle")

    return df_log[columnas]
