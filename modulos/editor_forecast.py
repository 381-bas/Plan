# B_FCS001: Importaciones y configuración de base de datos para consultas forecast
# # ∂B_FCS001/∂B0
from __future__ import annotations
import pandas as pd
from typing import Any
import re
import hashlib
import time
import numpy as np
from utils.db import DB_PATH, run_query
import streamlit as st
from pathlib import Path
from typing import Tuple  # noqa: E402
from typing import Optional
from datetime import datetime
import sqlite3


from hashlib import sha256
from pandas.util import hash_pandas_object  # hash estructural
from session_utils import (
    normalize_df_for_hash,
    load_df_parquet_safe,
    save_df_parquet_atomic,
)


from utils.db import (
    _run_forecast_insert_get_id,
    _run_log_to_sql,
)

from config.contexto import obtener_slpcode

from utils.repositorio_forecast.repositorio_forecast_editor import (
    obtener_buffer_cliente,  # ∂B
)

from utils.repositorio_forecast.repositorio_forecast_editor import get_key_buffer


# B_FCS005: Obtener forecast editable para edición directa por cliente
# ∂B_FCS005/∂B0
def obtener_forecast_editable(
    slp_code: int,
    card_code: str,
    anio: Optional[int] = None,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Devuelve el forecast editable (cantidad, precio UN, moneda) en formato ancho 01-12,
    tomando únicamente el último ForecastID completo del cliente/año.
    - Métrica fija: "Cantidad"
    - Meses como columnas "01".."12"
    - PrecioUN: promedio por clave (ItemCode, TipoForecast, OcrCode3)
    - DocCur: primer valor por clave
    """

    # ─────────────────────────────────────────────────────────────
    # Prints de trazabilidad
    # ─────────────────────────────────────────────────────────────
    print("[CFE-002] ▶ start obtener_forecast_editable")
    print(
        f"[CFE-002] args: slp_code={slp_code}({type(slp_code).__name__}), "
        f"card_code={card_code}({type(card_code).__name__}), anio={anio}, db_path={db_path}"
    )

    # ───────────────── Guards de tipo ─────────────────
    if not isinstance(slp_code, (int, np.integer)):
        raise TypeError("[CFE-002] slp_code debe ser int")
    if not isinstance(card_code, str):
        raise TypeError("[CFE-002] card_code debe ser str")
    if anio is not None and not isinstance(anio, (int, np.integer)):
        raise TypeError("[CFE-002] anio debe ser int o None")

    # ───────────────── Filtros por año (FechEntr) ─────────────────
    filtro_anio_det = "AND strftime('%Y', fd.FechEntr) = ?" if anio is not None else ""
    filtro_anio_id = "AND strftime('%Y', FechEntr) = ?" if anio is not None else ""

    # ───────────────── Paso 1: obtener el último ForecastID ─────────────────
    q_last_id = f"""
        SELECT MAX(ForecastID) AS ForecastID
        FROM Forecast_Detalle
        WHERE SlpCode = ? AND CardCode = ?
          {filtro_anio_id}
    """.strip()
    params_last: list[Any] = [slp_code, card_code]
    if anio is not None:
        params_last.append(str(anio))
    df_last = run_query(q_last_id, db_path, tuple(params_last))
    last_id = None if df_last.empty else df_last["ForecastID"].iloc[0]

    print(f"[CFE-002] último ForecastID detectado → {last_id}")

    # Si no hay último ID, devolver estructura vacía segura
    if last_id is None or (isinstance(last_id, float) and np.isnan(last_id)):
        print(
            "[CFE-002] ⚠️ No hay ForecastID para este cliente/año → retornando estructura vacía"
        )
        cols = [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            *[f"{m:02d}" for m in range(1, 13)],
            "PrecioUN",
            "DocCur",
        ]
        empty = pd.DataFrame({c: pd.Series(dtype="float64") for c in cols})
        # Ajustar dtypes de texto
        for c in [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            "DocCur",
        ]:
            empty[c] = empty[c].astype("string")
        empty["Métrica"] = empty["Métrica"].astype("string")
        print("[CFE-002] ✅ done obtener_forecast_editable (empty-safe: no last_id)")
        return empty

    # ───────────────── Paso 2: query principal para ese ForecastID ─────────────────
    query = f"""
        SELECT
            fd.ItemCode,
            i.ItemName,
            fd.TipoForecast,
            fd.OcrCode3,
            CAST(strftime('%m', fd.FechEntr) AS INTEGER) AS Mes,
            SUM(fd.Cant)     AS Cantidad,
            AVG(fd.PrecioUN) AS PrecioUN,
            MAX(fd.DocCur)   AS DocCur
        FROM Forecast_Detalle fd
        LEFT JOIN OITM i
          ON i.ItemCode = fd.ItemCode
        WHERE fd.ForecastID = ?
          AND fd.SlpCode    = ?
          AND fd.CardCode   = ?
          {filtro_anio_det}
        GROUP BY
            fd.ItemCode, i.ItemName, fd.TipoForecast, fd.OcrCode3, Mes
        ORDER BY
            fd.ItemCode, fd.TipoForecast, Mes
    """.strip()

    # Orden de parámetros: last_id + slp + card + [anio]
    params_main: list[Any] = [int(last_id), slp_code, card_code]
    if anio is not None:
        params_main.append(str(anio))
    print(f"[CFE-002] sql params(main): {tuple(params_main)}")

    df = run_query(query, db_path, tuple(params_main))
    print(f"[CFE-002] raw result shape: {df.shape}")

    if df.empty:
        print(
            "[CFE-002] ⚠️ Forecast vacío para este cliente/año (con last_id válido) → estructura vacía"
        )
        cols = [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            *[f"{m:02d}" for m in range(1, 13)],
            "PrecioUN",
            "DocCur",
        ]
        empty = pd.DataFrame({c: pd.Series(dtype="float64") for c in cols})
        for c in [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            "DocCur",
        ]:
            empty[c] = empty[c].astype("string")
        empty["Métrica"] = empty["Métrica"].astype("string")
        print("[CFE-002] ✅ done obtener_forecast_editable (empty-safe: no rows)")
        return empty

    # ───────────────── Normalizaciones ─────────────────
    # Mes a 01..12 como texto; cantidades y precio a float
    df["Mes"] = pd.to_numeric(df["Mes"], errors="coerce").clip(1, 12).astype("Int64")
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0.0)
    df["PrecioUN"] = pd.to_numeric(df["PrecioUN"], errors="coerce").fillna(0.0)
    for c in ["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur"]:
        df[c] = df[c].astype("string")

    # ───────────────── Pivot a meses ─────────────────
    pivot = df.pivot_table(
        index=["ItemCode", "ItemName", "TipoForecast", "OcrCode3"],
        columns="Mes",
        values="Cantidad",
        aggfunc="sum",
        fill_value=0.0,
        dropna=False,
    ).reset_index()

    # Asegurar columnas 01..12 (si faltan meses, crearlos con 0.0)
    pivot.columns = [
        f"{int(c):02d}" if isinstance(c, (int, np.integer)) else c
        for c in pivot.columns
    ]

    for m in range(1, 13):
        col = f"{m:02d}"
        if col not in pivot.columns:
            pivot[col] = 0.0

    # ───────────────── Adjuntar PrecioUN y DocCur por clave ─────────────────
    precio_un = df.groupby(["ItemCode", "TipoForecast", "OcrCode3"], as_index=False)[
        "PrecioUN"
    ].mean()
    doc_cur = df.groupby(["ItemCode", "TipoForecast", "OcrCode3"], as_index=False)[
        "DocCur"
    ].first()

    pivot = pivot.merge(
        precio_un, on=["ItemCode", "TipoForecast", "OcrCode3"], how="left"
    ).merge(doc_cur, on=["ItemCode", "TipoForecast", "OcrCode3"], how="left")

    # ───────────────── Métrica y orden final ─────────────────
    pivot["Métrica"] = "Cantidad"
    months = [f"{m:02d}" for m in range(1, 13)]
    orden = [
        "ItemCode",
        "ItemName",
        "TipoForecast",
        "Métrica",
        "OcrCode3",
        *months,
        "PrecioUN",
        "DocCur",
    ]
    # asegurar tipos numéricos en meses/precio
    for m in months:
        pivot[m] = (
            pd.to_numeric(pivot[m], errors="coerce").fillna(0.0).astype("float64")
        )
    pivot["PrecioUN"] = (
        pd.to_numeric(pivot["PrecioUN"], errors="coerce").fillna(0.0).astype("float64")
    )
    pivot["Métrica"] = pivot["Métrica"].astype("string")

    # ───────────────── Prints de diagnóstico ─────────────────
    print(f"[CFE-002] editable shape: {pivot.shape}")
    print(
        f"[CFE-002] meses present → {', '.join([c for c in months if c in pivot.columns])}"
    )
    print(
        f"[CFE-002] PrecioUN stats: count={pivot['PrecioUN'].notna().sum()}, "
        f"mean={float(pivot['PrecioUN'].mean()):.4f}"
    )
    try:
        # vista rápida (primeras filas, sin cortar columnas)
        with pd.option_context("display.max_columns", None):
            print("[CFE-002] head preview:")
            print(pivot.head().to_string(index=False))
    except Exception as e:
        print(f"[CFE-002] head preview error: {e}")

    print("[CFE-002] ✅ done obtener_forecast_editable")
    return pivot[orden]


def autocompletar_precios_en_buffer(cliente: str) -> None:
    """
    Si en alguna fila/mes: Cantidad>0 y Precio==0, completa Precio con el 'precio de referencia'
    de esa fila: el primer valor de precio no-cero del año para esa combinación
    (ItemCode, TipoForecast, OcrCode3). Si no hay ninguno, deja 0.0.
    """
    key = get_key_buffer(cliente)
    if key not in st.session_state:
        print("[AUTO$] buffer no existe -> skip")
        return

    df = st.session_state[key].reset_index().copy()
    months = [f"{m:02d}" for m in range(1, 13)]

    # separar bloques
    cant = df[df["Métrica"] == "Cantidad"].copy()
    prec = df[df["Métrica"] == "Precio"].copy()

    # join por clave base
    keys = ["ItemCode", "TipoForecast", "OcrCode3"]
    prec = prec.merge(cant[keys + months], on=keys, how="left", suffixes=("", "__cant"))

    # para cada fila, obtener precio_ref (primer precio no-cero del año)
    price_cols = months

    def _precio_ref(row):
        vals = [row[c] for c in price_cols if pd.notna(row[c]) and float(row[c]) != 0.0]
        return float(vals[0]) if vals else 0.0

    prec["__precio_ref"] = prec.apply(_precio_ref, axis=1)

    # completar por mes: si Cantidad>0 y Precio==0 -> usar precio_ref; else mantener
    for m in months:
        cant_col = f"{m}__cant"
        prec[m] = np.where(
            (pd.to_numeric(prec[cant_col], errors="coerce").fillna(0.0) > 0.0)
            & (pd.to_numeric(prec[m], errors="coerce").fillna(0.0) == 0.0),
            prec["__precio_ref"],
            prec[m],
        ).astype("float64")

    # limpiar columnas auxiliares y rearmar el buffer
    prec = prec.drop(
        columns=[f"{m}__cant" for m in months] + ["__precio_ref"], errors="ignore"
    )
    df_out = pd.concat([cant, prec], ignore_index=True).sort_values(keys + ["Métrica"])
    st.session_state[key] = df_out.set_index(
        ["ItemCode", "TipoForecast", "Métrica"]
    ).sort_index()
    print("[AUTO$] precios autocompletados en buffer (pre-guardado)")


# B_BUF003: Inicialización extendida del buffer forecast con estructura y Métrica
# # ∂B_BUF003/∂B0
def inicializar_buffer_cliente(
    cliente: str,
    df_base: pd.DataFrame,
    moneda_default: str = "CLP",
    precio_autofill_mode: str = "strict",  # "strict": Precio=PrecioUN solo donde Cantidad>0; "spread": si hay alguna Cantidad>0 en el año, todos los meses toman PrecioUN
    force_reinit: bool = False,  # recalcula el buffer aunque ya exista en sesión
) -> None:
    """
    Inicializa el buffer editable del cliente en st.session_state[get_key_buffer(cliente)].

    Métricas:
      - "Cantidad": usa los valores mensuales (01..12) de df_base.
      - "Precio":
          * modo "strict" (default): para cada mes, Precio = PrecioUN si Cantidad>0; en caso contrario 0.0.
          * modo "spread": si la suma de Cantidad en el año > 0 para la fila, todos los meses toman PrecioUN;
                           si no hay cantidad en ningún mes, todos los meses quedan en 0.0.
      En ambos modos, el precio queda editable en la UI.

    Requisitos mínimos en df_base: ["ItemCode","TipoForecast"] + meses "01".."12" (si faltan, se crean en 0.0).
    Mantiene/crea "OcrCode3", "DocCur"; si está "ItemName" y/o "Linea", se preservan.
    Guarda con índice ["ItemCode","TipoForecast","Métrica"] (la UI hace reset_index()).
    """
    print("[RFE-004] ▶ start inicializar_buffer_cliente")
    print(
        f"[RFE-004] args: cliente={cliente}({type(cliente).__name__}), "
        f"df_base.shape={getattr(df_base,'shape',None)}, moneda_default={moneda_default}, "
        f"precio_autofill_mode={precio_autofill_mode}, force_reinit={force_reinit}"
    )

    # ── Guards
    if not isinstance(cliente, str):
        raise TypeError("[RFE-004] cliente debe ser str")
    if not isinstance(df_base, pd.DataFrame):
        raise TypeError("[RFE-004] df_base debe ser DataFrame")
    if precio_autofill_mode not in ("strict", "spread"):
        raise ValueError("[RFE-004] precio_autofill_mode debe ser 'strict' o 'spread'")

    # ── Clave de sesión
    key = get_key_buffer(cliente)  # "forecast_buffer_{cliente}"
    print(f"[RFE-004] key de buffer -> {key}")
    if key in st.session_state and not force_reinit:
        print(
            f"[RFE-004] buffer ya existe en sesión: {key} -> skip (usa force_reinit=True para recalcular)"
        )
        return

    # ── Copia / normalización base
    df = df_base.copy()
    df.columns = df.columns.astype(str)

    required_cols = {"ItemCode", "TipoForecast"}
    faltan = required_cols - set(df.columns)
    if faltan:
        raise ValueError(f"[RFE-004] Faltan columnas esenciales en df_base: {faltan}")

    # Tipos de texto
    for c in ["ItemCode", "TipoForecast"]:
        df[c] = df[c].astype("string").str.strip()
    if "ItemName" in df.columns:
        df["ItemName"] = df["ItemName"].astype("string")
    if "OcrCode3" not in df.columns:
        df["OcrCode3"] = ""
    df["OcrCode3"] = df["OcrCode3"].astype("string")
    if "DocCur" not in df.columns:
        df["DocCur"] = moneda_default
    df["DocCur"] = df["DocCur"].astype("string")

    # Meses 01..12
    months = [f"{m:02d}" for m in range(1, 13)]
    for m in months:
        if m not in df.columns:
            df[m] = 0.0
        df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0.0).astype("float64")

    # ── Asegurar ambos tipos por clave base (Firme/Proyectado)
    tipos_obj = ["Firme", "Proyectado"]
    base_keys = ["ItemCode", "OcrCode3", "DocCur"]  # robusto por ítem/centro/moneda
    if "ItemName" in df.columns:
        base_keys.insert(1, "ItemName")

    frames = []
    for _, sub in df.groupby(base_keys, dropna=False):
        presentes = set(sub["TipoForecast"].dropna().unique().tolist())
        for t in tipos_obj:
            if t in presentes:
                frames.append(sub[sub["TipoForecast"] == t].copy())
            else:
                clone = sub.iloc[[0]].copy()
                clone["TipoForecast"] = t
                for m in months:
                    clone[m] = 0.0
                frames.append(clone)
    df = pd.concat(frames, ignore_index=True) if frames else df

    # ── PrecioUN por clave (si existe en df_base)
    precio_map = None
    if "PrecioUN" in df.columns:
        precio_map = df[["ItemCode", "TipoForecast", "OcrCode3", "PrecioUN"]].copy()
        precio_map["PrecioUN"] = pd.to_numeric(
            precio_map["PrecioUN"], errors="coerce"
        ).fillna(0.0)

    # Eliminar columnas de precio crudo para evitar duplicidades
    df = df.drop(
        columns=[
            c for c in ["PrecioUN", "_PrecioUN_", "PrecioUnitario"] if c in df.columns
        ],
        errors="ignore",
    )

    # ── Métrica = Cantidad
    cantidad = df.copy()
    cantidad["Métrica"] = "Cantidad"

    # ── Métrica = Precio (autocompletado)
    precio = df.copy()
    precio["Métrica"] = "Precio"
    if precio_map is not None and not precio_map.empty:
        precio = precio.merge(
            precio_map, on=["ItemCode", "TipoForecast", "OcrCode3"], how="left"
        )
        precio["PrecioUN"] = pd.to_numeric(precio["PrecioUN"], errors="coerce").fillna(
            0.0
        )

        if precio_autofill_mode == "strict":
            # Por mes: si Cantidad>0 → PrecioUN; else → 0.0
            for m in months:
                precio[m] = np.where(cantidad[m] > 0, precio["PrecioUN"], 0.0).astype(
                    "float64"
                )
        else:  # spread
            # Si hay alguna cantidad en el año → todos los meses toman PrecioUN; de lo contrario 0.0
            any_qty = (cantidad[months].sum(axis=1) > 0).astype(bool)
            for m in months:
                precio[m] = np.where(any_qty, precio["PrecioUN"], 0.0).astype("float64")

        # En el buffer, los meses ya llevan el precio editable (no necesitamos la columna auxiliar)
        precio = precio.drop(columns=["PrecioUN"], errors="ignore")
    else:
        for m in months:
            precio[m] = 0.0  # sin referencia → 0.0 editable

    # ── Unir métricas
    df_combo = pd.concat([cantidad, precio], ignore_index=True)

    # ── Orden de columnas amigable para el editor
    base_cols = ["ItemCode"]
    if "ItemName" in df_combo.columns:
        base_cols.append("ItemName")
    base_cols += ["TipoForecast", "Métrica", "OcrCode3"]
    tail_cols = []
    if "DocCur" in df_combo.columns:
        tail_cols.append("DocCur")
    if "Linea" in df_combo.columns:
        tail_cols.append("Linea")  # si estaba en df_base, la preservamos

    ordered = (
        [c for c in base_cols if c in df_combo.columns]
        + months
        + [c for c in tail_cols if c in df_combo.columns]
    )
    df_combo = df_combo[ordered].copy()

    # ── Guardar en sesión con índice esperado
    st.session_state[key] = df_combo.set_index(
        ["ItemCode", "TipoForecast", "Métrica"]
    ).sort_index()

    # ── Trazas
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


# B_SYN001: Sincronización persistente del buffer editable con edición de usuario
# # ∂B_SYN001/∂B0
def sincronizar_buffer_edicion(
    df_buffer: pd.DataFrame, key_buffer: str
) -> pd.DataFrame:
    """
    Refuerza persistencia de edición mixta:
    - Aplica cambios históricos del buffer editado a nueva vista df_buffer
    - Usa combinación única (ItemCode, TipoForecast, Métrica, OcrCode3) como clave de actualización
    """
    print(f"🔄 [SYNC-START] Iniciando sincronización para buffer: {key_buffer}")
    print(f"📊 [SYNC-INFO] df_buffer shape: {df_buffer.shape}")

    key_state = f"{key_buffer}_editado"
    if key_state not in st.session_state:
        print(f"❌ [SYNC-SKIP] No hay estado editado para: {key_buffer}")
        return df_buffer

    df_editado = st.session_state[key_state]
    print(f"📝 [SYNC-INFO] df_editado shape: {df_editado.shape}")

    # 🧠 NUEVO BLOQUE PARA CORTAR LOOP
    if hash_semantico(df_editado) == hash_semantico(df_buffer):
        print(
            f"🛑 [SCANNER] Sincronización evitada: edición idéntica para {key_buffer}"
        )
        return df_buffer

    columnas_clave = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
    print(f"🔑 [SYNC-INFO] Columnas clave: {columnas_clave}")

    # ✅ Validar unicidad de clave compuesta antes de indexar
    if df_editado.duplicated(subset=columnas_clave).any():
        print("[❌ DEBUG-SYNC] df_editado tiene claves duplicadas - update() fallará")
        duplicados = df_editado[
            df_editado.duplicated(subset=columnas_clave, keep=False)
        ].sort_values(columnas_clave)
        print(f"📋 [DUPLICADOS] {len(duplicados)} registros duplicados encontrados:")
        print(duplicados.head())
        raise ValueError(
            "Claves duplicadas detectadas en df_editado. No se puede sincronizar con update()"
        )

    columnas_mes = [f"{i:02d}" for i in range(1, 13)]
    print(f"📅 [SYNC-INFO] Columnas mes: {columnas_mes}")

    # 🔒 Filtrar columnas prohibidas
    columnas_prohibidas = ["PrecioUN", "_PrecioUN_", "PrecioUnitario"]
    columnas_eliminadas = [c for c in columnas_prohibidas if c in df_editado.columns]
    if columnas_eliminadas:
        print(f"🚫 [SYNC-INFO] Eliminando columnas prohibidas: {columnas_eliminadas}")

    df_editado = df_editado.drop(
        columns=columnas_eliminadas,
        errors="ignore",
    )

    # 🔍 Validar columnas mensuales
    faltantes = [col for col in columnas_mes if col not in df_editado.columns]
    if faltantes:
        print(f"❌ [SYNC-ERROR] Faltan columnas mensuales: {faltantes}")
        raise ValueError(
            f"El buffer editado carece de columnas mensuales requeridas: {faltantes}"
        )

    df_actualizado = df_buffer.copy()
    print(f"📋 [SYNC-INFO] df_actualizado shape inicial: {df_actualizado.shape}")

    try:
        print("🔧 [SYNC-STEP] Configurando índices...")
        df_actualizado = df_actualizado.set_index(columnas_clave)
        df_editado = df_editado.set_index(columnas_clave)
        print(
            f"📊 [SYNC-INFO] Índices configurados - df_actualizado: {df_actualizado.shape}, df_editado: {df_editado.shape}"
        )

        # 🧪 Validar cobertura de claves
        claves_faltantes = set(df_actualizado.index) - set(df_editado.index)
        if claves_faltantes:
            print(
                f"⚠️ [SYNC-WARN] {len(claves_faltantes)} combinaciones clave no fueron editadas"
            )

        # 🛑 Ordenar índices para evitar PerformanceWarning
        print("🔃 [SYNC-STEP] Ordenando índices...")
        df_actualizado = df_actualizado.sort_index()
        df_editado = df_editado.sort_index()

        # 🧠 Evitar update si no hay diferencias
        print("🔍 [SYNC-STEP] Comparando datos...")
        try:
            iguales = df_actualizado[columnas_mes].equals(df_editado[columnas_mes])
            print(f"📊 [SYNC-COMP] ¿Datos iguales? {iguales}")
        except Exception as e:
            print(f"[⚠️ COMPARACIÓN FALLIDA] {e}")
            iguales = False

        if not iguales:
            print("🔄 [SYNC-STEP] Aplicando actualizaciones...")
            df_actualizado.update(df_editado[columnas_mes])
            print("✅ [SYNC-STEP] Actualizaciones aplicadas")
        else:
            print("⏭️ [SYNC-STEP] Sin cambios - saltando actualización")

        # ✅ Restaurar columnas adicionales que no fueron tocadas por edición
        columnas_extra = [
            col
            for col in df_buffer.columns
            if col not in df_actualizado.reset_index().columns
        ]
        if columnas_extra:
            print(
                f"📋 [SYNC-STEP] Restaurando {len(columnas_extra)} columnas extra: {columnas_extra}"
            )
            for col in columnas_extra:
                df_actualizado[col] = df_buffer.set_index(columnas_clave)[col]

        df_actualizado = df_actualizado.reset_index()
        print(
            f"✅ [SYNC-SUCCESS] Sincronización completada - shape final: {df_actualizado.shape}"
        )

    except Exception as e:
        print(f"❌ [SYNC-ERROR] No se pudo sincronizar buffer editado: {e}")
        import traceback

        traceback.print_exc()
        return df_buffer

    return df_actualizado


def sincronizar_buffer_local(
    df_buffer: pd.DataFrame, df_editado: pd.DataFrame
) -> Tuple[pd.DataFrame, bool]:
    """Fusiona cambios del editor con el buffer y devuelve (df_final, hay_cambios)."""
    import time

    t0 = time.perf_counter()
    print("[SYNC.LOCAL.INFO] start")

    columnas_clave = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]

    # Detectar dinámicamente columnas de meses (01..12 o 1..12)
    columnas_mes = sorted(
        [c for c in df_editado.columns if c.isdigit() and len(c) <= 2],
        key=lambda x: int(x),
    )
    columnas_req = columnas_clave + columnas_mes
    if missing := (set(columnas_req) - set(df_editado.columns)):
        print(f"[SYNC.LOCAL.ERROR] missing_columns editado={missing}")
        raise ValueError(
            f"El DataFrame editado carece de columnas requeridas: {missing}"
        )

    print(f"[SYNC.LOCAL.INFO] meses_detectados={len(columnas_mes)}")

    # Índices normalizados y ordenados (mejor para update)
    buf_idx = df_buffer.set_index(columnas_clave).sort_index()
    edi_idx = df_editado.set_index(columnas_clave).sort_index()

    # Unión de índices (contempla altas/bajas)
    idx_union = buf_idx.index.union(edi_idx.index)
    buf_idx = buf_idx.reindex(idx_union).sort_index()
    edi_idx = edi_idx.reindex(idx_union).sort_index()

    print(
        f"[SYNC.LOCAL.INFO] shapes buffer={df_buffer.shape} "
        f"editado={df_editado.shape} union_idx={len(idx_union)}"
    )

    # Comparación tolerante a float/NaN
    if columnas_mes:
        diff_array = ~np.isclose(
            buf_idx[columnas_mes], edi_idx[columnas_mes], atol=1e-6, equal_nan=True
        )
        dif_mask = pd.DataFrame(diff_array, index=buf_idx.index, columns=columnas_mes)

        total_diff = int(dif_mask.values.sum())
        filas_diff = int(dif_mask.any(axis=1).sum())
        hay_cambios = total_diff > 0

        if hay_cambios:
            print(f"[SYNC.CHANGE] cells={total_diff} rows={filas_diff}")
            # Aplicar cambios de meses
            buf_idx.update(edi_idx[columnas_mes])

            # Filas completamente nuevas (todas las columnas-mes distintas)
            filas_nuevas = dif_mask.index[dif_mask.all(axis=1)]
            if len(filas_nuevas):
                print(f"[SYNC.LOCAL.NEW] rows={len(filas_nuevas)}")
                buf_idx.loc[filas_nuevas, columnas_mes] = edi_idx.loc[
                    filas_nuevas, columnas_mes
                ]
        else:
            print("[SYNC.LOCAL.INFO] sin_diferencias")
    else:
        # Sin columnas de mes: no cambia nada
        print("[SYNC.LOCAL.WARN] sin_columnas_mes -> no_changes")
        hay_cambios = False

    # Reconstrucción con columnas extra (ItemName, DocCur, etc.)
    cols_extra_union = [
        c
        for c in set(df_buffer.columns).union(df_editado.columns)
        if c not in columnas_clave and c not in columnas_mes
    ]
    print(f"[SYNC.LOCAL.INFO] cols_extra={len(cols_extra_union)}")
    if cols_extra_union:
        buf_idx[cols_extra_union] = df_buffer.set_index(columnas_clave)[
            cols_extra_union
        ].reindex(buf_idx.index)
        edi_extra = df_editado.set_index(columnas_clave)[cols_extra_union].reindex(
            buf_idx.index
        )
        buf_idx.update(edi_extra)

    df_final = buf_idx.reset_index().reindex(columns=columnas_req + cols_extra_union)

    # Mantener dtypes de columnas existentes
    dtype_map = {c: t for c, t in df_buffer.dtypes.items() if c in df_final.columns}
    df_final = df_final.astype(dtype_map, errors="ignore")
    print(f"[SYNC.LOCAL.INFO] dtypes_aplicados={len(dtype_map)}")

    print(
        f"[SYNC.LOCAL.INFO] end shape={df_final.shape} changes={hay_cambios} "
        f"elapsed={time.perf_counter()-t0:.3f}s"
    )
    return df_final, hay_cambios


# B_SYN002: Sincronización y guardado individual de buffer editado para cliente
# # ∂B_SYN002/∂B0
def sincronizar_para_guardado_final(key_buffer: str, df_editado: pd.DataFrame):
    import time

    t0 = time.perf_counter()
    print(f"[SYNC.FINAL.INFO] start key={key_buffer}")
    print(
        f"[SYNC.FINAL.INFO] df_init shape={df_editado.shape} cols={len(df_editado.columns)}"
    )
    print(f"[SYNC.FINAL.INFO] session_keys={len(st.session_state.keys())}")

    # 1) Unificación de métricas Cantidad + Precio
    t1 = time.perf_counter()
    print("[SYNC.FINAL.INFO] unify metrics Cantidad+Precio")
    df_editado_unificado = pd.concat(
        [
            df_editado[df_editado["Métrica"] == "Cantidad"],
            df_editado[df_editado["Métrica"] == "Precio"],
        ],
        ignore_index=True,
    )
    metricas_count = df_editado_unificado["Métrica"].value_counts().to_dict()
    print(
        f"[SYNC.FINAL.INFO] unified_rows={len(df_editado_unificado)} "
        f"metric_dist={metricas_count} elapsed={time.perf_counter()-t1:.3f}s"
    )

    # 2) Recuperar buffer actual
    t2 = time.perf_counter()
    print(f"[SYNC.FINAL.INFO] load base buffer key={key_buffer}")
    df_base_actual = obtener_buffer_cliente(key_buffer).reset_index()
    print(
        f"[SYNC.FINAL.INFO] base shape={df_base_actual.shape} "
        f"cols={len(df_base_actual.columns)} elapsed={time.perf_counter()-t2:.3f}s"
    )

    # 3) Sincronizar (devuelve tupla)
    t3 = time.perf_counter()
    print("[SYNC.FINAL.INFO] run sincronizar_buffer_local")
    df_sync, hay_cambios = sincronizar_buffer_local(
        df_base_actual, df_editado_unificado
    )
    print(
        f"[SYNC.FINAL.INFO] sync_result changes={hay_cambios} shape={df_sync.shape} "
        f"elapsed={time.perf_counter()-t3:.3f}s"
    )

    if not hay_cambios:
        print("[SYNC.FINAL.INFO] no_changes -> skip final save")
        print(
            f"[SYNC.FINAL.INFO] end shape={df_base_actual.shape} elapsed={time.perf_counter()-t0:.3f}s"
        )
        return df_base_actual

    # ──────────────────────────────────────────────────────────────
    # Solo si hay cambios reales
    # ──────────────────────────────────────────────────────────────
    print("[SYNC.FINAL.INFO] process changes")
    idx_stats = df_sync[["ItemCode", "TipoForecast", "Métrica"]].nunique()
    print(
        f"[SYNC.FINAL.INFO] cardinalidad ItemCode={idx_stats['ItemCode']} "
        f"TipoForecast={idx_stats['TipoForecast']} Metrica={idx_stats['Métrica']}"
    )

    # Validación de nulos en columnas clave
    null_warned = False
    for col in ["ItemCode", "TipoForecast", "Métrica"]:
        nulos = int(df_sync[col].isna().sum())
        if nulos:
            print(f"[SYNC.FINAL.WARN] nulls in {col} count={nulos}")
            null_warned = True
    if not null_warned:
        print("[SYNC.FINAL.INFO] no nulls in key columns")

    # Guardar en session_state ordenado por índice compuesto
    t4 = time.perf_counter()
    print("[SYNC.FINAL.INFO] save buffer to session_state")
    df_sync = df_sync.set_index(["ItemCode", "TipoForecast", "Métrica"]).sort_index()
    st.session_state[key_buffer] = df_sync
    df_guardar = df_sync.reset_index()
    print(
        f"[SYNC.FINAL.INFO] saved rows={len(df_guardar)} cols={len(df_guardar.columns)} "
        f"elapsed={time.perf_counter()-t4:.3f}s"
    )

    # Guardado temporal local
    t5 = time.perf_counter()
    print("[SYNC.FINAL.INFO] save temp local")
    guardar_temp_local(key_buffer, df_guardar)
    print(f"[SYNC.FINAL.INFO] temp local saved elapsed={time.perf_counter()-t5:.3f}s")

    # Actualización buffer global
    t6 = time.perf_counter()
    print("[SYNC.FINAL.INFO] update global buffer")
    actualizar_buffer_global(df_guardar, key_buffer)
    print(
        f"[SYNC.FINAL.INFO] global buffer updated elapsed={time.perf_counter()-t6:.3f}s"
    )

    # Marcar cliente como editado
    cliente = key_buffer.replace("forecast_buffer_", "")
    editados = st.session_state.get("clientes_editados", set())
    editados.add(cliente)
    st.session_state["clientes_editados"] = editados
    print(f"[SYNC.FINAL.INFO] cliente_editado add={cliente} total={len(editados)}")

    # Debug hash (mejor esfuerzo)
    try:
        hash_pre = hash_df(df_editado_unificado.sort_index(axis=1))
    except Exception:
        try:
            hash_pre = hash(df_editado_unificado.to_csv(index=False))
        except Exception:
            hash_pre = None
    if hash_pre is not None:
        print(f"[SYNC.FINAL.INFO] df_pre_sync_hash={hash_pre}")

    print(
        f"[SYNC.FINAL.INFO] end shape={df_guardar.shape} elapsed={time.perf_counter()-t0:.3f}s"
    )
    return df_guardar


BASE_TEMP = Path(__file__).resolve().parent.parent / "temp_ediciones"
BASE_TEMP.mkdir(parents=True, exist_ok=True)


# B_TMP002: Construcción de ruta de backup temporal para cliente
# # ∂B_TMP002/∂B0
def _ruta_temp(cliente: str) -> Path:
    return BASE_TEMP / f"{cliente}_forecast.parquet"


# B_HDF001: Hash robusto de DataFrame usando SHA-256 (control de integridad)
# # ∂B_HDF001/∂B0
def hash_df(df):
    return hashlib.sha256(
        pd.util.hash_pandas_object(df.sort_index(axis=1), index=True).values
    ).hexdigest()


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


# B_SYN002: Actualización simbólica y persistente del buffer editado en sesión global
# # ∂B_SYN002/∂B0
def actualizar_buffer_global(df_editado: pd.DataFrame, key_buffer: str):
    """
    Almacena el DataFrame editado en session_state como buffer vivo.
    Usa clave simbólica con sufijo '_editado' para edición persistente.
    Logs: [BUFFER.GLOBAL.INFO]/[BUFFER.GLOBAL.WARN]/[BUFFER.GLOBAL.ERROR] en una sola línea.
    """
    import time

    t0 = time.perf_counter()
    key_state = f"{key_buffer}_editado"
    rows, cols = df_editado.shape if df_editado is not None else (0, 0)
    print(f"[BUFFER.GLOBAL.INFO] start key_buffer={key_buffer} rows={rows} cols={cols}")

    # Validación estructural mínima
    columnas_requeridas = {"ItemCode", "TipoForecast", "Métrica", "OcrCode3"}
    faltantes = columnas_requeridas - set(df_editado.columns)
    if faltantes:
        msg = f"El DataFrame editado carece de columnas requeridas: {faltantes}"
        print(f"[BUFFER.GLOBAL.ERROR] missing_columns={faltantes}")
        raise ValueError(msg)

    # Limpieza defensiva
    columnas_prohibidas = ["PrecioUN", "_PrecioUN_", "PrecioUnitario"]
    a_eliminar = [c for c in columnas_prohibidas if c in df_editado.columns]
    if a_eliminar:
        print(f"[BUFFER.GLOBAL.WARN] dropping_forbidden_columns={a_eliminar}")
        df_editado = df_editado.drop(columns=a_eliminar, errors="ignore")

    # Buffer de edición persistente (copia defensiva)
    st.session_state[key_state] = df_editado.copy()
    print(
        f"[BUFFER.GLOBAL.INFO] session[{key_state}] set rows={len(df_editado)} cols={len(df_editado.columns)}"
    )

    # ✅ Sincronizar buffer principal con índice compuesto
    st.session_state[key_buffer] = df_editado.set_index(
        ["ItemCode", "TipoForecast", "Métrica"]
    )
    print(
        f"[BUFFER.GLOBAL.INFO] session[{key_buffer}] set index=('ItemCode','TipoForecast','Métrica') rows={len(df_editado)}"
    )

    # Marca interna de sincronización
    st.session_state["__buffer_editado__"] = True
    print("[BUFFER.GLOBAL.INFO] flag __buffer_editado__=True")

    print(
        f"[BUFFER.GLOBAL.INFO] end key_buffer={key_buffer} elapsed={time.perf_counter()-t0:.3f}s"
    )


# B_VFD001: Validación estructural y de contenido del DataFrame de forecast
# # ∂B_VFD001/∂B0
def validar_forecast_dataframe(df: pd.DataFrame) -> list[str]:
    print("🔍 [VALIDATION-START] Iniciando validación de DataFrame")

    errores: list[str] = []
    columnas_mes = [str(m).zfill(2) for m in range(1, 13)]

    # Columnas permitidas (base) + opcionales que NO deben gatillar error:
    # 👉 Se agrega explícitamente ItemName (y Linea) para cumplir el punto C.
    permitidas_base = {
        "itemcode",
        "tipoforecast",
        "métrica",
        "ocrcode3",
        "doccur",
        "itemname",
        "linea",  # opcionales toleradas
    }

    df = df.copy()
    df.columns = df.columns.astype(str)

    campos_requeridos = ["ItemCode", "TipoForecast", "Métrica", "DocCur"]
    for col in campos_requeridos:
        if col not in df.columns:
            errores.append(f"Falta la columna requerida: {col}")
            print(f"❌ [VALIDATION-ERROR] Falta columna requerida: {col}")
        else:
            print(f"✅ [VALIDATION-OK] Columna requerida presente: {col}")

    # Normalización de valores clave
    if "ItemCode" in df.columns:
        df["ItemCode"] = df["ItemCode"].astype(str).str.strip()
        print(
            f"✅ [VALIDATION-INFO] ItemCode normalizado - únicos: {df['ItemCode'].nunique()}"
        )
    if "TipoForecast" in df.columns:
        df["TipoForecast"] = df["TipoForecast"].astype(str).str.strip().str.capitalize()
        print(
            f"✅ [VALIDATION-INFO] TipoForecast normalizado - valores: {df['TipoForecast'].unique().tolist()}"
        )
    if "Métrica" in df.columns:
        df["Métrica"] = df["Métrica"].astype(str).str.strip().str.capitalize()
        print(
            f"✅ [VALIDATION-INFO] Métrica normalizada - valores: {df['Métrica'].unique().tolist()}"
        )
    if "DocCur" in df.columns:
        df["DocCur"] = df["DocCur"].astype(str).str.strip().str.upper()
        print(
            f"✅ [VALIDATION-INFO] DocCur normalizado - valores: {df['DocCur'].unique().tolist()}"
        )

    # Validaciones de contenido
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
        if not df["DocCur"].str.match(r"^[A-Z]{3}$").all():
            errores.append(
                "La columna 'DocCur' debe contener códigos de moneda de 3 letras (ej. CLP, USD, EUR)."
            )
            print(
                f"❌ [VALIDATION-ERROR] DocCur inválidos: {df['DocCur'].unique().tolist()}"
            )
        else:
            print("✅ [VALIDATION-OK] DocCur válidos")

    # Validación de columnas mes
    meses_faltantes = [col for col in columnas_mes if col not in df.columns]
    if meses_faltantes:
        errores.append(f"Faltan columnas de mes: {meses_faltantes}")
        print(f"❌ [VALIDATION-ERROR] Meses faltantes: {meses_faltantes}")
    else:
        print("✅ [VALIDATION-OK] Todas las columnas mensuales presentes")

    # Validación estructural extendida
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

    # Validación de columnas residuales sueltas
    if "PrecioUN" in df.columns and "Métrica" in df.columns:
        # Si 'Precio' existe, su granularidad debe estar en columnas mensuales, no en una suelta 'PrecioUN'
        if not df[df["Métrica"] == "Precio"].empty:
            errores.append(
                "La columna suelta 'PrecioUN' no debe existir cuando 'Métrica' = 'Precio'. Distribuir por meses."
            )
            print("❌ [VALIDATION-ERROR] Columna PrecioUN no permitida")

    # Validación de columnas inesperadas (tolerando ItemName/Linea)
    col_extranas = [
        col
        for col in df.columns
        if (col.lower() not in permitidas_base) and (col not in columnas_mes)
    ]
    if col_extranas:
        errores.append(f"Columnas inesperadas detectadas: {col_extranas}")
        print(f"❌ [VALIDATION-ERROR] Columnas inesperadas: {col_extranas}")
    else:
        print("✅ [VALIDATION-OK] Sin columnas inesperadas (ItemName/Linea toleradas)")

    if errores:
        print(f"❌ [VALIDATION-END] Validación fallida con {len(errores)} errores")
        return errores

    # Validación de tipo de datos y negativos
    df[columnas_mes] = df[columnas_mes].apply(pd.to_numeric, errors="coerce").fillna(0)
    print("✅ [VALIDATION-INFO] Columnas mensuales convertidas a numéricas")

    # Negativos en cualquiera de las métricas (Cantidad/Precio)
    for col in columnas_mes:
        negativos = df[df[col] < 0]
        if not negativos.empty:
            codigos = negativos["ItemCode"].astype(str).unique().tolist()[:5]
            errores.append(f"Valores negativos en mes {col} para: {codigos}")
            print(
                f"❌ [VALIDATION-ERROR] Valores negativos en {col}: {len(negativos)} registros (ej: {codigos})"
            )

    # Validación TipoForecast
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


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _ocr3_a_linea(ocr: str) -> str:
    """
    Mapea el valor de OcrCode3 al concepto de 'Linea'.

    Reglas actuales:
        - 'Pta-' ⭢ 'Planta'
        - 'Trd-' ⭢ 'Trader'
        - Cualquier otro prefijo o valor nulo ⭢ 'Desconocido'
    """
    if not ocr:  # None, NaN o string vacío
        return "Desconocido"
    if re.match(r"(?i)^pta[-_]", ocr):
        return "Planta"
    if re.match(r"(?i)^trd[-_]", ocr):
        return "Trader"
    return "Desconocido"


# B_TRF002: Conversión de DataFrame métrico de forecast a formato largo SCANNER
# ∂B_TRF002/∂B1
def df_forecast_metrico_to_largo(
    df: pd.DataFrame,
    anio: int,
    cardcode: str,
    slpcode: int,
    debug: bool = False,
) -> pd.DataFrame:
    """
    Convierte forecast “métrico” (columnas 01–12) a formato largo sin duplicados.

    Reglas:
      - Requiere: ["ItemCode","TipoForecast","OcrCode3","DocCur","Métrica"].
      - Métrica ∈ {"Cantidad","Precio"}.
      - Columnas "01".."12" faltantes → 0.
      - Cant = suma por clave; PrecioUN = último no-cero (si no hay, último valor).
      - FechEntr = primer día de cada mes de `anio` (date).
    """

    _dbg = print if debug else (lambda *a, **k: None)
    _dbg(
        f"[DEBUG-LARGO] ▶ Transformando forecast largo: card={cardcode}, año={anio}, slp={slpcode}"
    )

    columnas_mes = [f"{m:02d}" for m in range(1, 13)]
    columnas_base = ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Métrica"]

    df = df.copy()
    df.columns = df.columns.astype(str)

    # Validaciones base
    faltantes = [c for c in columnas_base if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas necesarias: {faltantes}")

    # Métricas válidas
    valid_metricas = {"Cantidad", "Precio"}
    metricas_distintas = set(df["Métrica"].dropna().unique().tolist())
    no_validas = metricas_distintas - valid_metricas
    if no_validas:
        raise ValueError(
            f"Métrica(s) no válidas: {sorted(no_validas)}. Esperadas: {sorted(valid_metricas)}"
        )

    # Garantizar columnas de mes y tipificarlas a numérico; NaN→0
    for col in columnas_mes:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    _dbg(f"[DEBUG-LARGO] Columnas disponibles: {df.columns.tolist()}")
    _dbg(f"[DEBUG-LARGO] Filas iniciales antes de deduplicar: {len(df)}")

    # Deduplicación previa (conservar última por clave lógica)
    df = df.sort_index().drop_duplicates(
        subset=["ItemCode", "TipoForecast", "OcrCode3", "Métrica"], keep="last"
    )
    _dbg(f"[DEBUG-LARGO] Filas después de deduplicación previa: {len(df)}")

    # Split por métrica
    df_cant = df[df["Métrica"] == "Cantidad"].copy()
    df_prec = df[df["Métrica"] == "Precio"].copy()

    # Melt (Cant)
    df_cant_largo = df_cant.melt(
        id_vars=["ItemCode", "TipoForecast", "OcrCode3", "DocCur"],
        value_vars=columnas_mes,
        var_name="Mes",
        value_name="Cant",
    )
    # Melt (Precio)
    df_prec_largo = df_prec.melt(
        id_vars=["ItemCode", "TipoForecast", "OcrCode3", "DocCur"],
        value_vars=columnas_mes,
        var_name="Mes",
        value_name="PrecioUN",
    )

    # Merge y saneo
    df_largo = (
        pd.merge(
            df_cant_largo,
            df_prec_largo,
            on=["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Mes"],
            how="outer",
        )
        .fillna({"Cant": 0, "PrecioUN": 0})
        .reset_index(drop=True)
    )

    # Consolidación sin duplicados:
    # - Cant: suma
    # - PrecioUN: último no-cero; si todos 0/NaN, último (0 si vacío)
    def _agg_precio(series: pd.Series) -> float:
        s = series.dropna()
        nz = s[s != 0]
        return (
            float(nz.iloc[-1])
            if not nz.empty
            else (float(s.iloc[-1]) if not s.empty else 0.0)
        )

    claves = ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Mes"]
    df_largo = df_largo.groupby(claves, as_index=False).agg(
        Cant=("Cant", "sum"), PrecioUN=("PrecioUN", _agg_precio)
    )

    # Tipos finales y atributos calculados
    df_largo["Linea"] = df_largo["OcrCode3"].apply(_ocr3_a_linea)

    df_largo["Mes"] = df_largo["Mes"].astype(str).str.zfill(2)
    df_largo["FechEntr"] = pd.to_datetime(
        f"{int(anio)}-" + df_largo["Mes"] + "-01",
        format="%Y-%m-%d",
        errors="coerce",
    ).dt.date

    df_largo["CardCode"] = cardcode
    df_largo["SlpCode"] = slpcode

    # Normaliza tipos numéricos
    df_largo["Cant"] = pd.to_numeric(df_largo["Cant"], errors="coerce").fillna(0.0)
    df_largo["PrecioUN"] = pd.to_numeric(df_largo["PrecioUN"], errors="coerce").fillna(
        0.0
    )

    # Reglas de negocio simples: negativos no permitidos (puedes relajar si hace falta)
    neg = (df_largo["Cant"] < 0) | (df_largo["PrecioUN"] < 0)
    if neg.any():
        raise ValueError(
            f"[LARGO] Valores negativos detectados en {int(neg.sum())} filas."
        )

    columnas_finales = [
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

    _dbg("[DEBUG-LARGO] Preview final:")
    _dbg(df_largo[columnas_finales].head(5).to_string(index=False))

    # Validación clave única BD
    claves_bd = ["ItemCode", "TipoForecast", "OcrCode3", "Mes", "CardCode"]
    duplicados = df_largo.duplicated(subset=claves_bd, keep=False)
    if duplicados.any():
        _dbg(f"[❌ LARGO-ERROR] {duplicados.sum()} duplicados para clave BD:")
        _dbg(
            df_largo[duplicados][claves_bd + ["Cant", "PrecioUN"]]
            .sort_values(claves_bd)
            .to_string(index=False)
        )
        raise ValueError("Duplicados en df_largo respecto a clave única de detalle.")

    return df_largo[columnas_finales]


def existe_forecast_individual(
    slpcode: int, cardcode: str, anio: int, db_path: str
) -> bool:
    """
    Verifica si existe un forecast individual para un cliente específico.
    Logs compactos [EXISTE-FORECAST.*], sin emojis.
    """
    import time

    t0 = time.perf_counter()

    print(
        f"[EXISTE-FORECAST.INFO] start slpcode={slpcode} cardcode={cardcode} anio={anio}"
    )

    qry = """
        SELECT 1
        FROM Forecast_Detalle
        WHERE SlpCode = ?
          AND CardCode = ?
          AND strftime('%Y', FechEntr) = ?
        LIMIT 1
    """.strip()

    print(f"[EXISTE-FORECAST.QUERY] {qry}")
    print(f"[EXISTE-FORECAST.PARAMS] ({slpcode}, {cardcode!r}, {str(anio)!r})")

    try:
        df = run_query(qry, params=(slpcode, cardcode, str(anio)), db_path=db_path)
    except Exception as e:
        print(f"[EXISTE-FORECAST.ERROR] query_fail err={e.__class__.__name__}: {e}")
        print(
            f"[EXISTE-FORECAST.END] exists=False elapsed={time.perf_counter()-t0:.3f}s"
        )
        return False

    shape = getattr(df, "shape", None)
    empty = (df is None) or getattr(df, "empty", True)
    print(f"[EXISTE-FORECAST.RESULT] shape={shape} empty={empty}")

    exists = not empty
    print(
        f"[EXISTE-FORECAST.END] exists={exists} elapsed={time.perf_counter()-t0:.3f}s"
    )
    return exists


# B_FEN002: Inserción de detalle de forecast a SQL (Forecast_Detalle)
# ∂B_FEN002/∂B1
def insertar_forecast_detalle(
    df_in: pd.DataFrame,
    forecast_id: int,
    anio: int,
    db_path: str = DB_PATH,
    replace: bool = True,
) -> int:
    """
    Inserta detalle de forecast en Forecast_Detalle para un ForecastID dado.

    Formatos soportados:
    1) ANCHO (buffer editor con Métrica):
       cols: ["ItemCode","TipoForecast","Métrica","OcrCode3", "01".."12", ("PrecioUN" opcional), "DocCur", ("Linea" opcional)]
       - Se hace melt a largo (Cant y PrecioUN), preservando 'Linea' si existe.

    2) LARGO:
       cols mínimas: ["ItemCode","TipoForecast","OcrCode3","Mes","Cant"] y opcionales ["PrecioUN","DocCur","Linea"]
       - 'Mes' en "01".."12" (str) o 1..12 (int)
       - 'PrecioUN' faltante => 0.0; 'DocCur' faltante => "CLP"

    Si `replace=True`, elimina el detalle previo de ese ForecastID antes de insertar.
    Devuelve el número de filas preparadas para insertar (aprox. filas insertadas).
    """
    print("[PFD-007] ▶ start insertar_forecast_detalle")
    print(
        f"[PFD-007] args: forecast_id={forecast_id}({type(forecast_id).__name__}), "
        f"anio={anio}, df_in.shape={getattr(df_in,'shape',None)}, db_path={db_path}, replace={replace}"
    )

    # ── Guards
    if not isinstance(df_in, pd.DataFrame):
        raise TypeError("[PFD-007] df_in debe ser DataFrame")
    if not isinstance(forecast_id, (int, np.integer)):
        raise TypeError("[PFD-007] forecast_id debe ser int")
    if not isinstance(anio, (int, np.integer)):
        raise TypeError("[PFD-007] anio debe ser int")

    months = [f"{m:02d}" for m in range(1, 13)]

    # ───────────────── Helper: header Forecast (para SlpCode/CardCode) ─────────────────
    def _leer_header_forecast(_id: int) -> dict:
        try:
            df_h = run_query(
                "SELECT Id, SlpCode, CardCode FROM Forecast WHERE Id = ?",
                db_path,
                (int(_id),),
            )
            if df_h.empty:
                print(
                    f"[PFD-007] ⚠️ ForecastId {forecast_id} no existe en header -> SlpCode/CardCode desconocidos"
                )
                return {}
            row = df_h.iloc[0].to_dict()
            print(
                f"[PFD-007] header Forecast -> SlpCode={row.get('SlpCode')} CardCode={row.get('CardCode')}"
            )
            return row
        except Exception as e:
            print(f"[PFD-007][warn] error leyendo header Forecast: {e}")
            return {}

    header = _leer_header_forecast(forecast_id)
    slp_from_header = header.get("SlpCode")
    card_from_header = header.get("CardCode")

    # ───────────────── Detectar formato ─────────────────
    cols = set(df_in.columns.astype(str))
    is_ancho_editor = ("Métrica" in cols) and (len(set(months) & cols) > 0)
    is_largo = {"ItemCode", "TipoForecast", "OcrCode3", "Mes"}.issubset(cols)
    print(
        f"[PFD-007] formato detectado -> ancho_editor={is_ancho_editor}, largo={is_largo}"
    )
    print(
        f"[PFD-007] Linea en df_in: {'sí' if 'Linea' in cols else 'no'} "
        f"| n_no_nulos={int(df_in['Linea'].notna().sum()) if 'Linea' in cols else 0}"
    )

    if not is_ancho_editor and not is_largo:
        raise ValueError(
            "[PFD-007] df_in no coincide con formatos soportados (ancho con Métrica o largo con Mes)."
        )

    # ───────────────── Normalizar a LARGO con ['Cant','PrecioUN'] y preservar 'Linea' ─────────────────
    if is_ancho_editor:
        df = df_in.copy()
        for c in ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]:
            df[c] = df[c].astype("string")
        if "DocCur" in df.columns:
            df["DocCur"] = df["DocCur"].astype("string")
        # melt Cantidad (agrega Linea si existe)
        id_vars_cant = [
            c
            for c in [
                "ItemCode",
                "TipoForecast",
                "OcrCode3",
                "DocCur",
                "Métrica",
                "Linea",
            ]
            if c in df.columns
        ]
        cant_wide = df[df["Métrica"] == "Cantidad"].copy()
        cant_long = cant_wide.melt(
            id_vars=id_vars_cant,
            value_vars=[m for m in months if m in cant_wide.columns],
            var_name="Mes",
            value_name="Cant",
        ).drop(columns=["Métrica"], errors="ignore")

        # melt Precio (si hay bloque "Precio"); también preserva Linea
        if (df["Métrica"] == "Precio").any():
            id_vars_prec = [
                c
                for c in [
                    "ItemCode",
                    "TipoForecast",
                    "OcrCode3",
                    "DocCur",
                    "Métrica",
                    "Linea",
                ]
                if c in df.columns
            ]
            precio_wide = df[df["Métrica"] == "Precio"].copy()
            precio_long = precio_wide.melt(
                id_vars=id_vars_prec,
                value_vars=[m for m in months if m in precio_wide.columns],
                var_name="Mes",
                value_name="PrecioUN",
            ).drop(columns=["Métrica"], errors="ignore")
        else:
            precio_long = cant_long[
                ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Mes"]
                + (["Linea"] if "Linea" in cant_long.columns else [])
            ].copy()
            precio_long["PrecioUN"] = 0.0

        merge_keys = [
            k
            for k in ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Mes", "Linea"]
            if k in cant_long.columns and k in precio_long.columns
        ]
        df_long = cant_long.merge(precio_long, on=merge_keys, how="left")

    else:
        df_long = df_in.copy()
        for c in ["ItemCode", "TipoForecast", "OcrCode3"]:
            df_long[c] = df_long[c].astype("string")
        if "DocCur" in df_long.columns:
            df_long["DocCur"] = df_long["DocCur"].astype("string")

        if "Cant" not in df_long.columns:
            if "Cant_Esperada" in df_long.columns:
                df_long = df_long.rename(columns={"Cant_Esperada": "Cant"})
            elif "Cantidad" in df_long.columns:
                df_long = df_long.rename(columns={"Cantidad": "Cant"})
            else:
                raise ValueError(
                    "[PFD-007] df_largo debe contener 'Cant'/'Cant_Esperada'/'Cantidad'."
                )

        if "PrecioUN" not in df_long.columns:
            df_long["PrecioUN"] = 0.0

        if "Mes" not in df_long.columns:
            raise ValueError("[PFD-007] df_largo debe contener 'Mes' (01..12).")

    # ───────────────── Limpieza final y defaults ─────────────────
    df_long["Mes"] = df_long["Mes"].astype(str).str.zfill(2)
    df_long["Cant"] = pd.to_numeric(df_long["Cant"], errors="coerce").fillna(0.0)
    df_long["PrecioUN"] = pd.to_numeric(df_long["PrecioUN"], errors="coerce").fillna(
        0.0
    )

    if "SlpCode" not in df_long.columns and slp_from_header is not None:
        df_long["SlpCode"] = int(slp_from_header)
    if "CardCode" not in df_long.columns and card_from_header is not None:
        df_long["CardCode"] = str(card_from_header)
    if "SlpCode" not in df_long.columns or "CardCode" not in df_long.columns:
        raise ValueError(
            "[PFD-007] No se pudo determinar SlpCode/CardCode (agregue al DF o verifique Forecast)."
        )

    if "DocCur" not in df_long.columns:
        df_long["DocCur"] = "CLP"
    df_long["DocCur"] = df_long["DocCur"].astype("string")

    if "OcrCode3" not in df_long.columns:
        df_long["OcrCode3"] = ""
    df_long["OcrCode3"] = df_long["OcrCode3"].astype("string")

    if "ItemCode" not in df_long.columns:
        raise ValueError("[PFD-007] Falta 'ItemCode' en df_long.")
    if "TipoForecast" not in df_long.columns:
        raise ValueError("[PFD-007] Falta 'TipoForecast' en df_long.")

    # Normalización de 'Linea' (si está presente)
    linea_present = "Linea" in df_long.columns
    if linea_present:
        # Intento de castear a entero si aplica; si no, mantener como string
        try:
            linea_num = pd.to_numeric(df_long["Linea"], errors="coerce")
            if (
                linea_num.notna().any()
                and df_long["Linea"]
                .astype(str)
                .str.fullmatch(r"\d+")
                .fillna(False)
                .all()
            ):
                df_long["Linea"] = linea_num.astype("Int64")
            else:
                df_long["Linea"] = df_long["Linea"].astype("string")
        except Exception as e:
            print(f"[PFD-007][warn] normalización de 'Linea' como Int64 falló: {e}")
            df_long["Linea"] = df_long["Linea"].astype("string")

    # Filtrar filas completamente vacías (Cant==0 y PrecioUN==0)
    before_rows = len(df_long)
    df_long = df_long[(df_long["Cant"] != 0) | (df_long["PrecioUN"] != 0)]
    removed = before_rows - len(df_long)
    print(
        f"[PFD-007] filtrado filas vacías -> removidas={removed}, restantes={len(df_long)}"
    )
    if linea_present:
        print(
            f"[PFD-007] 'Linea' no nulos tras filtro -> {int(df_long['Linea'].notna().sum())}"
        )

    if df_long.empty:
        print("[PFD-007] ⚠️ No hay filas para insertar tras filtrado. Nada que hacer.")
        return 0

    # ───────────────── Preflight de tabla y tipos (¿existe columna 'Mes'? ¿'Linea'?) ─────────────────
    with sqlite3.connect(db_path) as con:
        pragma = pd.read_sql_query("PRAGMA table_info(Forecast_Detalle)", con)
        cols_tabla_lower = pragma["name"].str.lower().tolist()
        tipos_tabla = dict(
            zip(pragma["name"].tolist(), pragma["type"].str.upper().fillna("").tolist())
        )
        has_col_mes = "mes" in cols_tabla_lower
        has_col_linea = "linea" in cols_tabla_lower
        print(
            f"[PFD-007] columnas en Forecast_Detalle: {cols_tabla_lower} | has_col_mes={has_col_mes} | has_col_linea={has_col_linea}"
        )

        # Borrado previo
        if replace:
            con.execute(
                "DELETE FROM Forecast_Detalle WHERE ForecastID = ?", (int(forecast_id),)
            )
            print(f"[PFD-007] detalle previo eliminado para ForecastID={forecast_id}")

        # Campos a insertar (dinámicos según existencia de Mes/Linea)
        fields = [
            "ForecastID",
            "SlpCode",
            "CardCode",
            "ItemCode",
            "OcrCode3",
            "FechEntr",
        ]
        if has_col_mes:
            fields.append("Mes")
        fields += ["Cant", "PrecioUN", "DocCur", "TipoForecast"]
        if has_col_linea:
            fields.append("Linea")

        placeholders = "(" + ",".join(["?"] * len(fields)) + ")"
        sql = (
            f"INSERT INTO Forecast_Detalle ({', '.join(fields)}) VALUES {placeholders}"
        )

        # Construir valores por fila
        def _row_tuple(r) -> tuple:
            base = [
                int(forecast_id),
                int(r["SlpCode"]),
                str(r["CardCode"]),
                str(r["ItemCode"]),
                str(r["OcrCode3"]),
                pd.to_datetime(r["FechEntr"]).strftime("%Y-%m-%d"),
            ]
            if has_col_mes:
                base.append(int(str(r["Mes"]).zfill(2)))
            base.extend(
                [
                    float(r["Cant"]),
                    float(r["PrecioUN"]),
                    str(r["DocCur"]),
                    str(r["TipoForecast"]),
                ]
            )
            if has_col_linea:
                val = r.get("Linea", None)
                # Cast según tipo declarado en la tabla
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    base.append(None)
                else:
                    tipo = tipos_tabla.get("Linea", "").upper()
                    if "INT" in tipo:
                        try:
                            base.append(int(val))
                        except Exception:
                            # fallback si viene texto no numérico
                            base.append(None)
                    else:
                        base.append(str(val))
            return tuple(base)

        # Asegurar FechEntr desde anio + Mes
        if "FechEntr" not in df_long.columns or df_long["FechEntr"].isna().any():
            df_long["FechEntr"] = pd.to_datetime(
                df_long["Mes"].radd(f"{int(anio)}-"), format="%Y-%m", errors="coerce"
            )
            df_long.loc[df_long["FechEntr"].isna(), "FechEntr"] = pd.to_datetime(
                f"{int(anio)}-01-01"
            )

        rows = [_row_tuple(r) for _, r in df_long.iterrows()]
        con.executemany(sql, rows)
        con.commit()
        inserted = con.total_changes

    print(
        f"[PFD-007] ✅ insert completado | filas_insertadas (aprox)={inserted} | rows_preparadas={len(rows)}"
    )
    # Vista de control (incluye 'Linea' si aplica)
    try:
        preview_cols = [
            "ItemCode",
            "TipoForecast",
            "OcrCode3",
            "Mes",
            "FechEntr",
            "Cant",
            "PrecioUN",
            "DocCur",
        ]
        if linea_present:
            preview_cols.append("Linea")
        preview_cols = [c for c in preview_cols if c in df_long.columns]
        with pd.option_context("display.max_columns", None):
            print("[PFD-007] preview a insertar (primeras 5):")
            print(df_long[preview_cols].head().to_string(index=False))
    except Exception as e:
        print(f"[PFD-007] preview error: {e}")

    print("[PFD-007] ⛳ done insertar_forecast_detalle")
    return len(rows)


# B_FEN004: Inserción de cabecera Forecast (SlpCode + Fecha_Carga)
# ∂B_FEN004/∂B1
def registrar_forecast_cabecera(
    slpcode: int,
    db_path: str | None = None,
) -> int:
    ahora = datetime.now().isoformat(sep=" ", timespec="seconds")
    sql = "INSERT INTO Forecast (SlpCode, Fecha_Carga) VALUES (?, ?)"
    return _run_forecast_insert_get_id(sql, (slpcode, ahora))


# B_HELP001: obtiene o crea ForecastID reutilizable
def obtener_forecast_activo(
    slpcode: int,
    cardcode: str,
    anio: int,
    db_path: str = DB_PATH,
    *,
    force_new: bool = False,
) -> int:
    """
    Devuelve un ForecastID único por cliente y día.
    No consulta la tabla Forecast; se basa en session_state.
    Logs compactos, sin emojis.
    """
    import time

    t0 = time.perf_counter()
    print("[FORECAST-ACTIVO-START] obtener_forecast_activo")
    print(
        f"[FORECAST-ACTIVO-INFO] slpcode={slpcode} cardcode={cardcode} anio={anio} force_new={force_new}"
    )

    llave = f"forecast_activo_{slpcode}_{cardcode}_{anio}"
    print(f"[FORECAST-ACTIVO-INFO] session_key={llave}")

    # Cache: reutiliza si no se fuerza uno nuevo
    if not force_new and llave in st.session_state:
        forecast_id = st.session_state[llave]
        activos = sum(k.startswith("forecast_activo_") for k in st.session_state.keys())
        print(
            f"[FORECAST-ACTIVO.CACHE] hit id={forecast_id} active_keys={activos} elapsed={time.perf_counter()-t0:.3f}s"
        )
        return forecast_id

    # Crear uno nuevo
    print("[FORECAST-ACTIVO-NEW] creating_new (cache_miss or force_new=True)")
    t1 = time.perf_counter()
    forecast_id = registrar_forecast_cabecera(slpcode, db_path)
    print(
        f"[FORECAST-ACTIVO-REGISTER] id={forecast_id} db_elapsed={time.perf_counter()-t1:.3f}s"
    )

    st.session_state[llave] = forecast_id
    activos = sum(k.startswith("forecast_activo_") for k in st.session_state.keys())
    print(
        f"[FORECAST-ACTIVO-SAVE] cached key={llave} id={forecast_id} active_keys={activos}"
    )

    print(
        f"[FORECAST-ACTIVO-END] id={forecast_id} elapsed={time.perf_counter()-t0:.3f}s"
    )
    return forecast_id


# B_SYN003: Guardado estructurado y seguro de buffers editados de todos los clientes
# # ∂B_SYN003/∂B0
def guardar_todos_los_clientes_editados(anio: int, db_path: str = DB_PATH):
    """
    Cambios principales:
    - (✔) Cada guardado usa un ForecastID NUEVO (force_new=True) y se limpia el cache del ID activo post-guardado.
    - (✔) Se crea la cabecera SOLO si hay cambios reales (evita cabeceras huérfanas).
    - (✔) Resets y hashing intactos; recuperación post-error solo si hubo ForecastID.
    - (✔) Logs más explícitos y defensas adicionales.
    """

    t0 = time.perf_counter()
    print("[SAVE.INFO] start")
    print(
        f"[SAVE.INFO] clientes_editados={st.session_state.get('clientes_editados', set())}"
    )

    clientes = st.session_state.get("clientes_editados", set()).copy()
    print(f"[SAVE.INFO] to_process={sorted(clientes)}")
    if not clientes:
        print(f"[SAVE.INFO] no_changes elapsed={time.perf_counter()-t0:.3f}s")
        st.info("✅ No hay cambios pendientes por guardar")
        return

    # Helper local para la clave de cache del forecast activo
    def _forecast_activo_cache_key(slpcode: int, cardcode: str, anio: int) -> str:
        return f"forecast_activo_{slpcode}_{cardcode}_{anio}"

    for cliente in clientes:
        t_cli = time.perf_counter()
        key_buffer = f"forecast_buffer_{cliente}"
        forecast_id = None  # ← Definido temprano para manejo en try/except

        # Estado previo (resumen, sin volcar dataframes gigantes)
        buf_val = st.session_state.get(key_buffer, None)
        if buf_val is None:
            print(f"[SAVE.WARN] buffer_missing cliente={cliente} key={key_buffer}")
            st.warning(f"⚠️ No se encontró buffer para cliente {cliente}.")
            continue
        else:
            try:
                if hasattr(buf_val, "shape"):
                    print(
                        f"[SAVE.INFO] processing cliente={cliente} key={key_buffer} pre.shape={buf_val.shape}"
                    )
                else:
                    print(
                        f"[SAVE.INFO] processing cliente={cliente} key={key_buffer} pre.type={type(buf_val).__name__}"
                    )
            except Exception:
                print(
                    f"[SAVE.INFO] processing cliente={cliente} key={key_buffer} pre=uninspectable"
                )

        try:

            autocompletar_precios_en_buffer(cliente)  # ← INSERTA ESTA LÍNEA AQUÍ

            df_base = st.session_state[key_buffer].reset_index()
            slpcode = int(obtener_slpcode())

            print(
                f"[SAVE.STEP] 1/9 df_base rows={len(df_base)} cols={len(df_base.columns)}"
            )
            try:
                print(df_base.head(3).to_string(index=False))
            except Exception as e_head:
                print(f"[SAVE.WARN] df_base_preview_unavailable err={e_head}")

            if df_base.empty:
                print(f"[SAVE.INFO] df_base_empty skip cliente={cliente}")
                continue

            # 1) Transformación a largo (usa tu función probada)
            df_largo = df_forecast_metrico_to_largo(df_base, anio, cliente, slpcode)
            print(f"[SAVE.STEP] 2/9 df_largo rows={len(df_largo)}")
            try:
                print(
                    df_largo[["ItemCode", "TipoForecast", "Mes", "Cant"]]
                    .head(8)
                    .to_string(index=False)
                )
            except Exception as e_head:
                print(f"[SAVE.WARN] df_largo_preview_unavailable err={e_head}")

            if df_largo.empty:
                print(f"[SAVE.INFO] df_largo_empty skip cliente={cliente}")
                st.info(f"ℹ️ Sin datos para guardar en cliente {cliente}.")
                continue

            # 2) Buscar histórico previo (ANTES de crear nueva cabecera)
            forecast_id_prev = _get_forecast_id_prev(slpcode, cliente, anio, db_path)
            print(f"[SAVE.STEP] 3/9 forecast_prev_id={forecast_id_prev}")

            # 3) Enriquecer + filtrar contra histórico
            modo_individual = existe_forecast_individual(
                slpcode, cliente, anio, db_path
            )
            df_largo_filtrado = _enriquecer_y_filtrar(
                df_largo,
                forecast_id_prev,
                slpcode,
                cliente,
                anio,
                db_path,
                incluir_deltas_cero_si_es_individual=modo_individual,
                forzar_incluir_todos=(forecast_id_prev is None),
            )
            print(f"[SAVE.STEP] 4/9 cambios_reales={len(df_largo_filtrado)}")

            if df_largo_filtrado.empty:
                print(f"[SAVE.INFO] no_real_changes skip cliente={cliente}")
                st.info(
                    f"⏩ Cliente {cliente}: sin cambios reales. Se omite inserción."
                )
                _reset_estado_edicion_por_cliente(cliente, key_buffer)
                continue

            # 4) Crear SIEMPRE nueva cabecera SOLO cuando hay cambios
            forecast_id = obtener_forecast_activo(
                slpcode, cliente, anio, db_path, force_new=False
            )
            print(
                f"[SAVE.STEP] 5/9 forecast_new_id={forecast_id} prev_id={forecast_id_prev}"
            )

            # 5) Logging delta (antes de insertar)
            print("[SAVE.STEP] 6/9 registrar_log_detalle_cambios")
            registrar_log_detalle_cambios(
                slpcode,
                cliente,
                anio,
                df_largo_filtrado.copy(),
                db_path,
                forecast_id=forecast_id,
                forecast_id_anterior=forecast_id_prev,
            )

            # 6) Inserción / UPSERT
            print("[SAVE.STEP] 7/9 insertar_forecast_detalle")
            print(
                f"[SAVE.INSERT.INFO] prep forecast_id={forecast_id} shape={df_largo_filtrado.shape}"
            )
            print("[SAVE.INSERT.INFO] dups_check preview:")
            try:
                print(
                    df_largo_filtrado.groupby(["ItemCode", "TipoForecast", "Mes"])
                    .size()
                    .reset_index(name="count")
                    .to_string(index=False)
                )
            except Exception as e_dups:
                print(f"[SAVE.WARN] dups_preview_unavailable err={e_dups}")

            insertar_forecast_detalle(
                df_largo_filtrado.assign(ForecastID=forecast_id),
                forecast_id,
                anio,
                db_path,
            )
            print("[SAVE.INSERT.INFO] done")
            print(f"[SAVE.DEBUG] session_state_keys={len(st.session_state.keys())}")

            # 7) Refrescar SIEMPRE el buffer UI desde BD (4×12 garantizado)
            print("[SAVE.STEP] 8/9 refresh_buffer_ui")
            _refrescar_buffer_ui(forecast_id, key_buffer, db_path)

            # Verificación 4×12
            try:
                df_ui = st.session_state[key_buffer].reset_index()
                base = df_ui[["ItemCode", "OcrCode3", "DocCur"]].drop_duplicates()
                expected = len(base) * 4
                real = len(df_ui)
                print(
                    f"[SAVE.INFO] verify_4x12 bases={len(base)} expected={expected} real={real}"
                )
                if real != expected:
                    print(
                        "[SAVE.WARN] ui_rows_not_multiple_of_4 review _refrescar_buffer_ui"
                    )
            except Exception as e_check:
                print(f"[SAVE.WARN] verify_4x12_failed err={e_check}")

            # 8) RESET del editor y marcas de edición
            print("[SAVE.STEP] 9/9 reset_ui_edit_state")
            _reset_estado_edicion_por_cliente(cliente, key_buffer)

            # Limpieza cache ForecastID activo
            cache_key = _forecast_activo_cache_key(slpcode, cliente, anio)
            if cache_key in st.session_state:
                print(
                    f"[SAVE.INFO] clear_active_forecast_cache key={cache_key} value={st.session_state[cache_key]}"
                )
                del st.session_state[cache_key]

            # 9) Recalcular y guardar hash del buffer actual (anti-parpadeo)
            try:
                df_for_hash = st.session_state[key_buffer].reset_index()
                h = pd.util.hash_pandas_object(df_for_hash, index=False).sum()
                st.session_state[f"{key_buffer}_hash"] = np.uint64(h & ((1 << 64) - 1))
                print(
                    f"[SAVE.INFO] buffer_hash_updated key={key_buffer} hash={st.session_state[f'{key_buffer}_hash']}"
                )
            except Exception as e_hash:
                print(f"[SAVE.WARN] buffer_hash_failed key={key_buffer} err={e_hash}")

            st.success(
                f"✅ Cliente {cliente} guardado correctamente (ForecastID={forecast_id})."
            )
            print(
                f"[SAVE.INFO] cliente_done cliente={cliente} forecast_id={forecast_id} elapsed={time.perf_counter()-t_cli:.3f}s"
            )

        except Exception as e:
            cls = e.__class__.__name__
            print(f"[SAVE.ERROR] cliente={cliente} exc={cls} msg={e}")
            st.error(f"❌ Error al guardar cliente {cliente}: {e}")

            # Recuperación visual mínima SOLO si hubo ForecastID (evita query con None)
            try:
                if forecast_id is None:
                    print("[SAVE.INFO] recovery_skip_no_forecast_id")
                    continue

                print("[SAVE.INFO] recovery_try_refresh_from_db")
                qry_ultimo = """
                    SELECT ItemCode, TipoForecast, OcrCode3, Linea, DocCur, Mes, 
                           SUM(Cant)     AS Cant, 
                           MAX(PrecioUN) AS PrecioUN
                    FROM (
                        SELECT 
                            ItemCode,
                            TipoForecast,
                            OcrCode3,
                            Linea,
                            DocCur,
                            CAST(strftime('%m', FechEntr) AS TEXT) AS Mes,
                            Cant,
                            PrecioUN
                        FROM Forecast_Detalle
                        WHERE ForecastID = ?
                    ) AS t
                    GROUP BY ItemCode, TipoForecast, OcrCode3, Linea, DocCur, Mes;
                """
                df_post = run_query(qry_ultimo, params=(forecast_id,), db_path=db_path)
                if not df_post.empty:
                    print(f"[SAVE.INFO] recovery_ok df_post.shape={df_post.shape}")
                    cols_meses = [f"{m:02d}" for m in range(1, 13)]
                    df_cant, df_prec = df_post.copy(), df_post.copy()
                    df_cant["Métrica"] = "Cantidad"
                    df_prec["Métrica"] = "Precio"
                    pivot_cant = df_cant.pivot_table(
                        index=[
                            "ItemCode",
                            "TipoForecast",
                            "OcrCode3",
                            "Linea",
                            "DocCur",
                            "Métrica",
                        ],
                        columns="Mes",
                        values="Cant",
                        aggfunc="first",
                    )
                    pivot_prec = df_prec.pivot_table(
                        index=[
                            "ItemCode",
                            "TipoForecast",
                            "OcrCode3",
                            "Linea",
                            "DocCur",
                            "Métrica",
                        ],
                        columns="Mes",
                        values="PrecioUN",
                        aggfunc="first",
                    )
                    df_metrico = (
                        pd.concat([pivot_cant, pivot_prec]).reset_index().fillna(0)
                    )
                    for mes in cols_meses:
                        if mes not in df_metrico.columns:
                            df_metrico[mes] = 0
                    cols_fijas = [c for c in df_metrico.columns if c not in cols_meses]
                    df_metrico = df_metrico[cols_fijas + cols_meses]
                    st.session_state[key_buffer] = df_metrico.set_index(
                        ["ItemCode", "TipoForecast", "Métrica"]
                    )
                    st.dataframe(df_metrico)
                    st.success(
                        f"✅ Cliente {cliente} guardado correctamente (ForecastID={forecast_id})."
                    )
                    _reset_estado_edicion_por_cliente(cliente, key_buffer)
            except Exception as e2:
                cls2 = e2.__class__.__name__
                print(
                    f"[SAVE.ERROR] recovery_failed cliente={cliente} exc={cls2} msg={e2}"
                )
                st.error(
                    f"❌ Error crítico al intentar refrescar buffer de cliente {cliente}: {e2}"
                )

    print("[SAVE.INFO] cleanup_clientes_editados")
    st.session_state.pop("clientes_editados", None)
    print(f"[SAVE.INFO] end elapsed={time.perf_counter()-t0:.3f}s")


def _get_forecast_id_prev(
    slpcode: int, cardcode: str, anio: int, db_path: str
) -> Optional[int]:
    """
    Devuelve el ForecastID inmediatamente anterior (por Fecha_Carga) para
    (SlpCode, CardCode, año). Si no hay por cliente, intenta por SlpCode (fallback).
    """
    import time
    from datetime import datetime

    logp = "[FORECAST.PREV]"
    print(
        f"{logp} start slpcode={slpcode} cardcode={cardcode} anio={anio} db={db_path}"
    )

    # Ventana anual sargable
    y0 = f"{anio}-01-01"
    y1 = f"{anio + 1}-01-01"
    now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{logp} window y0={y0} y1={y1} now={now_ts}")

    # 1) Cliente + año (ordenado por Fecha_Carga real)
    qry_cli = """
        SELECT f.ForecastID, f.Fecha_Carga
        FROM Forecast_Detalle fd
        JOIN Forecast f ON f.ForecastID = fd.ForecastID
        WHERE fd.SlpCode = ?
          AND fd.CardCode = ?
          AND fd.FechEntr >= ?
          AND fd.FechEntr <  ?
          AND f.Fecha_Carga < ?
        ORDER BY f.Fecha_Carga DESC, f.ForecastID DESC
        LIMIT 1
    """
    t0 = time.perf_counter()
    df_cli = run_query(
        qry_cli, params=(slpcode, cardcode, y0, y1, now_ts), db_path=db_path
    )
    t1 = time.perf_counter()
    shape_cli = None if df_cli is None else df_cli.shape
    print(f"{logp} q.client shape={shape_cli} elapsed={t1 - t0:0.3f}s")

    if df_cli is not None and not df_cli.empty:
        fid = int(df_cli.iloc[0]["ForecastID"])
        ts = str(df_cli.iloc[0]["Fecha_Carga"])
        print(f"{logp} found client ForecastID={fid} fecha_carga={ts}")
        return fid

    print(f"{logp} no client record, trying fallback by slpcode")

    # 2) Fallback: SlpCode + año (sin CardCode)
    qry_slp = """
        SELECT f.ForecastID, f.Fecha_Carga
        FROM Forecast_Detalle fd
        JOIN Forecast f ON f.ForecastID = fd.ForecastID
        WHERE fd.SlpCode = ?
          AND fd.FechEntr >= ?
          AND fd.FechEntr <  ?
          AND f.Fecha_Carga < ?
        ORDER BY f.Fecha_Carga DESC, f.ForecastID DESC
        LIMIT 1
    """
    t2 = time.perf_counter()
    df_slp = run_query(qry_slp, params=(slpcode, y0, y1, now_ts), db_path=db_path)
    t3 = time.perf_counter()
    shape_slp = None if df_slp is None else df_slp.shape
    print(f"{logp} q.fallback shape={shape_slp} elapsed={t3 - t2:0.3f}s")

    if df_slp is not None and not df_slp.empty:
        fid = int(df_slp.iloc[0]["ForecastID"])
        ts = str(df_slp.iloc[0]["Fecha_Carga"])
        print(f"{logp} found fallback ForecastID={fid} fecha_carga={ts}")
        return fid

    print(f"{logp} not_found (client nor fallback); will start from scratch")
    return None


def validate_delta_schema(
    df: pd.DataFrame, *, contexto: str = "[VALIDACIÓN DELTA]"
) -> None:
    """
    Valida que el DataFrame cumpla con el contrato `delta_schema_v3`, que estructura los cambios
    entre forecast actual y anterior. Este chequeo es obligatorio antes de insertar en logs o BD.

    ▸ Levanta ValueError si el esquema es inválido.
    ▸ El DataFrame debe contener exactamente las columnas esperadas.
    ▸ Debe integrarse en `_enriquecer_y_filtrar()` y `registrar_log_detalle_cambios()`.
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


def _enriquecer_y_filtrar(
    df_largo: pd.DataFrame,
    forecast_id_prev: Optional[int],
    slpcode: int,
    cardcode: str,
    anio: int,
    db_path: str,
    resolver_duplicados: str = "mean",  # "mean" | "sum" | "error"
    incluir_deltas_cero_si_es_individual: bool = False,  # DEPRECADO: se ignora
    forzar_incluir_todos: bool = False,
) -> pd.DataFrame:
    """
    Añade Cant_Anterior y devuelve SOLO filas a persistir siguiendo reglas idempotentes:
      (A) Δ != 0                                -> cambios reales
      (B) Cant == 0  y Cant_Anterior > 0        -> BAJA (>0→0)
      (C) Cant > 0   y Cant_Anterior == 0       -> ALTA (0→>0)

    Notas:
    - `incluir_deltas_cero_si_es_individual` está DEPRECADO (no se usa).
    - Normaliza Mes a '01'..'12', Cant a numérico.
    - Si hay duplicados en histórico, se resuelven según `resolver_duplicados`.
    - Propaga 'CardCode' si viene en df_largo (para validar unicidad).
    """
    import time

    LOG = "[DEBUG-FILTRO]"
    t0 = time.perf_counter()

    # 0) Validaciones mínimas + normalización de entrada
    req_cols = {"ItemCode", "TipoForecast", "OcrCode3", "Mes", "Cant"}
    missing = req_cols - set(df_largo.columns)
    if missing:
        print(f"{LOG} ❌ Faltan columnas requeridas en df_largo: {sorted(missing)}")
        raise ValueError(f"df_largo carece de columnas requeridas: {sorted(missing)}")

    if df_largo.empty:
        print(f"{LOG} ⚠️ df_largo vacío → no hay nada que enriquecer/filtrar")
        return df_largo.copy()

    df_largo = df_largo.copy()
    # Mes como '01'..'12'
    df_largo["Mes"] = df_largo["Mes"].astype(str).str.zfill(2)
    # Cant numérico
    df_largo["Cant"] = pd.to_numeric(df_largo["Cant"], errors="coerce").fillna(0.0)
    neg_in = int((df_largo["Cant"] < 0).sum())
    if neg_in:
        print(f"{LOG} ⚠️ Se detectaron {neg_in} valores negativos en 'Cant' (entrada)")

    print(
        f"{LOG} ▶ Enriqueciendo cliente={cardcode} slpcode={slpcode} anio={anio} "
        f"forecast_id_prev={forecast_id_prev} resolver_duplicados={resolver_duplicados} "
        f"forzar_incluir_todos={forzar_incluir_todos}"
    )
    print(
        f"{LOG} df_largo shape={df_largo.shape} uniques ItemCode={df_largo['ItemCode'].nunique()} TipoForecast={df_largo['TipoForecast'].nunique()}"
    )

    # 1) Recuperar histórico (Cant_Anterior)
    t1 = time.perf_counter()
    if forecast_id_prev is None:
        print(f"{LOG} info: sin histórico previo → Cant_Anterior = 0")
        df_prev = df_largo[["ItemCode", "TipoForecast", "OcrCode3", "Mes"]].copy()
        df_prev["Cant_Anterior"] = 0.0
    else:
        print(f"{LOG} usando ForecastID previo: {forecast_id_prev}")
        qry_prev = """
            SELECT ItemCode, TipoForecast, OcrCode3,
                   CAST(strftime('%m', FechEntr) AS TEXT) AS Mes,
                   Cant AS Cant_Anterior
            FROM   Forecast_Detalle
            WHERE  ForecastID = ?
        """
        df_prev = run_query(qry_prev, params=(forecast_id_prev,), db_path=db_path)
        if df_prev is None or df_prev.empty:
            print(
                f"{LOG} ⚠️ Histórico vacío para ForecastID={forecast_id_prev} → Cant_Anterior=0"
            )
            df_prev = df_largo[["ItemCode", "TipoForecast", "OcrCode3", "Mes"]].copy()
            df_prev["Cant_Anterior"] = 0.0
        else:
            df_prev["Mes"] = df_prev["Mes"].astype(str).str.zfill(2)
            df_prev["Cant_Anterior"] = pd.to_numeric(
                df_prev["Cant_Anterior"], errors="coerce"
            ).fillna(0.0)
            print(f"{LOG} histórico recuperado rows={len(df_prev)}")

            # Resolver duplicados por clave de negocio
            claves = ["ItemCode", "TipoForecast", "OcrCode3", "Mes"]
            dup_mask = df_prev.duplicated(subset=claves, keep=False)
            dup_count = int(dup_mask.sum())
            if dup_count:
                print(
                    f"{LOG} ⚠️ Duplicados en histórico por clave {claves}: rows={dup_count}"
                )
                if resolver_duplicados == "error":
                    raise ValueError(
                        "Duplicados en histórico y resolver_duplicados='error'"
                    )
                elif resolver_duplicados in {"mean", "sum"}:
                    print(
                        f"{LOG} resolviendo duplicados con agg='{resolver_duplicados}'"
                    )
                    df_prev = df_prev.groupby(claves, as_index=False).agg(
                        {"Cant_Anterior": resolver_duplicados}
                    )
                else:
                    raise ValueError(
                        f"resolver_duplicados inválido: {resolver_duplicados}"
                    )
    t2 = time.perf_counter()
    print(f"{LOG} histórico.elapsed={t2 - t1:0.3f}s")

    # 2) Merge + diagnóstico
    t3 = time.perf_counter()
    claves_merge = ["ItemCode", "TipoForecast", "OcrCode3", "Mes"]
    df_enr = df_largo.merge(df_prev, on=claves_merge, how="left")
    df_enr["Cant_Anterior"] = df_enr["Cant_Anterior"].fillna(0.0)

    # Propagar CardCode si venía
    if "CardCode" in df_largo.columns and "CardCode" not in df_enr.columns:
        df_enr["CardCode"] = df_largo["CardCode"].values

    df_enr["Delta"] = df_enr["Cant"] - df_enr["Cant_Anterior"]

    print(f"{LOG} diagnóstico previo al filtro — rows={len(df_enr)}")
    # Preview acotado para evitar spam
    try:
        prev_cols = [
            "ItemCode",
            "TipoForecast",
            "OcrCode3",
            "Mes",
            "Cant_Anterior",
            "Cant",
            "Delta",
        ]
        print(
            df_enr[prev_cols]
            .sort_values(["TipoForecast", "Mes"])
            .head(24)
            .to_string(index=False)
        )
        if len(df_enr) > 24:
            print(f"{LOG} (preview truncado a 24 de {len(df_enr)} filas)")
    except Exception as _e:
        print(f"{LOG} (no se pudo imprimir preview) err={_e!r}")
    t4 = time.perf_counter()
    print(f"{LOG} merge+diag.elapsed={t4 - t3:0.3f}s")

    # Resumen de Δ=0
    zeros = df_enr["Delta"].eq(0).sum()
    if zeros:
        print(f"{LOG} registros Δ=0: {int(zeros)}")

    # 3) Reglas A/B/C + métricas
    t5 = time.perf_counter()
    # Calcular siempre las máscaras para métricas
    bajas_mask = (df_enr["Cant_Anterior"] > 0) & (df_enr["Cant"] == 0)
    altas_mask = (df_enr["Cant_Anterior"] == 0) & (df_enr["Cant"] > 0)
    cambios_mask = (
        (df_enr["Cant_Anterior"] > 0) & (df_enr["Cant"] > 0) & (df_enr["Delta"] != 0)
    )

    bajas_cnt, altas_cnt, cambios_cnt = (
        int(bajas_mask.sum()),
        int(altas_mask.sum()),
        int(cambios_mask.sum()),
    )
    print(f"{LOG} zero_transitions_applied (>0→0): {bajas_cnt}")
    print(f"{LOG} altas (0→>0): {altas_cnt}")
    print(f"{LOG} otros cambios (>0→>0 y Δ≠0): {cambios_cnt}")

    if incluir_deltas_cero_si_es_individual:
        print(
            f"{LOG} info: 'incluir_deltas_cero_si_es_individual' está DEPRECADO y se ignora (reglas A/B/C)."
        )

    if forzar_incluir_todos:
        df_out = df_enr.copy()
        print(
            f"{LOG} modo forzado=ON → se incluyen todos los registros (métricas arriba informativas)"
        )
    else:
        df_out = df_enr[bajas_mask | altas_mask | cambios_mask].copy()

    print(f"{LOG} registros con cambio real a persistir: {len(df_out)}")
    if not df_out.empty:
        try:
            print(
                df_out[["ItemCode", "TipoForecast", "Mes", "Cant_Anterior", "Cant"]]
                .head(10)
                .to_string(index=False)
            )
            if len(df_out) > 10:
                print(f"{LOG} (preview cambios truncado a 10 de {len(df_out)})")
        except Exception as _e:
            print(f"{LOG} (no se pudo imprimir preview de cambios) err={_e!r}")

        delta_total = float(df_out["Delta"].sum())
        print(f"{LOG} variación total (sum Δ): {delta_total:,.2f}")

        try:
            resumen_mes = df_out.groupby("Mes", as_index=False)["Delta"].sum()
            print(f"{LOG} resumen Δ por mes:\n{resumen_mes.to_string(index=False)}")
        except Exception as _e:
            print(f"{LOG} (no se pudo calcular resumen por mes) err={_e!r}")
    t6 = time.perf_counter()
    print(f"{LOG} reglas.elapsed={t6 - t5:0.3f}s")

    # 4) Validación de esquema de delta (B2)
    t7 = time.perf_counter()
    df_val = df_out.rename(
        columns={"Cant_Anterior": "CantidadAnterior", "Cant": "CantidadNueva"}
    )
    validate_delta_schema(df_val, contexto="[VALIDACIÓN ENRIQUECER]")
    t8 = time.perf_counter()
    print(f"{LOG} validate_schema.elapsed={t8 - t7:0.3f}s")

    # 5) Validación de unicidad post-enriquecido (si CardCode disponible)
    t9 = time.perf_counter()
    claves_bd = ["ItemCode", "TipoForecast", "OcrCode3", "Mes", "CardCode"]
    if "CardCode" in df_out.columns:
        dup_out = df_out.duplicated(subset=claves_bd, keep=False)
        dup_cnt = int(dup_out.sum())
        if dup_cnt:
            print(
                f"{LOG} ❌ duplicados post-enriquecimiento rows={dup_cnt} en claves {claves_bd}"
            )
            try:
                print(
                    df_out[dup_out][claves_bd + ["Cant", "Cant_Anterior"]]
                    .sort_values(claves_bd)
                    .head(30)
                    .to_string(index=False)
                )
                if dup_cnt > 30:
                    print(f"{LOG} (preview duplicados truncado a 30 de {dup_cnt})")
            except Exception as _e:
                print(f"{LOG} (no se pudo imprimir duplicados) err={_e!r}")
            raise ValueError(
                "df_out contiene claves duplicadas que violan la restricción única de Forecast_Detalle."
            )
    else:
        print(
            f"{LOG} ⚠️ 'CardCode' no presente en df_out — se omite validación de unicidad por clave completa"
        )
    t10 = time.perf_counter()
    print(f"{LOG} uniq-check.elapsed={t10 - t9:0.3f}s")

    print(
        f"{LOG} end total.elapsed={time.perf_counter() - t0:0.3f}s out.shape={df_out.shape}"
    )
    return df_out


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
        df_prev = run_query(qry, params=(forecast_id_anterior,), db_path=db_path)
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


# ————————————————————————————————————————————————————————————
# Helper local: Reset de estado de edición por cliente (A)
def _reset_estado_edicion_por_cliente(cliente: str, key_buffer: str):
    """
    Limpia el editor y las marcas de edición asociadas a un cliente.
    - Borra editor_forecast_{cliente}
    - Borra {key_buffer}_editado (copia temporal)
    - Setea __buffer_editado__ = False
    - Saca al cliente del set 'clientes_editados' (si existiera)
    """
    try:
        editor_key = f"editor_forecast_{cliente}"
        edit_copy_key = f"{key_buffer}_editado"
        print(
            f"[DEBUG-RESET] Limpiando estado de edición → editor_key={editor_key}, edit_copy_key={edit_copy_key}"
        )

        if editor_key in st.session_state:
            prev = st.session_state[editor_key]
            st.session_state.pop(editor_key, None)
            print(f"[DEBUG-RESET] editor_key eliminado. Valor previo: {prev}")

        if edit_copy_key in st.session_state:
            st.session_state.pop(edit_copy_key, None)
            print(f"[DEBUG-RESET] {edit_copy_key} eliminado.")

        st.session_state["__buffer_editado__"] = False
        print(
            f"[DEBUG-RESET] __buffer_editado__ = {st.session_state.get('__buffer_editado__')}"
        )

        # Mantener consistencia inmediata del set (aunque luego se limpia globalmente)
        if (
            "clientes_editados" in st.session_state
            and cliente in st.session_state["clientes_editados"]
        ):
            st.session_state["clientes_editados"].discard(cliente)
            print(
                f"[DEBUG-RESET] '{cliente}' removido de clientes_editados (restan: {st.session_state['clientes_editados']})"
            )

    except Exception as e_reset:
        print(
            f"[DEBUG-RESET] ⚠️ No se pudo limpiar por completo el estado de edición: {e_reset}"
        )


def _refrescar_buffer_ui(
    forecast_id: int, key_buffer: str, db_path: str = DB_PATH
) -> None:
    """
    Reconstruye el buffer de edición (Cantidad/Precio) para un ForecastID dado y lo
    guarda en st.session_state[key_buffer] con índice ["ItemCode","TipoForecast","Métrica"].

    - Cantidad: SUM(Cant) por mes (01..12).
    - Precio:   replica el promedio PrecioUN por clave (ItemCode, TipoForecast, OcrCode3)
                en todos los meses.
    - DocCur:   primer valor por clave (se preserva como columna).
    - Linea:    si existía en el buffer anterior, se intenta preservar por (ItemCode, OcrCode3).
    """
    print("[RFE-005] ▶ start _refrescar_buffer_ui")
    print(
        f"[RFE-005] args: forecast_id={forecast_id}({type(forecast_id).__name__}), "
        f"key_buffer={key_buffer}, db_path={db_path}"
    )

    # ── Guards
    if not isinstance(forecast_id, (int, np.integer)):
        raise TypeError("[RFE-005] forecast_id debe ser int")
    if not isinstance(key_buffer, str) or not key_buffer:
        raise TypeError("[RFE-005] key_buffer debe ser str no vacío")

    # ── Cargar buffer previo (para preservar 'Linea' si existía)
    prev_linea_map = None
    if key_buffer in st.session_state:
        try:
            prev_df = st.session_state[key_buffer].reset_index().copy()
            if "Linea" in prev_df.columns:
                prev_linea_map = prev_df.drop_duplicates(["ItemCode", "OcrCode3"])[
                    ["ItemCode", "OcrCode3", "Linea"]
                ]
                print(
                    f"[RFE-005] preservando 'Linea' (registros únicos map={len(prev_linea_map)})"
                )
        except Exception as e:
            print(f"[RFE-005][warn] no se pudo leer buffer previo para 'Linea': {e}")

    # ── Consulta base por ForecastID
    query = """
        SELECT
            fd.ItemCode,
            i.ItemName,
            fd.TipoForecast,
            fd.OcrCode3,
            CAST(strftime('%m', fd.FechEntr) AS INTEGER) AS Mes,
            SUM(fd.Cant)     AS Cantidad,
            AVG(fd.PrecioUN) AS PrecioUN,
            MAX(fd.DocCur)   AS DocCur
        FROM Forecast_Detalle fd
        LEFT JOIN OITM i
          ON i.ItemCode = fd.ItemCode
        WHERE fd.ForecastID = ?
        GROUP BY
            fd.ItemCode, i.ItemName, fd.TipoForecast, fd.OcrCode3, Mes
        ORDER BY
            fd.ItemCode, fd.TipoForecast, Mes
    """.strip()

    df = run_query(query, db_path, (int(forecast_id),))
    print(f"[RFE-005] raw result shape: {df.shape}")

    # ── Estructura vacía segura si no hay filas
    months = [f"{m:02d}" for m in range(1, 13)]
    if df.empty:
        print("[RFE-005] ⚠️ sin filas para ForecastID -> buffer vacío pero consistente")
        cols = [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            *months,
            "PrecioUN",
            "DocCur",
        ]
        empty = pd.DataFrame({c: pd.Series(dtype="float64") for c in cols})
        for c in [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            "DocCur",
        ]:
            empty[c] = empty[c].astype("string")
        st.session_state[key_buffer] = empty.set_index(
            ["ItemCode", "TipoForecast", "Métrica"]
        ).sort_index()
        print("[RFE-005] ✅ done _refrescar_buffer_ui (empty-safe)")
        return

    # ── Normalizaciones
    df["Mes"] = pd.to_numeric(df["Mes"], errors="coerce").clip(1, 12).astype("Int64")
    df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0.0)
    df["PrecioUN"] = pd.to_numeric(df["PrecioUN"], errors="coerce").fillna(0.0)
    for c in ["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur"]:
        df[c] = df[c].astype("string")

    # ── PIVOT Cantidad 01..12
    piv_cant = df.pivot_table(
        index=["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur"],
        columns="Mes",
        values="Cantidad",
        aggfunc="sum",
        dropna=False,
        fill_value=0.0,
    ).reset_index()
    # renombrar columnas de meses a "01".."12" y crear faltantes
    piv_cant.columns = [
        f"{c:02d}" if isinstance(c, (int, np.integer)) else c for c in piv_cant.columns
    ]
    for m in months:
        if m not in piv_cant.columns:
            piv_cant[m] = 0.0
    piv_cant = piv_cant[
        ["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur", *months]
    ].copy()

    # ── Precio promedio por clave y réplica en meses
    precio_avg = (
        df.groupby(
            ["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur"], dropna=False
        )["PrecioUN"]
        .mean()
        .reset_index()
    )

    # Métrica = Cantidad
    cantidad_df = piv_cant.copy()
    cantidad_df.insert(3, "Métrica", "Cantidad")
    cantidad_df = cantidad_df[
        [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            *months,
            "DocCur",
        ]
    ]

    # Métrica = Precio (replicar PrecioUN por mes)
    precio_df = piv_cant.merge(
        precio_avg,
        on=["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur"],
        how="left",
    )
    for m in months:
        precio_df[m] = (
            pd.to_numeric(precio_df["PrecioUN"], errors="coerce")
            .fillna(0.0)
            .astype("float64")
        )
    precio_df.insert(3, "Métrica", "Precio")
    # Opcional: mantener PrecioUN como columna para auditoría; si no, se elimina:
    # precio_df = precio_df.drop(columns=["PrecioUN"], errors="ignore")
    precio_df = precio_df[
        [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "Métrica",
            "OcrCode3",
            *months,
            "PrecioUN",
            "DocCur",
        ]
    ]

    # ── Unir métricas
    buffer_df = pd.concat([cantidad_df, precio_df], ignore_index=True)

    # ── Reincorporar 'Linea' si existía
    if prev_linea_map is not None and not prev_linea_map.empty:
        buffer_df = buffer_df.merge(
            prev_linea_map, on=["ItemCode", "OcrCode3"], how="left"
        )

    # ── Casts finales y guardar en sesión
    for c in ["ItemCode", "ItemName", "TipoForecast", "Métrica", "OcrCode3", "DocCur"]:
        buffer_df[c] = buffer_df[c].astype("string")
    for m in months:
        buffer_df[m] = (
            pd.to_numeric(buffer_df[m], errors="coerce").fillna(0.0).astype("float64")
        )
    if "PrecioUN" in buffer_df.columns:
        buffer_df["PrecioUN"] = (
            pd.to_numeric(buffer_df["PrecioUN"], errors="coerce")
            .fillna(0.0)
            .astype("float64")
        )

    # Mantener columnas ordenadas y setear índice esperado
    ordered = ["ItemCode", "ItemName", "TipoForecast", "Métrica", "OcrCode3", *months]
    if "PrecioUN" in buffer_df.columns:
        ordered += ["PrecioUN"]
    ordered += ["DocCur"]
    if "Linea" in buffer_df.columns:
        ordered += ["Linea"]

    buffer_df = buffer_df[ordered].copy()

    st.session_state[key_buffer] = buffer_df.set_index(
        ["ItemCode", "TipoForecast", "Métrica"]
    ).sort_index()

    # ── Trazas
    print(f"[RFE-005] saved buffer -> {key_buffer}")
    print(f"[RFE-005] buffer shape (rows x cols): {st.session_state[key_buffer].shape}")
    print(f"[RFE-005] columnas: {list(st.session_state[key_buffer].columns)}")
    try:
        with pd.option_context("display.max_columns", None):
            print("[RFE-005] head (indexed):")
            print(st.session_state[key_buffer].head().to_string())
    except Exception as e:
        print(f"[RFE-005] head preview error: {e}")

    print("[RFE-005] ✅ done _refrescar_buffer_ui")
