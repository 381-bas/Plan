# B_FCS001: Importaciones y configuración de base de datos para consultas forecast
# # ∂B_FCS001/∂B0
from __future__ import annotations
import pandas as pd
from typing import Any
import os
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
from hashlib import sha256
from pandas.util import hash_pandas_object  # hash estructural
from session_utils import normalize_df_for_hash, safe_pickle_load, atomic_pickle_dump

from utils.db import (
    _run_forecast_write,
    _run_forecast_insert_get_id,
    _run_log_to_sql,
)

from config.contexto import obtener_slpcode

from utils.repositorio_forecast.repositorio_forecast_editor import (
    obtener_buffer_cliente,  # ∂B
    get_key_buffer,
)


# B_FCS005: Obtener forecast editable para edición directa por cliente
# ∂B_FCS005/∂B0
def obtener_forecast_editable(
    slp_code: int,
    card_code: str,
    anio: int | None = None,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Devuelve el forecast editable (cantidad, precio UN, moneda) en formato ancho 01-12,
    tomando únicamente el último ForecastID completo del cliente/año.
    """
    filtro_anio = "AND strftime('%Y', fd.FechEntr) = ?" if anio else ""
    filtro_anio_id = "AND strftime('%Y', FechEntr) = ?" if anio else ""

    query = f"""
    WITH ultimo_id AS (
        SELECT MAX(ForecastID) AS ForecastID
        FROM Forecast_Detalle
        WHERE SlpCode = ? AND CardCode = ?
          {filtro_anio_id}
    )
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
    JOIN ultimo_id u
      ON fd.ForecastID = u.ForecastID
    LEFT JOIN OITM i
      ON i.ItemCode = fd.ItemCode
    WHERE fd.SlpCode = ?
      AND fd.CardCode = ?
      {filtro_anio}
    GROUP BY
        fd.ItemCode, i.ItemName, fd.TipoForecast, fd.OcrCode3, Mes
    ORDER BY
        fd.ItemCode, fd.TipoForecast, Mes;
    """

    # orden exacto de parámetros: (para CTE) slp, card, [anio]  +  (para WHERE final) slp, card, [anio]
    params: list[Any] = [slp_code, card_code]
    if anio:
        params.append(str(anio))
    params += [slp_code, card_code]
    if anio:
        params.append(str(anio))

    df = run_query(query, db_path, tuple(params))

    # Si no hay datos para el último ForecastID, devolver estructura vacía
    if df.empty:
        return pd.DataFrame(
            columns=[
                "ItemCode",
                "ItemName",
                "TipoForecast",
                "Métrica",
                "OcrCode3",
                "PrecioUN",
                "DocCur",
                *[str(m).zfill(2) for m in range(1, 13)],
            ]
        )

    # Pivot a 01..12
    pivot = df.pivot_table(
        index=["ItemCode", "ItemName", "TipoForecast", "OcrCode3"],
        columns="Mes",
        values="Cantidad",
        fill_value=0,
    ).reset_index()

    # Adjunta PrecioUN y DocCur por clave (promedio / first son seguros por clave)
    precio_un = df.groupby(["ItemCode", "TipoForecast", "OcrCode3"], as_index=False)[
        "PrecioUN"
    ].mean()
    doc_cur = df.groupby(["ItemCode", "TipoForecast", "OcrCode3"], as_index=False)[
        "DocCur"
    ].first()

    pivot = pivot.merge(precio_un, on=["ItemCode", "TipoForecast", "OcrCode3"]).merge(
        doc_cur, on=["ItemCode", "TipoForecast", "OcrCode3"]
    )

    # Nombres de columnas 01..12 y relleno de faltantes
    pivot.columns = [
        str(c).zfill(2) if isinstance(c, int) else c for c in pivot.columns
    ]
    for m in range(1, 13):
        col = f"{m:02d}"
        if col not in pivot.columns:
            pivot[col] = 0

    pivot["Métrica"] = "Cantidad"

    orden = (
        ["ItemCode", "ItemName", "TipoForecast", "Métrica", "OcrCode3"]
        + [f"{m:02d}" for m in range(1, 13)]
        + ["PrecioUN", "DocCur"]
    )
    return pivot[orden]


# B_BUF003: Inicialización extendida del buffer forecast con estructura y Métrica
# # ∂B_BUF003/∂B0
def inicializar_buffer_cliente(
    cliente: str, df_base: pd.DataFrame, moneda_default: str = "CLP"
):
    key = get_key_buffer(cliente)
    if key in st.session_state:
        return

    df_base = df_base.copy()
    df_base.columns = df_base.columns.astype(str)
    df_base["ItemCode"] = df_base["ItemCode"].astype(str).str.strip()
    df_base["TipoForecast"] = df_base["TipoForecast"].astype(str).str.strip()

    # 🧬 Forzar expansión por TipoForecast: cada ItemCode debe tener Firme y Proyectado
    tipos = ["Firme", "Proyectado"]
    df_expandido = []

    for itemcode in df_base["ItemCode"].unique():
        df_item = df_base[df_base["ItemCode"] == itemcode]
        tipos_actuales = df_item["TipoForecast"].unique()

        for tipo in tipos:
            if tipo in tipos_actuales:
                df_expandido.append(df_item[df_item["TipoForecast"] == tipo].copy())
            else:
                df_clon = df_item[df_item["TipoForecast"] == tipos_actuales[0]].copy()
                df_clon["TipoForecast"] = tipo
                meses = [str(m).zfill(2) for m in range(1, 13)]
                df_clon[meses] = 0  # Reiniciar valores de meses
                df_expandido.append(df_clon)

    df_base = pd.concat(df_expandido, ignore_index=True)

    # 🔒 Validar estructura mínima
    columnas_minimas = {"ItemCode", "TipoForecast"}
    if not columnas_minimas.issubset(df_base.columns):
        raise ValueError(
            f"Faltan columnas esenciales en df_base: {columnas_minimas - set(df_base.columns)}"
        )

    # ⚠️ Captura los valores de PrecioUN antes de eliminar
    precio_un_map = None
    if "PrecioUN" in df_base.columns:
        precio_un_map = df_base[
            ["ItemCode", "TipoForecast", "OcrCode3", "PrecioUN"]
        ].copy()

    # 🧹 Eliminar columnas conflictivas
    columnas_prohibidas = ["PrecioUN", "_PrecioUN_", "PrecioUnitario"]
    df_base = df_base.drop(
        columns=[c for c in columnas_prohibidas if c in df_base.columns],
        errors="ignore",
    )

    # Asegurar columnas adicionales
    if "OcrCode3" not in df_base.columns:
        df_base["OcrCode3"] = ""
    if "DocCur" not in df_base.columns:
        df_base["DocCur"] = moneda_default

    columnas_mes = [str(m).zfill(2) for m in range(1, 13)]

    # Crear duplicado por Métrica
    cantidad = df_base.copy()
    cantidad["Métrica"] = "Cantidad"

    precio = df_base.copy()
    precio["Métrica"] = "Precio"

    # ✅ Aplicar PrecioUN a todos los meses si estaba disponible
    if precio_un_map is not None:
        precio = precio.merge(
            precio_un_map, on=["ItemCode", "TipoForecast", "OcrCode3"], how="left"
        )
        for col in columnas_mes:
            precio[col] = precio["PrecioUN"]
        precio = precio.drop(columns=["PrecioUN"], errors="ignore")
    else:
        precio[columnas_mes] = 0

    df_combo = pd.concat([cantidad, precio], ignore_index=True)

    st.session_state[key] = df_combo.set_index(["ItemCode", "TipoForecast", "Métrica"])


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


BASE_TEMP = os.path.join(os.path.dirname(__file__), "..", "temp_ediciones")
os.makedirs(BASE_TEMP, exist_ok=True)


# B_TMP002: Construcción de ruta de backup temporal para cliente
# # ∂B_TMP002/∂B0
def _ruta_temp(cliente: str) -> str:
    return os.path.join(BASE_TEMP, f"{cliente}_forecast.pkl")


# B_HDF001: Hash robusto de DataFrame usando SHA-256 (control de integridad)
# # ∂B_HDF001/∂B0
def hash_df(df):
    return hashlib.sha256(
        pd.util.hash_pandas_object(df.sort_index(axis=1), index=True).values
    ).hexdigest()


# B_TMP003: Guardado seguro de backup temporal (.pkl) del DataFrame de cliente
# # ∂B_TMP003/∂B0
def guardar_temp_local(cliente: str, df: pd.DataFrame):
    """
    Backup temporal (pickle) por cliente:
    - Hash estructural estable para evitar escrituras redundantes.
    - Lectura segura (lista blanca) confinada al directorio destino.
    - Escritura atómica (tmp + replace).
    Logs: [TMP.INFO]/[TMP.WARN]/[TMP.ERROR] en una sola línea, sin emojis.
    """

    t0 = time.perf_counter()
    ruta_str = _ruta_temp(cliente)  # p.ej. ".../tmp/<cliente>.pkl"
    ruta = Path(ruta_str).resolve()
    ruta.parent.mkdir(parents=True, exist_ok=True)

    try:
        rows, cols = getattr(df, "shape", (0, 0))
        print(f"[TMP.INFO] start cliente={cliente} path={ruta} shape=({rows},{cols})")

        df_norm = normalize_df_for_hash(df)
        nuevo_hash = int(hash_pandas_object(df_norm, index=True).sum())

        hash_prev = None
        if ruta.exists():
            try:
                df_prev = safe_pickle_load(ruta, ruta.parent)
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

        atomic_pickle_dump(df, ruta)
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
    import pandas as pd

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
    df_detalle: pd.DataFrame,
    forecast_id: int,
    anio: int,
    db_path: str | None = None,
):
    """
    Inserta (o reemplaza) el detalle de un Forecast de forma idempotente.
    ▸ NO inserta Cant==0 (bajas se materializan con DELETE puntual).
    ▸ Construye FechEntr desde anio+Mes (YYYY-MM-01).
    ▸ Crea índice único para habilitar UPSERT.
    Logs compactos y consistentes: [DETALLE.INFO]/[DETALLE.WARN]/[DETALLE.ERROR] + [METRICAS].
    """
    import time

    t0 = time.perf_counter()
    print(
        f"[DETALLE.START] insertar_forecast_detalle forecast_id={forecast_id} anio={anio}"
    )

    if not forecast_id or forecast_id < 0:
        raise ValueError(f"[DETALLE.ERROR] ForecastID inválido: {forecast_id}")

    required = {
        "CardCode",
        "ItemCode",
        "TipoForecast",
        "OcrCode3",
        "Linea",
        "Cant",
        "PrecioUN",
        "DocCur",
        "SlpCode",
        "Mes",
    }
    missing = required - set(df_detalle.columns)
    if missing:
        raise ValueError(f"[DETALLE.ERROR] Faltan columnas requeridas: {missing}")

    # 0) Normalización
    df = df_detalle.copy()
    print(f"[DETALLE.INFO] rows_in={len(df)}")

    df["Mes"] = df["Mes"].astype(str).str.zfill(2)
    df["Cant"] = (
        pd.to_numeric(df["Cant"], errors="coerce").fillna(0.0).astype("float64")
    )
    df["PrecioUN"] = (
        pd.to_numeric(df["PrecioUN"], errors="coerce").fillna(0.0).astype("float64")
    )

    # 1) Pre-chequeo de duplicados en el LOTE (clave negocio + Mes)
    clave_lote = [
        "CardCode",
        "ItemCode",
        "TipoForecast",
        "OcrCode3",
        "Linea",
        "Mes",
        "DocCur",
        "SlpCode",
    ]
    dup_counts = df.groupby(clave_lote).size().reset_index(name="count")
    dup_keys = int((dup_counts["count"] > 1).sum())
    print(f"[DETALLE.INFO] check_dups keys_duplicadas={dup_keys}")
    if dup_keys:
        print("[DETALLE.WARN] lote_duplicados keep=last (no suma)")
        df = df.sort_index().drop_duplicates(subset=clave_lote, keep="last")

    # 2) Construir FechEntr = YYYY-MM-01
    df["FechEntr"] = pd.to_datetime(
        df["Mes"].radd(f"{anio}-"), format="%Y-%m", errors="coerce"
    ).dt.strftime("%Y-%m-01")
    if df["FechEntr"].isna().any():
        errores = df[df["FechEntr"].isna()][["ItemCode", "TipoForecast", "Mes"]]
        print(f"[DETALLE.ERROR] fechentr_invalid rows={len(errores)}")
        raise ValueError("Mes inválido: no se pudo construir FechEntr.")

    sample_fech = ", ".join(df["FechEntr"].dropna().astype(str).unique()[:3])
    print(
        f"[DETALLE.INFO] fechas_generadas uniq={df['FechEntr'].nunique()} sample=[{sample_fech}]"
    )

    # 3) Índice único para habilitar UPSERT (idempotencia)
    t_idx = time.perf_counter()
    _run_forecast_write(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_ForecastDetalle
        ON Forecast_Detalle (
          ForecastID, SlpCode, CardCode, ItemCode, Linea, OcrCode3, TipoForecast, FechEntr
        );
        """,
        None,
        many=False,
        db_path=db_path,
    )
    print(f"[DETALLE.INFO] index_ready elapsed={time.perf_counter()-t_idx:.3f}s")

    # 4) Particionar BAJAS (Cant==0) vs UPserts (Cant>0)
    df_bajas = df[df["Cant"] == 0.0].copy()
    df_upsert = df[df["Cant"] > 0.0].copy()
    print(f"[DETALLE.INFO] partitions bajas={len(df_bajas)} upsert={len(df_upsert)}")

    # 4.a) BAJAS: DELETE puntual por clave COMPLETA
    rows_deleted = 0
    if not df_bajas.empty:
        t_del = time.perf_counter()
        tuplas_delete = [
            (
                forecast_id,
                int(r.SlpCode),
                r.CardCode,
                int(r.ItemCode),
                r.Linea,
                r.OcrCode3,
                r.TipoForecast,
                r.FechEntr,
            )
            for r in df_bajas.itertuples()
        ]
        _run_forecast_write(
            """
            DELETE FROM Forecast_Detalle
            WHERE ForecastID = ?
              AND SlpCode    = ?
              AND CardCode   = ?
              AND ItemCode   = ?
              AND Linea      = ?
              AND OcrCode3   = ?
              AND TipoForecast = ?
              AND FechEntr   = ?
            """,
            tuplas_delete,
            many=True,
            db_path=db_path,
        )
        rows_deleted = len(tuplas_delete)
        print(
            f"[DETALLE.INFO] delete_applied rows={rows_deleted} elapsed={time.perf_counter()-t_del:.3f}s"
        )

    # 4.b) ALTAS/MODIF: UPSERT (NO insertamos ceros)
    rows_upserted = 0
    if not df_upsert.empty:
        t_ins = time.perf_counter()
        tuplas_upsert = [
            (
                forecast_id,
                r.CardCode,
                int(r.ItemCode),
                r.FechEntr,
                r.TipoForecast,
                r.OcrCode3,
                r.Linea,
                float(r.Cant),
                float(r.PrecioUN),
                r.DocCur,
                int(r.SlpCode),
            )
            for _, r in df_upsert.iterrows()
        ]
        print(f"[DETALLE.INFO] upsert rows={len(tuplas_upsert)}")
        _run_forecast_write(
            """
            INSERT INTO Forecast_Detalle (
                ForecastID, CardCode, ItemCode, FechEntr,
                TipoForecast, OcrCode3, Linea, Cant,
                PrecioUN, DocCur, SlpCode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ForecastID, SlpCode, CardCode, ItemCode, Linea, OcrCode3, TipoForecast, FechEntr)
            DO UPDATE SET
              Cant      = excluded.Cant,
              PrecioUN  = excluded.PrecioUN,
              DocCur    = excluded.DocCur;
            """,
            tuplas_upsert,
            many=True,
            db_path=db_path,
        )
        rows_upserted = len(tuplas_upsert)
        print(
            f"[DETALLE.INFO] upsert_done rows={rows_upserted} elapsed={time.perf_counter()-t_ins:.3f}s"
        )

    total_cant = float(df_upsert["Cant"].sum()) if not df_upsert.empty else 0.0
    print(
        f"[METRICAS] rows_deleted={rows_deleted}, rows_upserted={rows_upserted}, zero_transitions_applied={len(df_bajas)}"
    )
    print(
        f"[DETALLE.END] total_cantidad_pos={total_cant:,.2f} elapsed={time.perf_counter()-t0:.3f}s"
    )


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
    import time
    import numpy as np
    import pandas as pd  # ← Asegura disponibilidad de pd dentro de la función

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


def _refrescar_buffer_ui(forecast_id: int, key_buffer: str, db_path: str):
    """
    Reconstruye el buffer de UI SOLO desde BD para el ForecastID activo,
    deduplicando por clave de negocio y sin sumar registros duplicados.
    Garantiza 4 filas (Cantidad/Precio × Firme/Proyectado) y 12 meses (01..12)
    por par base (ItemCode+OcrCode3+DocCur).
    Logs: [BUFFER.INFO]/[BUFFER.WARN]/[BUFFER.ERROR] en una sola línea, sin emojis.
    """
    import time

    t0 = time.perf_counter()
    print(f"[BUFFER.INFO] refresh.start forecast_id={forecast_id} key={key_buffer}")

    # 1) Traer SOLO el detalle del ForecastID activo (sin Timestamp/ID)
    qry = """
        SELECT
            ItemCode,
            TipoForecast,
            OcrCode3,
            Linea,
            DocCur,
            CAST(strftime('%m', FechEntr) AS TEXT) AS Mes,
            Cant,
            PrecioUN,
            ROWID AS _rid
        FROM Forecast_Detalle
        WHERE ForecastID = ?
    """
    try:
        df_post = run_query(qry, params=(forecast_id,), db_path=db_path)
    except Exception as e:
        print(
            f"[BUFFER.ERROR] query_failed forecast_id={forecast_id} exc={type(e).__name__} msg={e}"
        )
        raise

    if df_post is None or df_post.empty:
        print(
            f"[BUFFER.WARN] empty forecast_id={forecast_id} action=session_clear key={key_buffer} elapsed={time.perf_counter()-t0:.3f}s"
        )
        st.session_state[key_buffer] = None
        return

    print(f"[BUFFER.INFO] fetch rows={len(df_post)} cols={list(df_post.columns)}")

    # 2) Normalizar Mes a '01'..'12'
    df_post["Mes"] = df_post["Mes"].astype(str).str.zfill(2)

    # 3) Ordenar para que 'last' sea realmente el último registro insertado
    sort_cols = ["_rid"]
    if "FechEntr" in df_post.columns:
        sort_cols.insert(0, "FechEntr")
    df_post = df_post.sort_values(sort_cols)
    print(f"[BUFFER.INFO] sort.by={sort_cols}")

    # 4) Deduplicar por clave de negocio a nivel de mes (quedarse con el último)
    claves_mes = ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Mes"]
    before_dedup = len(df_post)
    df_dedup = df_post.drop_duplicates(claves_mes, keep="last").copy()
    after_dedup = len(df_dedup)
    print(
        f"[BUFFER.INFO] dedup key={claves_mes} before={before_dedup} after={after_dedup} removed={before_dedup-after_dedup}"
    )

    # 5) Mapear 'Linea' representativa
    clave_sin_mes_full = ["ItemCode", "TipoForecast", "OcrCode3", "DocCur"]
    clave_base = ["ItemCode", "OcrCode3", "DocCur"]
    linea_map_full = df_dedup.groupby(clave_sin_mes_full, as_index=False)[
        "Linea"
    ].last()
    linea_map_base = df_dedup.groupby(clave_base, as_index=False)["Linea"].last()

    # 6) Pivots (usar último valor, sin sumas)
    meses = [f"{i:02d}" for i in range(1, 13)]

    pivot_cant = (
        df_dedup.pivot_table(
            index=clave_sin_mes_full,
            columns="Mes",
            values="Cant",
            aggfunc="last",
            fill_value=0.0,
        )
        .reindex(columns=meses, fill_value=0.0)
        .reset_index()
    )
    pivot_cant["Métrica"] = "Cantidad"

    pivot_prec = (
        df_dedup.pivot_table(
            index=clave_sin_mes_full,
            columns="Mes",
            values="PrecioUN",
            aggfunc="last",
            fill_value=0.0,
        )
        .reindex(columns=meses, fill_value=0.0)
        .reset_index()
    )
    pivot_prec["Métrica"] = "Precio"

    print(f"[BUFFER.INFO] pivot.shapes cant={pivot_cant.shape} prec={pivot_prec.shape}")

    # 7) Unir métricas y re-incorporar 'Linea'
    df_metrico = pd.concat([pivot_cant, pivot_prec], ignore_index=True)
    df_metrico = df_metrico.merge(
        linea_map_full, on=clave_sin_mes_full, how="left", suffixes=("", "_from_full")
    )

    # Fallback Linea por clave base si quedó nulo
    mask_linea_null = df_metrico["Linea"].isna()
    if mask_linea_null.any():
        missing = int(mask_linea_null.sum())
        print(
            f"[BUFFER.INFO] linea.null.after_full count={missing} fallback_key={clave_base}"
        )
        df_metrico = df_metrico.merge(
            linea_map_base, on=clave_base, how="left", suffixes=("", "_fallback")
        )
        df_metrico["Linea"] = df_metrico["Linea"].fillna(df_metrico["Linea_fallback"])
        df_metrico.drop(
            columns=[c for c in df_metrico.columns if c.endswith("_fallback")],
            inplace=True,
        )

    # 8) Garantizar 4 filas por base (Firme/Proyectado × Cantidad/Precio)
    base = df_metrico[clave_base].drop_duplicates()
    tipos = pd.DataFrame({"TipoForecast": ["Firme", "Proyectado"]})
    metricas = pd.DataFrame({"Métrica": ["Cantidad", "Precio"]})
    grid = base.merge(tipos, how="cross").merge(metricas, how="cross")

    df_metrico = grid.merge(
        df_metrico,
        how="left",
        on=["ItemCode", "OcrCode3", "DocCur", "TipoForecast", "Métrica"],
        suffixes=("", "_y"),
    )

    # Rellenos de meses
    for m in meses:
        if m not in df_metrico.columns:
            df_metrico[m] = 0.0
        df_metrico[m] = pd.to_numeric(df_metrico[m], errors="coerce").fillna(0.0)

    # Si aún faltase Linea, completar con el último disponible por base
    if df_metrico["Linea"].isna().any():
        left = int(df_metrico["Linea"].isna().sum())
        print(f"[BUFFER.INFO] linea.null.after_merge count={left} fill=base_map")
        df_metrico = df_metrico.merge(
            linea_map_base, on=clave_base, how="left", suffixes=("", "_b")
        )
        df_metrico["Linea"] = df_metrico["Linea"].fillna(df_metrico["Linea_b"])
        df_metrico.drop(
            columns=[c for c in df_metrico.columns if c.endswith("_b")], inplace=True
        )

    # 9) Reordenar columnas finales
    cols_fijas = ["ItemCode", "TipoForecast", "OcrCode3", "Linea", "DocCur", "Métrica"]
    df_metrico = (
        df_metrico[cols_fijas + meses]
        .sort_values(by=["ItemCode", "TipoForecast", "Métrica"])
        .reset_index(drop=True)
    )

    expected = len(base) * 4
    final_rows = len(df_metrico)
    if final_rows != expected:
        print(
            f"[BUFFER.WARN] row_mismatch base={len(base)} expected={expected} final={final_rows}"
        )

    print(
        f"[BUFFER.INFO] grid.base={len(base)} rows.final={final_rows} cols.final={len(df_metrico.columns)}"
    )
    print(f"[BUFFER.INFO] cols.final={df_metrico.columns.tolist()}")

    # 10) Persistir en sesión con el índice usado por la UI
    st.session_state[key_buffer] = df_metrico.set_index(
        ["ItemCode", "TipoForecast", "Métrica"]
    )
    print(
        f"[BUFFER.INFO] session.set key={key_buffer} rows={final_rows} index=('ItemCode','TipoForecast','Métrica') elapsed={time.perf_counter()-t0:.3f}s"
    )
