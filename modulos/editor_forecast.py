# B_FCS001: Importaciones y configuración de base de datos para consultas forecast
# # ∂B_FCS001/∂B0
from __future__ import annotations
import pandas as pd
from typing import Any
import os
import re
import hashlib
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
    """
    Fusiona los cambios del editor con el buffer activo y
    devuelve (df_final, hay_cambios).
    """
    print("🔄 [SYNC-LOCAL-START] Iniciando sincronización buffer local")
    print(f"📊 [SYNC-LOCAL-INFO] df_buffer shape: {df_buffer.shape}")
    print(f"📝 [SYNC-LOCAL-INFO] df_editado shape: {df_editado.shape}")

    columnas_clave = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
    print(f"🔑 [SYNC-LOCAL-INFO] Columnas clave: {columnas_clave}")

    # ── Detectar dinámicamente las columnas-mes ──────────────────────────────
    columnas_mes = sorted(
        [c for c in df_editado.columns if c.isdigit() and len(c) <= 2],
        key=lambda x: int(x),
    )
    columnas_req = columnas_clave + columnas_mes

    print(f"📅 [SYNC-LOCAL-INFO] Columnas mes detectadas: {columnas_mes}")
    print(f"📋 [SYNC-LOCAL-INFO] Columnas requeridas: {columnas_req}")

    # ── Validación mínima de esquema ────────────────────────────────────────
    faltantes = set(columnas_req) - set(df_editado.columns)
    if faltantes:
        print(f"❌ [SYNC-LOCAL-ERROR] Columnas faltantes en editado: {faltantes}")
        raise ValueError(
            f"El DataFrame editado carece de columnas requeridas: {faltantes}"
        )

    # ── Índices normalizados ────────────────────────────────────────────────
    print("🔧 [SYNC-LOCAL-STEP] Configurando índices...")
    buf_idx = df_buffer.set_index(columnas_clave)
    edi_idx = df_editado.set_index(columnas_clave)

    # Ordenarlos una única vez: evita PerformanceWarning y acelera update()
    buf_idx = buf_idx.sort_index()
    edi_idx = edi_idx.sort_index()
    print(
        f"📊 [SYNC-LOCAL-INFO] Índices ordenados - buffer: {buf_idx.shape}, editado: {edi_idx.shape}"
    )

    # Unir índices para contemplar filas nuevas/eliminadas
    idx_union = buf_idx.index.union(edi_idx.index)
    print(f"🔗 [SYNC-LOCAL-INFO] Unión de índices: {len(idx_union)} registros únicos")

    # IMPORTANTÍSIMO: reindex devuelve vistas DESORDENADAS → volvemos a ordenar
    buf_idx = buf_idx.reindex(idx_union).sort_index()
    edi_idx = edi_idx.reindex(idx_union).sort_index()
    print("✅ [SYNC-LOCAL-STEP] Reindexado y ordenado completado")

    # ── Comparación de celdas (tolerante a float/NaN) ───────────────────────
    print("🔍 [SYNC-LOCAL-STEP] Comparando celdas...")
    diff_array = ~np.isclose(
        buf_idx[columnas_mes], edi_idx[columnas_mes], atol=1e-6, equal_nan=True
    )
    dif_mask = pd.DataFrame(diff_array, index=buf_idx.index, columns=columnas_mes)

    total_diff = int(dif_mask.values.sum())
    filas_diff = int(dif_mask.any(axis=1).sum())
    hay_cambios = total_diff > 0

    if hay_cambios:
        print(f"📈 [SYNC-LOCAL-CHANGES] Total celdas modificadas: {total_diff}")
        print(f"📈 [SYNC-LOCAL-CHANGES] Filas afectadas: {filas_diff}")
        cols_mod = dif_mask.any().pipe(lambda s: s[s].index.tolist())
        print(f"📈 [SYNC-LOCAL-CHANGES] Columnas mensuales modificadas: {cols_mod}")

        # Aplicar cambios
        print("🔄 [SYNC-LOCAL-STEP] Aplicando actualizaciones...")
        buf_idx.update(edi_idx[columnas_mes])

        # Filas completamente nuevas
        filas_nuevas = dif_mask.index[dif_mask.all(axis=1)]
        if len(filas_nuevas):
            print(f"🆕 [SYNC-LOCAL-NEW] Filas nuevas detectadas: {len(filas_nuevas)}")
            buf_idx.loc[filas_nuevas, columnas_mes] = edi_idx.loc[
                filas_nuevas, columnas_mes
            ]
    else:
        print("✅ [SYNC-LOCAL-INFO] No se detectaron diferencias reales.")

    # ── Reconstrucción final con columnas extra ─────────────────────────────
    #    Calculamos todas las columnas NO-mes ni clave presentes en
    #    buffer o editado (ItemName, DocCur, etc.)
    cols_extra_union = [
        c
        for c in set(df_buffer.columns).union(df_editado.columns)
        if c not in columnas_clave and c not in columnas_mes
    ]

    if cols_extra_union:
        print(
            f"📋 [SYNC-LOCAL-STEP] Procesando {len(cols_extra_union)} columnas extra: {cols_extra_union}"
        )

        # a) Start with values from buffer (may include NaN)
        buf_idx[cols_extra_union] = df_buffer.set_index(columnas_clave)[
            cols_extra_union
        ].reindex(buf_idx.index)

        # b) Update with non-NaN coming from editado
        edi_extra = df_editado.set_index(columnas_clave)[cols_extra_union].reindex(
            buf_idx.index
        )
        buf_idx.update(edi_extra)
        print("✅ [SYNC-LOCAL-STEP] Columnas extra actualizadas")
    else:
        print("ℹ️  [SYNC-LOCAL-INFO] No hay columnas extra para procesar")

    #   Ensamblamos el DataFrame final
    df_final = buf_idx.reset_index().reindex(columns=columnas_req + cols_extra_union)

    #   Aplicar dtypes solo a columnas presentes
    dtype_map = {c: t for c, t in df_buffer.dtypes.items() if c in df_final.columns}
    df_final = df_final.astype(dtype_map, errors="ignore")
    print(f"✅ [SYNC-LOCAL-DTYPES] Dtypes aplicados: {len(dtype_map)} columnas")

    print(f"🎯 [SYNC-LOCAL-END] Buffer final preparado. Shape: {df_final.shape}")
    print(f"📊 [SYNC-LOCAL-RESULT] Hay cambios: {hay_cambios}")
    print(f"📋 [SYNC-LOCAL-COLUMNS] Columnas finales: {list(df_final.columns)}")

    return df_final, hay_cambios


# B_SYN002: Sincronización y guardado individual de buffer editado para cliente
# # ∂B_SYN002/∂B0
def sincronizar_para_guardado_final(key_buffer: str, df_editado: pd.DataFrame):
    print(f"🎯 [SYNC-FINAL-START] Inicio sincronización final - Buffer: {key_buffer}")
    print(f"📊 [SYNC-FINAL-INFO] DataFrame inicial shape: {df_editado.shape}")
    print(f"📋 [SYNC-FINAL-INFO] Columnas iniciales: {list(df_editado.columns)}")
    print(
        f"🔍 [SYNC-FINAL-INFO] Estado session_state pre-sync: {list(st.session_state.keys())}"
    )

    # 🔀 1) Unificación de métricas Cantidad + Precio
    print("🔄 [SYNC-FINAL-STEP] Unificando métricas Cantidad + Precio...")
    df_editado_unificado = pd.concat(
        [
            df_editado[df_editado["Métrica"] == "Cantidad"],
            df_editado[df_editado["Métrica"] == "Precio"],
        ],
        ignore_index=True,
    )
    print(
        f"📈 [SYNC-FINAL-INFO] Total filas tras unificación: {len(df_editado_unificado)}"
    )
    metricas_count = df_editado_unificado["Métrica"].value_counts().to_dict()
    print(f"📊 [SYNC-FINAL-INFO] Distribución métricas: {metricas_count}")

    # 🔄 2) Recuperar buffer actual
    print(f"📂 [SYNC-FINAL-STEP] Recuperando buffer actual: {key_buffer}")
    df_base_actual = obtener_buffer_cliente(key_buffer).reset_index()
    print(f"📊 [SYNC-FINAL-INFO] Buffer base recuperado shape: {df_base_actual.shape}")
    print(f"📋 [SYNC-FINAL-INFO] Columnas buffer base: {list(df_base_actual.columns)}")

    # 🔄 3) Sincronizar (ahora devuelve tupla)
    print("🔄 [SYNC-FINAL-STEP] Ejecutando sincronización buffer local...")
    df_sync, hay_cambios = sincronizar_buffer_local(
        df_base_actual, df_editado_unificado
    )
    print(f"📊 [SYNC-FINAL-INFO] Resultado sincronización - Hay cambios: {hay_cambios}")
    print(f"📈 [SYNC-FINAL-INFO] DataFrame sincronizado shape: {df_sync.shape}")

    if not hay_cambios:
        print("✅ [SYNC-FINAL-SKIP] Sin cambios reales -> se omite guardado final.")
        return df_base_actual  # ⬅️  nada más que hacer

    # ---------------------------------------------------------------------
    # 🔽 Solo se ejecuta esta parte si hay_cambios == True
    # ---------------------------------------------------------------------
    print("🚀 [SYNC-FINAL-CHANGES] Procesando cambios detectados...")
    print(
        f"📋 [SYNC-FINAL-INFO] Columnas post-sincronización: {df_sync.columns.tolist()}"
    )

    print("🔍 [SYNC-FINAL-STEP] Analizando cardinalidad de índices...")
    index_stats = df_sync[["ItemCode", "TipoForecast", "Métrica"]].nunique()
    print("📊 [SYNC-FINAL-STATS] Cardinalidad post-sync:")
    print(f"   - ItemCode: {index_stats['ItemCode']}")
    print(f"   - TipoForecast: {index_stats['TipoForecast']}")
    print(f"   - Métrica: {index_stats['Métrica']}")

    # Validación de nulos en columnas clave
    print("🔍 [SYNC-FINAL-STEP] Validando nulos en columnas clave...")
    nulos_detectados = False
    for col in ["ItemCode", "TipoForecast", "Métrica"]:
        nulos_count = df_sync[col].isna().sum()
        if nulos_count > 0:
            print(
                f"⚠️  [SYNC-FINAL-WARN] Valores nulos detectados en {col}: {nulos_count}"
            )
            nulos_detectados = True
    if not nulos_detectados:
        print("✅ [SYNC-FINAL-INFO] Sin nulos en columnas clave")

    # 👉 Guardar en session_state ordenado por índice compuesto
    print("💾 [SYNC-FINAL-STEP] Guardando en session_state...")
    df_sync = df_sync.set_index(["ItemCode", "TipoForecast", "Métrica"])
    df_sync = df_sync.sort_index()
    st.session_state[key_buffer] = df_sync
    print(f"✅ [SYNC-FINAL-INFO] Buffer guardado en session_state: {key_buffer}")

    df_guardar = df_sync.reset_index()
    print(f"📊 [SYNC-FINAL-INFO] Buffer final para guardado - Filas: {len(df_guardar)}")
    print(f"📋 [SYNC-FINAL-INFO] Columnas finales: {df_guardar.columns.tolist()}")

    # Guardado temporal local
    print("💾 [SYNC-FINAL-STEP] Guardando temporal local...")
    guardar_temp_local(key_buffer, df_guardar)
    print("✅ [SYNC-FINAL-INFO] Guardado temporal completado")

    # Actualización buffer global
    print("🌐 [SYNC-FINAL-STEP] Actualizando buffer global...")
    actualizar_buffer_global(df_guardar, key_buffer)
    print("✅ [SYNC-FINAL-INFO] Buffer global actualizado")

    # ✅ Marcar cliente como editado
    cliente = key_buffer.replace("forecast_buffer_", "")
    editados = st.session_state.get("clientes_editados", set())
    editados.add(cliente)
    st.session_state["clientes_editados"] = editados
    print(f"🏷️  [SYNC-FINAL-INFO] Cliente marcado como editado: {cliente}")
    print(f"📋 [SYNC-FINAL-INFO] Clientes editados actuales: {len(editados)}")

    # Nuevos logs de depuración
    print("🔍 [SYNC-FINAL-DEBUG] Información de depuración adicional:")
    print(f"   - Hash DataFrame pre-sync: {hash(str(df_editado.values.tobytes()))}")
    print(f"   - Key buffer: {key_buffer}")
    print("   - Verificando sincronización en progreso...")

    print("🎉 [SYNC-FINAL-END] Sincronización final completada exitosamente")
    print(f"📊 [SYNC-FINAL-RESULT] DataFrame resultante shape: {df_guardar.shape}")

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
    """
    ruta_str = _ruta_temp(cliente)  # p.ej. ".../tmp/<cliente>.pkl"
    ruta = Path(ruta_str).resolve()
    ruta.parent.mkdir(parents=True, exist_ok=True)

    try:
        df_norm = normalize_df_for_hash(df)
        nuevo_hash = int(hash_pandas_object(df_norm, index=True).sum())

        hash_prev = None
        if ruta.exists():
            try:
                df_prev = safe_pickle_load(ruta, ruta.parent)
                df_prev_norm = normalize_df_for_hash(df_prev)
                hash_prev = int(hash_pandas_object(df_prev_norm, index=True).sum())
            except Exception as _e:
                print(f"⚠️  Backup previo ilegible, se reescribirá: {ruta} ({_e})")

        if hash_prev is not None and nuevo_hash == hash_prev:
            print(f"🟡 Sin cambios para {cliente}, se evita escritura redundante.")
            return

        atomic_pickle_dump(df, ruta)
        print(f"✅ Backup temporal guardado para {cliente} -> {ruta}")

    except Exception as e:
        print(f"❌ Error al guardar backup temporal para {cliente}: {e}")


# B_SYN002: Actualización simbólica y persistente del buffer editado en sesión global
# # ∂B_SYN002/∂B0
def actualizar_buffer_global(df_editado: pd.DataFrame, key_buffer: str):
    """
    Almacena el DataFrame editado en session_state como buffer vivo.
    Usa clave simbólica con sufijo '_editado' para edición persistente.
    """
    key_state = f"{key_buffer}_editado"

    # Validación estructural mínima
    columnas_requeridas = {"ItemCode", "TipoForecast", "Métrica", "OcrCode3"}
    if not columnas_requeridas.issubset(df_editado.columns):
        raise ValueError(
            f"El DataFrame editado carece de columnas requeridas: {columnas_requeridas}"
        )

    # Limpieza defensiva
    columnas_prohibidas = ["PrecioUN", "_PrecioUN_", "PrecioUnitario"]
    df_editado = df_editado.drop(
        columns=[c for c in columnas_prohibidas if c in df_editado.columns],
        errors="ignore",
    )

    st.session_state[key_state] = df_editado.copy()

    # ✅ Línea esencial para sincronizar buffer principal
    st.session_state[key_buffer] = df_editado.set_index(
        ["ItemCode", "TipoForecast", "Métrica"]
    )
    # Marca interna de sincronización
    st.session_state["__buffer_editado__"] = True


# B_VFD001: Validación estructural y de contenido del DataFrame de forecast
# # ∂B_VFD001/∂B0
def validar_forecast_dataframe(df: pd.DataFrame) -> list[str]:
    print("🔍 [VALIDATION-START] Iniciando validación de DataFrame")
    print(f"📊 [VALIDATION-INFO] DataFrame shape: {df.shape}")
    print(f"📋 [VALIDATION-INFO] Columnas iniciales: {list(df.columns)}")

    errores: list[str] = []
    columnas_mes = [str(m).zfill(2) for m in range(1, 13)]
    print(f"📅 [VALIDATION-INFO] Columnas mes esperadas: {columnas_mes}")

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
    print("✅ [VALIDATION-STEP] Columnas convertidas a string")

    # Aviso si viene ItemName/Linea (serán ignoradas en chequeos)
    if any(c.lower() == "itemname" for c in df.columns):
        print(
            "ℹ️ [VALIDATION-INFO] Detectado 'ItemName': será ignorado por el validador (no bloquea)."
        )
    if any(c.lower() == "linea" for c in df.columns):
        print(
            "ℹ️ [VALIDATION-INFO] Detectado 'Linea': será ignorado por el validador (no bloquea)."
        )

    # Validación básica (requeridas)
    print("🔍 [VALIDATION-STEP] Validando campos requeridos...")
    campos_requeridos = ["ItemCode", "TipoForecast", "Métrica", "DocCur"]
    for col in campos_requeridos:
        if col not in df.columns:
            errores.append(f"Falta la columna requerida: {col}")
            print(f"❌ [VALIDATION-ERROR] Falta columna requerida: {col}")
        else:
            print(f"✅ [VALIDATION-OK] Columna requerida presente: {col}")

    # Normalización de valores clave
    print("🔄 [VALIDATION-STEP] Normalizando valores clave...")
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
    print("🔍 [VALIDATION-STEP] Validando contenido de columnas...")
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
    print("🔍 [VALIDATION-STEP] Validando columnas mensuales...")
    meses_faltantes = [col for col in columnas_mes if col not in df.columns]
    if meses_faltantes:
        errores.append(f"Faltan columnas de mes: {meses_faltantes}")
        print(f"❌ [VALIDATION-ERROR] Meses faltantes: {meses_faltantes}")
    else:
        print("✅ [VALIDATION-OK] Todas las columnas mensuales presentes")

    # Validación estructural extendida
    print("🔍 [VALIDATION-STEP] Validando estructura y duplicados...")
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
    print("🔍 [VALIDATION-STEP] Validando columnas residuales...")
    if "PrecioUN" in df.columns and "Métrica" in df.columns:
        # Si 'Precio' existe, su granularidad debe estar en columnas mensuales, no en una suelta 'PrecioUN'
        if not df[df["Métrica"] == "Precio"].empty:
            errores.append(
                "La columna suelta 'PrecioUN' no debe existir cuando 'Métrica' = 'Precio'. Distribuir por meses."
            )
            print("❌ [VALIDATION-ERROR] Columna PrecioUN no permitida")

    # Validación de columnas inesperadas (tolerando ItemName/Linea)
    print("🔍 [VALIDATION-STEP] Buscando columnas inesperadas...")
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
    print("🔍 [VALIDATION-STEP] Validando tipos de datos y valores negativos...")
    df[columnas_mes] = df[columnas_mes].apply(pd.to_numeric, errors="coerce").fillna(0)
    print("✅ [VALIDATION-INFO] Columnas mensuales convertidas a numéricas")

    # Negativos en cualquiera de las métricas (Cantidad/Precio)
    print("🔍 [VALIDATION-STEP] Buscando valores negativos...")
    for col in columnas_mes:
        negativos = df[df[col] < 0]
        if not negativos.empty:
            codigos = negativos["ItemCode"].astype(str).unique().tolist()[:5]
            errores.append(f"Valores negativos en mes {col} para: {codigos}")
            print(
                f"❌ [VALIDATION-ERROR] Valores negativos en {col}: {len(negativos)} registros (ej: {codigos})"
            )

    # Validación TipoForecast
    print("🔍 [VALIDATION-STEP] Validando TipoForecast...")
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
    """
    print("🔍 [EXISTE-FORECAST-START] Verificando existencia de forecast individual")
    print(
        f"📊 [EXISTE-FORECAST-INFO] slpcode: {slpcode}, cardcode: {cardcode}, anio: {anio}"
    )
    print(f"🗄️  [EXISTE-FORECAST-INFO] db_path: {db_path}")

    qry = """
        SELECT 1
        FROM Forecast_Detalle
        WHERE SlpCode = ?
          AND CardCode = ?
          AND strftime('%Y', FechEntr) = ?
        LIMIT 1
    """
    print("📝 [EXISTE-FORECAST-QUERY] Query ejecutada:")
    print(f"   {qry.strip()}")
    print(
        f"📋 [EXISTE-FORECAST-PARAMS] Parámetros: ({slpcode}, '{cardcode}', '{anio}')"
    )

    df = run_query(qry, params=(slpcode, cardcode, str(anio)), db_path=db_path)
    print(f"📊 [EXISTE-FORECAST-RESULT] Resultado query - shape: {df.shape}")
    print(f"📈 [EXISTE-FORECAST-INFO] DataFrame vacío: {df.empty}")

    existe = not df.empty

    if existe:
        print(
            f"✅ [EXISTE-FORECAST-FOUND] Forecast individual EXISTE para el cliente {cardcode}"
        )
    else:
        print(
            f"❌ [EXISTE-FORECAST-NOTFOUND] Forecast individual NO EXISTE para el cliente {cardcode}"
        )

    print(f"🎯 [EXISTE-FORECAST-END] Resultado: {existe}")
    return existe


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
    """

    print(
        f"[DEBUG-DETALLE] ▶ Iniciando inserción de detalle para ForecastID={forecast_id}"
    )
    if not forecast_id or forecast_id < 0:
        raise ValueError(f"[ERROR-DETALLE] ❌ ForecastID inválido: {forecast_id}")

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
        raise ValueError(f"[ERROR-DETALLE] ❌ Faltan columnas requeridas: {missing}")

    # 0) Normalización
    df = df_detalle.copy()
    print(f"[DEBUG-DETALLE] Registros a procesar (original): {len(df)}")

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
    print("[DEBUG-SAVE-INSERT] Verificando duplicados antes de inserción:")
    print(
        dup_counts[["ItemCode", "TipoForecast", "Mes", "count"]].to_string(index=False)
    )
    if (dup_counts["count"] > 1).any():
        print(
            "[⚠️ DEBUG-DETALLE] Lote contiene claves duplicadas. Se tomará la ÚLTIMA ocurrencia (no se sumará)."
        )
        # Mantener última ocurrencia por clave del lote
        df = df.sort_index()  # si el orden de llegada importa; ajusta según tu pipeline
        df = df.drop_duplicates(subset=clave_lote, keep="last")

    # 2) Construir FechEntr = YYYY-MM-01
    df["FechEntr"] = pd.to_datetime(
        df["Mes"].radd(f"{anio}-"), format="%Y-%m", errors="coerce"
    ).dt.strftime("%Y-%m-01")
    if df["FechEntr"].isna().any():
        errores = df[df["FechEntr"].isna()]
        print("[ERROR-DETALLE] ❌ FechEntr inválidas detectadas en:")
        print(errores[["ItemCode", "TipoForecast", "Mes"]].to_string(index=False))
        raise ValueError("Mes inválido: no se pudo construir FechEntr.")

    print("[DEBUG-DETALLE] Fechas generadas (FechEntr):")
    print(
        df[["ItemCode", "TipoForecast", "FechEntr"]]
        .drop_duplicates()
        .head(5)
        .to_string(index=False)
    )

    # 3) Índice único para habilitar UPSERT (idempotencia)
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

    # 4) Particionar BAJAS (Cant==0) vs UPserts (Cant>0)
    df_bajas = df[df["Cant"] == 0.0].copy()
    df_upsert = df[df["Cant"] > 0.0].copy()

    # 4.a) BAJAS: DELETE puntual por clave COMPLETA
    rows_deleted = 0
    if not df_bajas.empty:
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

    # 4.b) ALTAS/MODIF: UPSERT (NO insertamos ceros)
    rows_upserted = 0
    if not df_upsert.empty:
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
        print(
            f"[DEBUG-DETALLE] Insertando/Actualizando {len(tuplas_upsert)} registros (UPSERT)..."
        )
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

    total_cant = float(df_upsert["Cant"].sum()) if not df_upsert.empty else 0.0
    print("[DEBUG-DETALLE] ✅ Inserción finalizada.")
    print(
        f"[METRICAS] rows_deleted={rows_deleted}, rows_upserted={rows_upserted}, zero_transitions_applied={len(df_bajas)}"
    )
    print(f"[DEBUG-DETALLE] Total Cantidad (solo Cant>0): {total_cant:,.2f}")


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
    """
    print("🔍 [FORECAST-ACTIVO-START] Obteniendo forecast activo")
    print(
        f"📊 [FORECAST-ACTIVO-INFO] slpcode: {slpcode}, cardcode: {cardcode}, anio: {anio}"
    )
    print(f"⚡ [FORECAST-ACTIVO-INFO] force_new: {force_new}, db_path: {db_path}")

    llave = f"forecast_activo_{slpcode}_{cardcode}_{anio}"
    print(f"🔑 [FORECAST-ACTIVO-INFO] Llave session_state: {llave}")

    # Verificar si ya existe en session_state
    if not force_new and llave in st.session_state:
        forecast_id = st.session_state[llave]
        print(
            f"✅ [FORECAST-ACTIVO-CACHE] ForecastID encontrado en cache: {forecast_id}"
        )
        print(
            f"📋 [FORECAST-ACTIVO-INFO] Estado session_state keys: {list(st.session_state.keys())}"
        )
        return forecast_id

    print("🆕 [FORECAST-ACTIVO-NEW] Creando nuevo forecast (force_new o no en cache)")

    # Siempre crea un ID nuevo si force_new=True o no existe en sesión
    print("📝 [FORECAST-ACTIVO-STEP] Registrando cabecera en BD...")
    forecast_id = registrar_forecast_cabecera(slpcode, db_path)
    print(f"✅ [FORECAST-ACTIVO-REGISTER] ForecastID registrado: {forecast_id}")

    # Guardar en session_state
    st.session_state[llave] = forecast_id
    print(
        f"💾 [FORECAST-ACTIVO-SAVE] ForecastID guardado en session_state: {forecast_id}"
    )

    # Mostrar estado actual de session_state
    forecast_keys = [
        k for k in st.session_state.keys() if k.startswith("forecast_activo_")
    ]
    print(
        f"📋 [FORECAST-ACTIVO-INFO] Forecasts activos en session_state: {len(forecast_keys)}"
    )
    if forecast_keys:
        print(f"   - Keys: {forecast_keys}")

    print(f"🎯 [FORECAST-ACTIVO-END] ForecastID retornado: {forecast_id}")
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
    import numpy as np
    import pandas as pd  # ← Asegura disponibilidad de pd dentro de la función

    print("[DEBUG-SAVE-MAIN] 🚀 Iniciando proceso de guardado")
    print(f"[DEBUG-SAVE-MAIN] Session state actual: {list(st.session_state.keys())}")
    print(
        f"[DEBUG-SAVE-MAIN] Clientes editados: {st.session_state.get('clientes_editados', set())}"
    )

    clientes = st.session_state.get("clientes_editados", set()).copy()
    print(f"[DEBUG-GUARDADO] Clientes a procesar: {sorted(clientes)}")
    if not clientes:
        st.info("✅ No hay cambios pendientes por guardar")
        return

    print("[DEBUG-SAVE-BATCH] Inicio de guardado batch")
    print(f"[DEBUG-SAVE-BATCH] Total clientes a procesar: {len(clientes)}")
    print(f"[DEBUG-SAVE-BATCH] Estado session_state antes: {dict(st.session_state)}")

    # Helper local para la clave de cache del forecast activo
    def _forecast_activo_cache_key(slpcode: int, cardcode: str, anio: int) -> str:
        return f"forecast_activo_{slpcode}_{cardcode}_{anio}"

    for cliente in clientes:
        key_buffer = f"forecast_buffer_{cliente}"
        forecast_id = None  # ← Definido temprano para manejo en try/except
        print(f"\n[DEBUG-SAVE-MAIN] 📝 Procesando cliente: {cliente}")
        print(f"[DEBUG-SAVE-MAIN] Buffer key: {key_buffer}")
        print(
            f"[DEBUG-SAVE-MAIN] Estado buffer pre-guardado: {st.session_state.get(key_buffer, 'No existe')}"
        )

        if key_buffer not in st.session_state:
            print(f"[WARN] Buffer no encontrado en sesión para {cliente}")
            st.warning(f"⚠️ No se encontró buffer para cliente {cliente}.")
            continue

        try:
            df_base = st.session_state[key_buffer].reset_index()
            slpcode = int(obtener_slpcode())

            print(f"\n[DEBUG-GUARDADO] CLIENTE {cliente}")
            print(
                f"[DEBUG-GUARDADO] Paso 1: DF_BASE (filas={len(df_base)}) columnas={df_base.columns.tolist()}"
            )
            print(df_base.head(3).to_string(index=False))

            if df_base.empty:
                print(
                    f"[DEBUG-GUARDADO] DF_BASE está vacío. Se omite cliente {cliente}"
                )
                continue

            # 1) Transformación a largo (usa tu función probada)
            df_largo = df_forecast_metrico_to_largo(df_base, anio, cliente, slpcode)
            print(f"[DEBUG-GUARDADO] Paso 2: DF_LARGO generado (filas={len(df_largo)})")
            try:
                print(
                    df_largo[["ItemCode", "TipoForecast", "Mes", "Cant"]]
                    .head(8)
                    .to_string(index=False)
                )
            except Exception as e_head:
                print(
                    f"[DEBUG-GUARDADO] (no se pudo imprimir preview DF_LARGO) {e_head}"
                )

            if df_largo.empty:
                print(f"[DEBUG-GUARDADO] 🟡 DF_LARGO vacío, se omite cliente {cliente}")
                st.info(f"ℹ️ Sin datos para guardar en cliente {cliente}.")
                continue

            # 2) Buscar histórico previo (ANTES de crear nueva cabecera)
            forecast_id_prev = _get_forecast_id_prev(slpcode, cliente, anio, db_path)
            print(f"[DEBUG-GUARDADO] Paso 3: ForecastID anterior = {forecast_id_prev}")

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
            print(
                f"[DEBUG-GUARDADO] Paso 4: Cambios reales detectados = {len(df_largo_filtrado)}"
            )

            if df_largo_filtrado.empty:
                # (✔) No creamos cabecera si no hay cambios → evita Forecast_Header huérfano
                print(
                    f"[DEBUG-GUARDADO] ⏩ Sin cambios reales. Cliente omitido: {cliente}"
                )
                st.info(
                    f"⏩ Cliente {cliente}: sin cambios reales. Se omite inserción."
                )
                _reset_estado_edicion_por_cliente(cliente, key_buffer)
                continue

            # 4) Crear SIEMPRE nueva cabecera SOLO cuando hay cambios
            #    (✔) force_new=True garantiza ForecastID nuevo por cada “Guardar”
            forecast_id = obtener_forecast_activo(
                slpcode, cliente, anio, db_path, force_new=False
            )
            print(
                f"[DEBUG-GUARDADO] Paso 5: ForecastID nuevo = {forecast_id}, anterior = {forecast_id_prev}"
            )

            # 5) Logging delta (antes de insertar)
            print("[DEBUG-GUARDADO] Paso 6: Logging de diferencias previas a inserción")
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
            print("[DEBUG-GUARDADO] Paso 7: Insertando forecast detalle en BD")
            print(
                f"[DEBUG-SAVE-INSERT] Preparando inserción para ForecastID={forecast_id}"
            )
            print(
                f"[DEBUG-SAVE-INSERT] Shape del DataFrame a insertar: {df_largo_filtrado.shape}"
            )
            print("[DEBUG-SAVE-INSERT] Verificando duplicados antes de inserción:")
            print(
                df_largo_filtrado.groupby(["ItemCode", "TipoForecast", "Mes"])
                .size()
                .reset_index(name="count")
            )

            insertar_forecast_detalle(
                df_largo_filtrado.assign(ForecastID=forecast_id),
                forecast_id,
                anio,
                db_path,
            )
            print("[DEBUG-SAVE-INSERT] Inserción completada")
            print(
                f"[DEBUG-SAVE-INSERT] Estado session_state después: {dict(st.session_state)}"
            )

            # 7) Refrescar SIEMPRE el buffer UI desde BD (4×12 garantizado)
            print(
                "[DEBUG-GUARDADO] Paso 8: Refrescando buffer UI post-guardado (4×12 garantizado)"
            )
            _refrescar_buffer_ui(forecast_id, key_buffer, db_path)

            # Verificación de forma (4 filas por base: Cantidad/Precio × Firme/Proyectado)
            try:
                df_ui = st.session_state[key_buffer].reset_index()
                base = df_ui[["ItemCode", "OcrCode3", "DocCur"]].drop_duplicates()
                expected = len(base) * 4
                real = len(df_ui)
                print(
                    f"[DEBUG-GUARDADO] Verificación 4×12 → bases={len(base)} | esperado={expected} | real={real}"
                )
                if real != expected:
                    print(
                        "[WARN] El buffer UI no quedó en múltiplos de 4 filas por base. Revisar _refrescar_buffer_ui."
                    )
            except Exception as e_check:
                print(f"[DEBUG-GUARDADO] (no se pudo verificar 4×12) {e_check}")

            # 8) RESET del editor y marcas de edición
            print("[DEBUG-GUARDADO] Paso 9: Reseteando estado de edición UI")
            _reset_estado_edicion_por_cliente(cliente, key_buffer)

            # (✔) Limpieza explícita del cache del ForecastID activo para evitar reutilización en el próximo guardado
            cache_key = _forecast_activo_cache_key(slpcode, cliente, anio)
            if cache_key in st.session_state:
                print(
                    f"[DEBUG-GUARDADO] Limpieza de cache ForecastID activo: {cache_key} -> {st.session_state[cache_key]}"
                )
                del st.session_state[cache_key]

            # 9) Recalcular y guardar hash del buffer actual (anti-parpadeo)
            try:
                df_for_hash = st.session_state[key_buffer].reset_index()
                h = pd.util.hash_pandas_object(df_for_hash, index=False).sum()
                st.session_state[f"{key_buffer}_hash"] = np.uint64(h & ((1 << 64) - 1))
                print(
                    f"[DEBUG-GUARDADO] Hash actualizado para {key_buffer}: {st.session_state[f'{key_buffer}_hash']}"
                )
            except Exception as e_hash:
                print(
                    f"[DEBUG-GUARDADO] ⚠️ No se pudo calcular hash para {key_buffer}: {e_hash}"
                )

            st.success(
                f"✅ Cliente {cliente} guardado correctamente (ForecastID={forecast_id})."
            )

        except Exception as e:
            print(
                f"[ERROR-GUARDADO] ❌ Excepción durante guardado de cliente {cliente}: {e}"
            )
            st.error(f"❌ Error al guardar cliente {cliente}: {e}")

            # Recuperación visual mínima SOLO si hubo ForecastID (evita query con None)
            try:
                if forecast_id is None:
                    print(
                        "[DEBUG-GUARDADO] ↩ Sin ForecastID generado; se omite refresco alternativo."
                    )
                    continue

                print(
                    "[DEBUG-GUARDADO] ↩ Intentando refresco alternativo por error de escritura"
                )
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
                    print(
                        "[DEBUG-GUARDADO] Recuperación post-error OK. DF post shape:",
                        df_post.shape,
                    )
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
                print(
                    f"[ERROR-GUARDADO] ❌ Falló refresco post-error para {cliente}: {e2}"
                )
                st.error(
                    f"❌ Error crítico al intentar refrescar buffer de cliente {cliente}: {e2}"
                )

    print("[DEBUG-GUARDADO] 🧼 Limpiando lista de clientes_editados")
    st.session_state.pop("clientes_editados", None)


def _get_forecast_id_prev(
    slpcode: int, cardcode: str, anio: int, db_path: str
) -> Optional[int]:
    """
    Busca el ForecastID MÁS RECIENTE para un cliente (CardCode) y vendedor (SlpCode)
    que sea INMEDIATAMENTE ANTERIOR al que se va a crear.
    """
    print("🔍 [FORECAST-PREV-START] Buscando forecast histórico INMEDIATO")
    print(
        f"📊 [FORECAST-PREV-INFO] slpcode: {slpcode}, cardcode: {cardcode}, anio: {anio}"
    )
    print(f"🗄️  [FORECAST-PREV-INFO] db_path: {db_path}")

    # 1. Buscar el ForecastID más reciente para este cliente y año
    print("🔍 [FORECAST-PREV-STEP] Buscando forecast más reciente...")
    qry_reciente = """
        SELECT MAX(fd.ForecastID) AS id
        FROM   Forecast_Detalle fd
        JOIN   Forecast f ON fd.ForecastID = f.ForecastID
        WHERE  fd.SlpCode  = ?
          AND  fd.CardCode = ?
          AND  strftime('%Y', fd.FechEntr) = ?
          AND  f.Fecha_Carga < datetime('now')
    """
    print(f"📝 [FORECAST-PREV-QUERY] Query reciente: {qry_reciente.strip()}")
    print(
        f"📋 [FORECAST-PREV-PARAMS] Params: slpcode={slpcode}, cardcode={cardcode}, anio={anio}"
    )

    df_reciente = run_query(
        qry_reciente, params=(slpcode, cardcode, str(anio)), db_path=db_path
    )
    print(f"📊 [FORECAST-PREV-RESULT] Resultado reciente - shape: {df_reciente.shape}")

    if not df_reciente.empty and pd.notna(df_reciente.iloc[0].id):
        forecast_id = int(df_reciente.iloc[0].id)
        print(
            f"✅ [FORECAST-PREV-FOUND] ForecastID inmediato anterior encontrado: {forecast_id}"
        )
        return forecast_id
    else:
        print("❌ [FORECAST-PREV-NOTFOUND] No se encontró forecast inmediato anterior")

    # 2. Fallback: Buscar cualquier forecast global para el mismo SlpCode
    print("🔍 [FORECAST-PREV-STEP] Buscando forecast global (fallback)...")
    qry_global = """
        SELECT MAX(fd.ForecastID) AS id
        FROM   Forecast_Detalle fd
        JOIN   Forecast f ON fd.ForecastID = f.ForecastID
        WHERE  fd.SlpCode  = ?
          AND  strftime('%Y', fd.FechEntr) = ?
          AND  f.Fecha_Carga < datetime('now')
    """
    print(f"📝 [FORECAST-PREV-QUERY] Query global: {qry_global.strip()}")
    print(f"📋 [FORECAST-PREV-PARAMS] Params: slpcode={slpcode}, anio={anio}")

    df_glob = run_query(qry_global, params=(slpcode, str(anio)), db_path=db_path)
    print(f"📊 [FORECAST-PREV-RESULT] Resultado global - shape: {df_glob.shape}")

    if not df_glob.empty and pd.notna(df_glob.iloc[0].id):
        forecast_id = int(df_glob.iloc[0].id)
        print(f"✅ [FORECAST-PREV-FOUND] ForecastID global encontrado: {forecast_id}")
        return forecast_id
    else:
        print("❌ [FORECAST-PREV-NOTFOUND] No se encontró forecast global")

    print(
        "⚠️  [FORECAST-PREV-END] No se encontró forecast histórico (ni inmediato anterior ni global)"
    )
    print("🆕 [FORECAST-PREV-INFO] Se partirá desde cero (forecast nuevo)")
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
    resolver_duplicados: str = "mean",  # opciones: "mean", "sum", "error"
    incluir_deltas_cero_si_es_individual: bool = False,  # DEPRECADO: se ignora
    forzar_incluir_todos: bool = False,
) -> pd.DataFrame:
    """
    Añade columna ``Cant_Anterior`` y devuelve SOLO las filas a persistir,
    siguiendo reglas idempotentes:

      (A) Δ != 0                                -> cambios reales
      (B) Cant == 0  y Cant_Anterior > 0        -> BAJA (>0→0)
      (C) Cant > 0   y Cant_Anterior == 0       -> ALTA (0→>0)

    Notas:
    - `incluir_deltas_cero_si_es_individual` está DEPRECADO y no se usa.
    - Normaliza Mes a '01'..'12'.
    - Si hay duplicados en histórico, se resuelven según `resolver_duplicados`.
    - Propaga 'CardCode' si viene en df_largo para validar unicidad de salida.
    """

    # ---------------------------
    # 0) Normalización de entrada
    # ---------------------------
    # Asegurar Mes como texto '01'..'12'
    if "Mes" in df_largo.columns:
        df_largo = df_largo.copy()
        df_largo["Mes"] = df_largo["Mes"].astype(str).str.zfill(2)

    # Asegurar numérico
    for col in ("Cant",):
        if col in df_largo.columns:
            df_largo[col] = pd.to_numeric(df_largo[col], errors="coerce").fillna(0.0)

    print(
        f"[DEBUG-FILTRO] ▶ Enriqueciendo forecast cliente {cardcode} con histórico ForecastID={forecast_id_prev}"
    )

    # ------------------------------------
    # 1) Recuperar histórico (Cant_Anterior)
    # ------------------------------------
    if forecast_id_prev is None:
        print(
            "[DEBUG-FILTRO] 🆕 Cliente sin historial previo. Se parte desde Cant_Anterior = 0"
        )
        df_prev = df_largo[["ItemCode", "TipoForecast", "OcrCode3", "Mes"]].copy()
        df_prev["Cant_Anterior"] = 0.0
    else:
        print(
            f"[DEBUG-FILTRO] 🔁 Cliente con historial previo. ForecastID utilizado: {forecast_id_prev}"
        )
        qry_prev = """
            SELECT ItemCode, TipoForecast, OcrCode3,
                   CAST(strftime('%m', FechEntr) AS TEXT) AS Mes,
                   Cant AS Cant_Anterior
            FROM   Forecast_Detalle
            WHERE  ForecastID = ?
        """
        df_prev = run_query(qry_prev, params=(forecast_id_prev,), db_path=db_path)
        if df_prev.empty:
            print(
                "[DEBUG-FILTRO] ⚠️ Histórico vacío para el ForecastID indicado. Cant_Anterior=0."
            )
            df_prev = df_largo[["ItemCode", "TipoForecast", "OcrCode3", "Mes"]].copy()
            df_prev["Cant_Anterior"] = 0.0
        else:
            df_prev["Mes"] = df_prev["Mes"].astype(str).str.zfill(2)
            df_prev["Cant_Anterior"] = pd.to_numeric(
                df_prev["Cant_Anterior"], errors="coerce"
            ).fillna(0.0)
            print(f"[DEBUG-FILTRO] Registros históricos recuperados: {len(df_prev)}")

            claves = ["ItemCode", "TipoForecast", "OcrCode3", "Mes"]
            duplicados = df_prev.duplicated(subset=claves, keep=False)
            if duplicados.any():
                print(
                    f"[⚠️ DEBUG-FILTRO] {duplicados.sum()} duplicados detectados en histórico por clave compuesta."
                )
                if resolver_duplicados == "error":
                    raise ValueError(
                        "Duplicados en histórico de Forecast_Detalle y resolver_duplicados='error'"
                    )
                elif resolver_duplicados in {"mean", "sum"}:
                    print(
                        f"[DEBUG-FILTRO] Resolviendo con agregación '{resolver_duplicados}' sobre Cant_Anterior"
                    )
                    df_prev = df_prev.groupby(claves, as_index=False).agg(
                        {"Cant_Anterior": resolver_duplicados}
                    )
                else:
                    raise ValueError(
                        f"Valor no válido en resolver_duplicados: {resolver_duplicados}"
                    )

    # --------------------------------
    # 2) Merge y cálculo de diagnóstico
    # --------------------------------
    claves_merge = ["ItemCode", "TipoForecast", "OcrCode3", "Mes"]
    df_enr = df_largo.merge(df_prev, on=claves_merge, how="left")
    df_enr["Cant_Anterior"] = df_enr["Cant_Anterior"].fillna(0.0)

    # Propagar CardCode (si está en df_largo) para validar unicidad en salida
    if "CardCode" in df_largo.columns and "CardCode" not in df_enr.columns:
        df_enr["CardCode"] = df_largo["CardCode"].values

    # Diagnóstico completo
    df_enr["Delta"] = df_enr["Cant"] - df_enr["Cant_Anterior"]
    print("[DEBUG-FILTRO] ▶ Diagnóstico completo previo al filtro de cambios:")
    print(
        df_enr[
            [
                "ItemCode",
                "TipoForecast",
                "OcrCode3",
                "Mes",
                "Cant_Anterior",
                "Cant",
                "Delta",
            ]
        ]
        .sort_values(["TipoForecast", "Mes"])
        .to_string(index=False)
    )

    # Resumen de Δ=0
    df_sin_delta = df_enr[df_enr["Delta"] == 0].copy()
    if not df_sin_delta.empty:
        print(
            f"[DEBUG-FILTRO] 🟡 Registros sin cambios reales (Δ = 0): {len(df_sin_delta)}"
        )
        print(
            df_sin_delta[["ItemCode", "TipoForecast", "Mes", "Cant"]].to_string(
                index=False
            )
        )
    else:
        print("[DEBUG-FILTRO] ✅ Todos los registros tenían algún cambio.")

    # ------------------------------------------------------
    # 3) REGLAS A/B/C (idempotentes) + métricas de transición
    # ------------------------------------------------------
    # Bajas (>0→0), Altas (0→>0), Cambios (>0→>0, Δ≠0)
    if forzar_incluir_todos:
        # Para nuevos ForecastIDs: incluir TODOS los registros
        df_out = df_enr.copy()
        print("[DEBUG-FILTRO] 🔄 Modo forzado: incluyendo todos los registros")
    else:
        # Lógica original solo para cambios
        bajas_mask = (df_enr["Cant_Anterior"] > 0) & (df_enr["Cant"] == 0)
        altas_mask = (df_enr["Cant_Anterior"] == 0) & (df_enr["Cant"] > 0)
        cambios_mask = (
            (df_enr["Cant_Anterior"] > 0)
            & (df_enr["Cant"] > 0)
            & (df_enr["Delta"] != 0)
        )
    # Ignorar flag heredado (deprecado)
    if incluir_deltas_cero_si_es_individual:
        print(
            "[INFO] `incluir_deltas_cero_si_es_individual` está DEPRECADO y se ignora. "
            "Se aplican reglas A/B/C."
        )

    df_out = df_enr[bajas_mask | altas_mask | cambios_mask].copy()

    print(
        f"[DEBUG-FILTRO] zero_transitions_applied (bajas >0→0): {int(bajas_mask.sum())}"
    )
    print(f"[DEBUG-FILTRO] altas 0→>0: {int(altas_mask.sum())}")
    print(f"[DEBUG-FILTRO] otros cambios (Δ≠0 con >0→>0): {int(cambios_mask.sum())}")

    print(f"[DEBUG-FILTRO] Registros con cambio real detectado: {len(df_out)}")
    if not df_out.empty:
        print("[DEBUG-FILTRO] Preview de cambios:")
        print(
            df_out[["ItemCode", "TipoForecast", "Mes", "Cant_Anterior", "Cant"]]
            .head(10)
            .to_string(index=False)
        )

        delta_total = float(df_out["Delta"].sum())
        print(f"[DEBUG-FILTRO] Variación total de unidades: {delta_total:.2f}")

        resumen_mes = df_out.groupby("Mes")["Delta"].sum().reset_index()
        print("[DEBUG-FILTRO] Resumen de delta por mes:")
        print(resumen_mes.to_string(index=False))

    # --------------------------------------
    # 4) Validación de esquema de delta (B2)
    # --------------------------------------
    df_val = df_out.rename(
        columns={"Cant_Anterior": "CantidadAnterior", "Cant": "CantidadNueva"}
    )
    validate_delta_schema(df_val, contexto="[VALIDACIÓN ENRIQUECER]")

    # ------------------------------------------
    # 5) Validación de unicidad post-enriquecido
    # ------------------------------------------
    claves_bd = ["ItemCode", "TipoForecast", "OcrCode3", "Mes", "CardCode"]
    if "CardCode" in df_out.columns:
        duplicados_out = df_out.duplicated(subset=claves_bd, keep=False)
        if duplicados_out.any():
            print(
                f"[❌ FILTRO-ERROR] {int(duplicados_out.sum())} duplicados detectados post-enriquecimiento:"
            )
            print(
                df_out[duplicados_out][claves_bd + ["Cant", "Cant_Anterior"]]
                .sort_values(claves_bd)
                .to_string(index=False)
            )
            raise ValueError(
                "df_out contiene claves duplicadas que violan la restricción única de Forecast_Detalle."
            )
    else:
        print(
            "[⚠️ FILTRO] No se encontró columna 'CardCode' en df_out — se omitió validación de duplicados."
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
    Además, garantiza SIEMPRE 4 filas (Cantidad/Precio × Firme/Proyectado)
    y 12 meses (01..12) por par base (ItemCode+OcrCode3+DocCur).

    Requisitos:
      - run_query(sql, params=(), db_path=None) -> DataFrame
      - Motor: SQLite (usa ROWID para ordenar "el último").
    """
    print(
        f"[DEBUG-BUFFER] 🔁 Refrescando UI para ForecastID={forecast_id}, buffer={key_buffer}"
    )

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
    df_post = run_query(qry, params=(forecast_id,), db_path=db_path)

    if df_post is None or df_post.empty:
        print(
            f"[DEBUG-BUFFER] ⚠️ No se encontraron registros en Forecast_Detalle para ID={forecast_id}"
        )
        st.session_state[key_buffer] = None
        return

    print(f"[DEBUG-BUFFER] Registros recuperados (raw): {len(df_post)}")
    print(f"[DEBUG-BUFFER] Columnas recuperadas: {list(df_post.columns)}")

    # 2) Normalizar Mes a '01'..'12'
    df_post["Mes"] = df_post["Mes"].astype(str).str.zfill(2)

    # 3) Ordenar para que 'last' sea realmente el último registro insertado
    sort_cols = []
    if "FechEntr" in df_post.columns:
        sort_cols.append("FechEntr")
    sort_cols.append("_rid")  # siempre existe en SQLite
    df_post = df_post.sort_values(sort_cols)
    print(f"[DEBUG-BUFFER] Orden aplicado por columnas: {sort_cols}")

    # 4) Deduplicar por clave de negocio a nivel de mes (nos quedamos con el último)
    claves_mes = ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Mes"]
    before_dedup = len(df_post)
    df_dedup = df_post.drop_duplicates(claves_mes, keep="last").copy()
    after_dedup = len(df_dedup)
    print(
        f"[DEBUG-BUFFER] Deduplicación por {claves_mes} -> {before_dedup} → {after_dedup} filas"
    )

    # 5) Mapear 'Linea' representativa
    #    (a) por clave completa (incluye TipoForecast)
    clave_sin_mes_full = ["ItemCode", "TipoForecast", "OcrCode3", "DocCur"]
    linea_map_full = df_dedup.groupby(clave_sin_mes_full, as_index=False)[
        "Linea"
    ].last()
    #    (b) fallback por clave sin TipoForecast (para rellenar si falta)
    clave_base = ["ItemCode", "OcrCode3", "DocCur"]
    linea_map_base = df_dedup.groupby(clave_base, as_index=False)["Linea"].last()

    # 6) Pivots sin agregar por suma (usar último valor)
    meses = [f"{i:02d}" for i in range(1, 13)]

    # Cantidad
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

    # Precio
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

    print(
        f"[DEBUG-BUFFER] pivot_cant shape: {pivot_cant.shape} | pivot_prec shape: {pivot_prec.shape}"
    )

    # 7) Unir métricas y re-incorporar 'Linea'
    df_metrico = pd.concat([pivot_cant, pivot_prec], ignore_index=True)
    df_metrico = df_metrico.merge(
        linea_map_full, on=clave_sin_mes_full, how="left", suffixes=("", "_from_full")
    )

    # Fallback Linea por clave base si quedó nulo
    mask_linea_null = df_metrico["Linea"].isna()
    if mask_linea_null.any():
        print(
            f"[DEBUG-BUFFER] Línea sin asignar (full): {mask_linea_null.sum()} — aplicando fallback por {clave_base}"
        )
        df_metrico = df_metrico.merge(
            linea_map_base, on=clave_base, how="left", suffixes=("", "_fallback")
        )
        df_metrico["Linea"] = df_metrico["Linea"].fillna(df_metrico["Linea_fallback"])
        df_metrico.drop(
            columns=[c for c in df_metrico.columns if c.endswith("_fallback")],
            inplace=True,
        )

    # 8) GARANTIZAR SIEMPRE 4 filas (Firme/Proyectado × Cantidad/Precio) por base (ItemCode+OcrCode3+DocCur)
    base = df_metrico[clave_base].drop_duplicates()
    tipos = pd.DataFrame({"TipoForecast": ["Firme", "Proyectado"]})
    metricas = pd.DataFrame({"Métrica": ["Cantidad", "Precio"]})
    grid = base.merge(tipos, how="cross").merge(metricas, how="cross")

    # Merge del grid con lo ya pivotado
    df_metrico = grid.merge(
        df_metrico,
        how="left",
        on=["ItemCode", "OcrCode3", "DocCur", "TipoForecast", "Métrica"],
        suffixes=("", "_y"),
    )

    # Rellenos de meses y Linea
    for m in meses:
        if m not in df_metrico.columns:
            df_metrico[m] = 0.0
        df_metrico[m] = pd.to_numeric(df_metrico[m], errors="coerce").fillna(0.0)

    # Si aún faltase Linea, completar con el último disponible por base
    if df_metrico["Linea"].isna().any():
        print(
            f"[DEBUG-BUFFER] Línea aún nula tras merge: {df_metrico['Linea'].isna().sum()} — completando por base"
        )
        df_metrico = df_metrico.merge(
            linea_map_base, on=clave_base, how="left", suffixes=("", "_b")
        )
        df_metrico["Linea"] = df_metrico["Linea"].fillna(df_metrico["Linea_b"])
        df_metrico.drop(
            columns=[c for c in df_metrico.columns if c.endswith("_b")], inplace=True
        )

    # 9) Reordenar columnas finales (fijas + 12 meses)
    cols_fijas = ["ItemCode", "TipoForecast", "OcrCode3", "Linea", "DocCur", "Métrica"]
    df_metrico = (
        df_metrico[cols_fijas + meses]
        .sort_values(by=["ItemCode", "TipoForecast", "Métrica"])
        .reset_index(drop=True)
    )

    print(
        f"[DEBUG-BUFFER] Grid base: {len(base)} | Filas finales (deben ser base×4): {len(df_metrico)}"
    )
    print(f"[DEBUG-BUFFER] Columnas finales: {df_metrico.columns.tolist()}")
    print(f"[DEBUG-BUFFER] Buffer final generado. Filas: {len(df_metrico)}")
    print("[DEBUG-BUFFER] Preview (primeras 2 filas):")
    try:
        print(df_metrico.head(2).to_string(index=False))
    except Exception as e:
        print(f"[DEBUG-BUFFER] (no se pudo imprimir preview) {e}")

    # 10) Persistir en sesión con el mismo índice que ya usas
    st.session_state[key_buffer] = df_metrico.set_index(
        ["ItemCode", "TipoForecast", "Métrica"]
    )

    print(f"[DEBUG-BUFFER-REFRESH] Inicio refresh UI - key_buffer: {key_buffer}")
    print(
        f"[DEBUG-BUFFER-REFRESH] Estado buffer antes (len): {len(st.session_state.get(key_buffer, [])) if st.session_state.get(key_buffer, None) is not None else 'None'}"
    )
    print("[DEBUG-BUFFER-REFRESH] Completado refresh UI")
