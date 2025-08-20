# B_SYN001: Importaciones principales y utilidades para sincronización y guardado multicliente
# # ∂B_SYN001/∂B0
import pandas as pd
import numpy as np
import streamlit as st
from typing import Optional

from utils.repositorio_forecast.repositorio_forecast_editor import (
    obtener_buffer_cliente,
    actualizar_buffer_global,
    sincronizar_buffer_local,
)
from utils.transformadores import df_forecast_metrico_to_largo
from utils.db import run_query, DB_PATH
from services.forecast_engine import (
    insertar_forecast_detalle,
    registrar_log_detalle_cambios,
    obtener_forecast_activo,
)
from services.sync import guardar_temp_local  # ∂B49
from config.contexto import obtener_slpcode
from utils.repositorio_forecast.forecast_writer import (
    validate_delta_schema,
    existe_forecast_individual,
)


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
    cliente = key_buffer.replace("forecast_buffer_cliente_", "")
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


# ---------------------------------------------------------------------------
# Helper: enriquecer DF_LARGO con Cant_Anterior y filtrar cambios reales
# ---------------------------------------------------------------------------


def _enriquecer_y_filtrar(
    df_largo: pd.DataFrame,
    forecast_id_prev: Optional[int],
    slpcode: int,
    cardcode: str,
    anio: int,
    db_path: str,
    resolver_duplicados: str = "mean",  # opciones: "mean", "sum", "error"
    incluir_deltas_cero_si_es_individual: bool = False,  # DEPRECADO: se ignora
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
    bajas_mask = (df_enr["Cant_Anterior"] > 0) & (df_enr["Cant"] == 0)
    altas_mask = (df_enr["Cant_Anterior"] == 0) & (df_enr["Cant"] > 0)
    cambios_mask = (
        (df_enr["Cant_Anterior"] > 0) & (df_enr["Cant"] > 0) & (df_enr["Delta"] != 0)
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


# ---------------------------------------------------------------------------
# Helper: refrescar buffer UI después de guardar
# ---------------------------------------------------------------------------


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


# B_SYN003: Guardado estructurado y seguro de buffers editados de todos los clientes
# # ∂B_SYN003/∂B0
def guardar_todos_los_clientes_editados(anio: int, db_path: str = DB_PATH):

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

    for cliente in clientes:
        key_buffer = f"forecast_buffer_cliente_{cliente}"
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

            # 1) Transformación a largo
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

            # 2) Obtener IDs de forecast
            forecast_id = obtener_forecast_activo(
                slpcode, cliente, anio, db_path, force_new=False
            )
            forecast_id_prev = _get_forecast_id_prev(slpcode, cliente, anio, db_path)
            print(
                f"[DEBUG-GUARDADO] Paso 3: ForecastID nuevo = {forecast_id}, anterior = {forecast_id_prev}"
            )

            # 3) Filtrado en base a histórico
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
            )
            print(
                f"[DEBUG-GUARDADO] Paso 4: Cambios reales detectados = {len(df_largo_filtrado)}"
            )
            if df_largo_filtrado.empty:
                print(
                    f"[DEBUG-GUARDADO] ⏩ Sin cambios reales en métricas. Cliente omitido: {cliente}"
                )
                st.info(
                    f"⏩ Cliente {cliente}: sin cambios reales. Se omite inserción."
                )
                # 🔁 RESET suave: aunque no haya cambios, limpiamos estado de edición de este cliente
                _reset_estado_edicion_por_cliente(cliente, key_buffer)
                continue

            # 4) Logging delta
            print("[DEBUG-GUARDADO] Paso 5: Logging de diferencias previas a inserción")
            registrar_log_detalle_cambios(
                slpcode,
                cliente,
                anio,
                df_largo_filtrado.copy(),
                db_path,
                forecast_id=forecast_id,
                forecast_id_anterior=forecast_id_prev,
            )

            # 5) Inserción / upsert
            print("[DEBUG-GUARDADO] Paso 6: Insertando forecast detalle en BD")
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

            # 6) Reconstituir SIEMPRE el buffer UI desde BD (4 filas × 12 meses)
            print(
                "[DEBUG-GUARDADO] Paso 7: Refrescando buffer UI post-guardado (4×12 garantizado)"
            )
            _refrescar_buffer_ui(forecast_id, key_buffer, db_path)

            # ✅ Verificación de forma 4×12 (Cantidad/Precio × Firme/Proyectado) por base
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

            # 7) RESET del editor y marcas de edición (parche A)
            print("[DEBUG-GUARDADO] Paso 8: Reseteando estado de edición UI")
            _reset_estado_edicion_por_cliente(cliente, key_buffer)

            # 8) Recalcular y guardar hash del buffer actual (utilidad anti-pestañeo)
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

            # Intento de recuperación visual mínima
            try:
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
                    # Incluso en recuperación, limpiamos estado de edición para evitar loops
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


def seleccionar_forecast_base(
    slpcode: int, cardcode: str, anio: int, db_path: str
) -> Optional[int]:
    """
    Retorna el ForecastID base más adecuado para enriquecer el forecast actual.

    Orden de prioridad:
    1. Último ForecastID individual del cliente y vendedor para ese año.
    2. Último ForecastID global (sin segmentación por cliente) con datos del cliente.
    3. None si no se encuentra referencia.
    """

    # 1. Buscar ForecastID individual
    sql_indiv = """
        SELECT MAX(ForecastID) as id
        FROM Forecast
        WHERE SlpCode = ?
        AND EXISTS (
            SELECT 1 FROM Forecast_Detalle
            WHERE Forecast_Detalle.ForecastID = Forecast.ForecastID
              AND CardCode = ?
        )
        AND strftime('%Y', date(Fecha_Carga, 'unixepoch')) = ?
    """
    df_indiv = run_query(
        sql_indiv, params=(slpcode, cardcode, str(anio)), db_path=db_path
    )
    if not df_indiv.empty and pd.notna(df_indiv.at[0, "id"]):
        return int(df_indiv.at[0, "id"])

    # 2. Buscar ForecastID global que contenga al cliente
    sql_global = """
        SELECT MAX(ForecastID) as id
        FROM Forecast_Detalle
        WHERE CardCode = ?
    """
    df_global = run_query(sql_global, params=(cardcode,), db_path=db_path)
    if not df_global.empty and pd.notna(df_global.at[0, "id"]):
        return int(df_global.at[0, "id"])

    # 3. No se encontró referencia previa
    return None


def _get_forecast_id_prev(
    slpcode: int, cardcode: str, anio: int, db_path: str
) -> Optional[int]:
    """
    Busca el ForecastID más reciente para un cliente (CardCode) y vendedor (SlpCode).
    Si no existe Forecast individual, intenta buscar uno global (sin CardCode).
    Retorna None si no se encuentra ningún historial.
    """
    print("🔍 [FORECAST-PREV-START] Buscando forecast histórico")
    print(
        f"📊 [FORECAST-PREV-INFO] slpcode: {slpcode}, cardcode: {cardcode}, anio: {anio}"
    )
    print(f"🗄️  [FORECAST-PREV-INFO] db_path: {db_path}")

    # 1. Intento por cliente específico
    print(
        "🔍 [FORECAST-PREV-STEP] Buscando forecast individual (cliente específico)..."
    )
    qry_individual = """
        SELECT MAX(fd.ForecastID) AS id
        FROM   Forecast_Detalle fd
        WHERE  fd.SlpCode  = ?
          AND  fd.CardCode = ?
          AND  strftime('%Y', fd.FechEntr) = ?;
    """
    print(f"📝 [FORECAST-PREV-QUERY] Query individual: {qry_individual.strip()}")
    print(
        f"📋 [FORECAST-PREV-PARAMS] Params: slpcode={slpcode}, cardcode={cardcode}, anio={anio}"
    )

    df_ind = run_query(
        qry_individual, params=(slpcode, cardcode, str(anio)), db_path=db_path
    )
    print(f"📊 [FORECAST-PREV-RESULT] Resultado individual - shape: {df_ind.shape}")

    if not df_ind.empty and pd.notna(df_ind.iloc[0].id):
        forecast_id = int(df_ind.iloc[0].id)
        print(
            f"✅ [FORECAST-PREV-FOUND] ForecastID individual encontrado: {forecast_id}"
        )
        return forecast_id
    else:
        print("❌ [FORECAST-PREV-NOTFOUND] No se encontró forecast individual")

    # 2. Intento fallback: Forecast global para el mismo SlpCode (sin filtrar CardCode)
    print("🔍 [FORECAST-PREV-STEP] Buscando forecast global (fallback)...")
    qry_global = """
        SELECT MAX(fd.ForecastID) AS id
        FROM   Forecast_Detalle fd
        WHERE  fd.SlpCode  = ?
          AND  strftime('%Y', fd.FechEntr) = ?;
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
        "⚠️  [FORECAST-PREV-END] No se encontró forecast histórico (ni individual ni global)"
    )
    print("🆕 [FORECAST-PREV-INFO] Se partirá desde cero (forecast nuevo)")
    return None
