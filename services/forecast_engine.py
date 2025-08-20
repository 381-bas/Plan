# B_FEN001: Importaciones y carga base de forecast_engine
# # ∂B_FEN001/∂B0


import pandas as pd
import logging
from datetime import datetime  # noqa: E402,F811
import os
import streamlit as st
from utils.db import (
    _run_forecast_write,
    _run_log_to_sql,
    _run_forecast_insert_get_id,
    DB_PATH,
    run_query,
)
from utils.repositorio_forecast.forecast_writer import validate_delta_schema
from hashlib import sha256


# Configuración básica del logger local
logger = logging.getLogger(__name__)
if not logger.handlers:
    _hdl = logging.StreamHandler()
    _fmt = logging.Formatter("[%(levelname)s] %(name)s: %(message)s")
    _hdl.setFormatter(_fmt)
    logger.addHandler(_hdl)
logger.setLevel(logging.INFO)
if os.getenv("DEBUG_IMPORTS"):
    print("📍 forecast_engine.py LOADED desde:", __file__)


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
