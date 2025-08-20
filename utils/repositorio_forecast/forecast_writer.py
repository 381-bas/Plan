# B_BUF001: Importaciones principales y dependencias del buffer de edición
# # ∂B_BUF001/∂B0
import pandas as pd
import streamlit as st
from utils.repositorio_forecast.repositorio_forecast_editor import get_key_buffer
from utils.db import run_query


# B_BUF002: Detección robusta de cambios entre edición nueva y buffer actual
# # ∂B_BUF002/∂B0
def detectar_cambios_buffer(cliente: str, df_nuevo: pd.DataFrame) -> bool:
    key = get_key_buffer(cliente)
    if key not in st.session_state:
        return True  # Considerar como cambio si no hay buffer previo

    df_nuevo = df_nuevo.copy()
    df_nuevo.columns = df_nuevo.columns.astype(str)
    df_nuevo["ItemCode"] = df_nuevo["ItemCode"].astype(str).str.strip()
    df_nuevo["TipoForecast"] = df_nuevo["TipoForecast"].astype(str).str.strip()

    buffer_actual = st.session_state[key].copy()

    # 🧠 Ajuste clave: alinear índice con el buffer real si incluye 'Métrica'
    if "Métrica" in df_nuevo.columns and "Métrica" in buffer_actual.index.names:
        df_nuevo["Métrica"] = df_nuevo["Métrica"].astype(str).str.strip()
        df_nuevo_indexed = df_nuevo.set_index(["ItemCode", "TipoForecast", "Métrica"])
    else:
        df_nuevo_indexed = df_nuevo.set_index(["ItemCode", "TipoForecast"])

    # Detectar columnas de mes (01..12) y columna de precio válida
    columnas_mes = [col for col in df_nuevo_indexed.columns if col.isdigit()]
    columna_precio = "PrecioUN" if "PrecioUN" in df_nuevo_indexed.columns else None
    columnas_comparar = columnas_mes + ([columna_precio] if columna_precio else [])

    # Asegurar que todas las columnas existan en ambos
    columnas_comunes = [
        col
        for col in columnas_comparar
        if col in df_nuevo_indexed.columns and col in buffer_actual.columns
    ]
    if not columnas_comunes:
        return True  # No hay columnas comparables

    # Conversión numérica defensiva
    df_nuevo_indexed[columnas_comunes] = (
        df_nuevo_indexed[columnas_comunes]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
    )
    buffer_actual[columnas_comunes] = (
        buffer_actual[columnas_comunes].apply(pd.to_numeric, errors="coerce").fillna(0)
    )

    # Comparar solo intersección de índices y columnas
    indices_comunes = buffer_actual.index.intersection(df_nuevo_indexed.index)

    df_a = buffer_actual.loc[indices_comunes, columnas_comunes].sort_index()
    df_b = df_nuevo_indexed.loc[indices_comunes, columnas_comunes].sort_index()

    return not df_a.equals(df_b)


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
