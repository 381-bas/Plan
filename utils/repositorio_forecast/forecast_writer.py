# B_BUF001: Importaciones principales y dependencias del buffer de edición
# # ∂B_BUF001/∂B0
import pandas as pd
import streamlit as st
from utils.repositorio_forecast.repositorio_forecast_editor import get_key_buffer


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
