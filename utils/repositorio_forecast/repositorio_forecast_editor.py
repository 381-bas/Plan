# B_BUF001: Importaciones principales para gestión y persistencia de buffers de forecast
# # ∂B_BUF001/∂B0
import pandas as pd  # noqa: E402
import streamlit as st
from config.contexto import obtener_mes


# B_BUF002: Generación de clave única de buffer para cliente
# # ∂B_BUF002/∂B0
def get_key_buffer(cliente: str) -> str:
    return f"forecast_buffer_{cliente}"


# B_BUF004: Obtención del DataFrame del buffer de cliente desde sesión
# # ∂B_BUF004/∂B0
def obtener_buffer_cliente(cliente: str) -> pd.DataFrame:
    key = get_key_buffer(cliente)
    df = st.session_state.get(key, pd.DataFrame()).copy()
    df.columns = df.columns.astype(str)
    return df


# B_BUF005: Actualización del buffer completo desde edición del usuario
# # ∂B_BUF005/∂B0
def actualizar_buffer_cliente(cliente: str, df_editado: pd.DataFrame):
    key = get_key_buffer(cliente)
    if key not in st.session_state:
        raise ValueError(
            f"El buffer para el cliente {cliente} no ha sido inicializado."
        )

    buffer_actual = st.session_state[key].copy()
    df_editado = df_editado.copy()
    df_editado.columns = df_editado.columns.astype(str)
    df_editado["ItemCode"] = df_editado["ItemCode"].astype(str).str.strip()
    df_editado["TipoForecast"] = df_editado["TipoForecast"].astype(str).str.strip()
    df_editado_indexed = df_editado.set_index(["ItemCode", "TipoForecast"])

    if not df_editado_indexed.index.equals(buffer_actual.index):
        raise ValueError(
            "Los índices del DataFrame editado no coinciden con el buffer actual."
        )

    columnas_comunes = buffer_actual.columns.intersection(df_editado_indexed.columns)
    buffer_actual.update(df_editado_indexed[columnas_comunes])
    st.session_state[key] = buffer_actual


# B_BUF006: Limpieza del buffer para cliente
# # ∂B_BUF006/∂B0
def limpiar_buffer_cliente(cliente: str):
    key = get_key_buffer(cliente)
    if key in st.session_state:
        del st.session_state[key]


# B_BUF007: Sincronización parcial de edición sobre buffer cliente
# # ∂B_BUF007/∂B0
def sincronizar_edicion_parcial(cliente: str, df_editado_parcial: pd.DataFrame):
    key = get_key_buffer(cliente)
    if key not in st.session_state:
        raise ValueError(
            f"El buffer para el cliente {cliente} no ha sido inicializado."
        )

    buffer = st.session_state[key].copy()
    df_editado_parcial = df_editado_parcial.copy()
    df_editado_parcial.columns = df_editado_parcial.columns.astype(str)

    df_editado_parcial["ItemCode"] = (
        df_editado_parcial["ItemCode"].astype(str).str.strip()
    )
    df_editado_parcial["TipoForecast"] = (
        df_editado_parcial["TipoForecast"].astype(str).str.strip()
    )
    df_editado_parcial = df_editado_parcial.set_index(["ItemCode", "TipoForecast"])

    mes_actual = obtener_mes()
    columnas_objetivo = [str(m) for m in range(mes_actual, 13)] + ["PrecioUN"]

    faltantes = [
        col for col in columnas_objetivo if col not in df_editado_parcial.columns
    ]
    if faltantes:
        raise ValueError(f"Faltan columnas requeridas en edición: {faltantes}")

    for col in buffer.columns.intersection(df_editado_parcial.columns):
        if col in columnas_objetivo:
            buffer[col] = pd.to_numeric(buffer[col], errors="coerce").fillna(0)
            df_editado_parcial[col] = pd.to_numeric(
                df_editado_parcial[col], errors="coerce"
            ).fillna(0)

    for idx in df_editado_parcial.index:
        for col in columnas_objetivo:
            if col in df_editado_parcial.columns:
                buffer.at[idx, col] = df_editado_parcial.at[idx, col]

    st.session_state[key] = buffer
