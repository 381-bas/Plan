# B_BUF001: Importaciones principales para gestión y persistencia de buffers de forecast
# # ∂B_BUF001/∂B0
import pandas as pd  # noqa: E402
import streamlit as st


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


# B_BUF006: Limpieza del buffer para cliente
# # ∂B_BUF006/∂B0
def limpiar_buffer_cliente(cliente: str):
    key = get_key_buffer(cliente)
    if key in st.session_state:
        del st.session_state[key]
