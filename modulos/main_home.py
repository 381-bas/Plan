# B_CTX001: Importaciones principales y obtención de contexto para el forecast inverso
# # ∂B_CTX001/∂B0
import streamlit as st
<<<<<<< HEAD
from streamlit.runtime.scriptrunner import RerunException, RerunData
from utils.db import (
    _run_home_select,
)
=======
from utils.db import _run_home_select
>>>>>>> 15e7611 (docs(ventas.py): comenta manejo de RerunData y notas B_ROUT001 (sin cambio de lógica))


# B_RUN001: Ejecutor principal – Visualización y navegación de módulos de Quimick
# # ∂B_RUN001/∂B0
def run():

    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1rem !important;
            }
        </style>
    """,
        unsafe_allow_html=True,
    )

    st.title("🧬 Quimick")
    st.caption("Selecciona un módulo para comenzar o edita tu forecast existente")

    # B_UIX001: Renderizado visual de menú principal por módulos
    # # ∂B_UIX001/∂B0
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("#### 📈 Gestión")
        st.markdown("[Ir a Gestión](?modulo=gestion)")

    with col2:
        st.markdown("#### 🏪 Producción")
        st.markdown("[Ir a Producción](?modulo=produccion)")

    with col3:
        st.markdown("#### ⚙️ Admin / admi_panel")
        st.markdown("[Panel Admin](?modulo=admi_panel)")

    with col4:
        st.markdown("#### 🧠 Núcleo Control")
        st.markdown("[Ir a Control](?modulo=nucleo_control)")

    st.divider()

    # B_UIX002: Visualización y acceso a forecast por vendedor desde SQL
    # # ∂B_UIX002/∂B0
    st.subheader("🗓️ Forecast cargado por cliente")

    # --- PATCH: try/except SOLO para el query a BD (no incluye botones ni rerun)
    query = """
        SELECT DISTINCT f.SlpCode, o.SlpName
        FROM Forecast f
        JOIN OSLP o ON f.SlpCode = o.SlpCode
        ORDER BY f.SlpCode
    """
    try:
        df_vendedores = _run_home_select(query)
    except Exception as e:
<<<<<<< HEAD
        try:
            if isinstance(e, (RerunException, RerunData)):
                raise
        except Exception:
            if e.__class__.__name__ in ("RerunException", "RerunData"):
                raise
        st.error(f"❌ Error en inicio: {e}")
        st.stop()
=======
        st.warning(f"❌ No se pudo cargar la lista de vendedores con forecast: {e}")
        return

    if df_vendedores is None or df_vendedores.empty:
        st.info("No hay vendedores con forecast.")
        return

    # --- Fuera del try: botones que navegan a 'ventas' y hacen rerun sin ser atrapados
    for _, row in df_vendedores.iterrows():
        col1, col2 = st.columns([1, 5])
        with col1:
            if st.button("✍️ Ir", key=f"btn_{row['SlpCode']}"):
                st.query_params.update(
                    modulo="ventas", vendedor=str(int(row["SlpCode"]))
                )
                st.rerun()
        with col2:
            st.markdown(f"**{int(row['SlpCode'])} – {row['SlpName']}**")
>>>>>>> 15e7611 (docs(ventas.py): comenta manejo de RerunData y notas B_ROUT001 (sin cambio de lógica))
