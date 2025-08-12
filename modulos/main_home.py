# B_CTX001: Importaciones principales y obtenciÃ³n de contexto para el forecast inverso
# # âˆ‚B_CTX001/âˆ‚B0
import streamlit as st

from utils.db import (
    _run_home_select,
)


# B_RUN001: Ejecutor principal â€“ VisualizaciÃ³n y navegaciÃ³n de mÃ³dulos de Quimick
# # âˆ‚B_RUN001/âˆ‚B0
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

    st.title("ðŸ§¬ Quimick")
    st.caption("Selecciona un mÃ³dulo para comenzar o edita tu forecast existente")

    # B_UIX001: Renderizado visual de menÃº principal por mÃ³dulos
    # # âˆ‚B_UIX001/âˆ‚B0
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("#### ðŸ“ˆ GestiÃ³n")
        st.markdown("[Ir a GestiÃ³n](?modulo=gestion)")

    with col2:
        st.markdown("#### ðŸª ProducciÃ³n")
        st.markdown("[Ir a ProducciÃ³n](?modulo=produccion)")

    with col3:
        st.markdown("#### âš™ï¸ Admin / admi_panel")
        st.markdown("[Panel Admin](?modulo=admi_panel)")

    with col4:
        st.markdown("#### ðŸ§  NÃºcleo Control")
        st.markdown("[Ir a Control](?modulo=nucleo_control)")

    st.divider()

    # B_UIX002: VisualizaciÃ³n y acceso a forecast por vendedor desde SQL
    # # âˆ‚B_UIX002/âˆ‚B0
    st.subheader("ðŸ—“ï¸ Forecast cargado por cliente")

    try:
        query = """
            SELECT DISTINCT f.SlpCode, o.SlpName
            FROM Forecast f
            JOIN OSLP o ON f.SlpCode = o.SlpCode
            ORDER BY f.SlpCode
        """
        df_vendedores = _run_home_select(query)

        for _, row in df_vendedores.iterrows():
            col1, col2 = st.columns([1, 5])
            with col1:
                if st.button("âœï¸ Ir", key=f"btn_{row['SlpCode']}"):
                    st.query_params.update(modulo="ventas", vendedor=row["SlpCode"])
                    st.rerun()
            with col2:
                st.markdown(f"**{row['SlpCode']} â€“ {row['SlpName']}**")

    except Exception as e:
        st.warning(f"âŒ No se pudo cargar la lista de vendedores con forecast: {e}")
