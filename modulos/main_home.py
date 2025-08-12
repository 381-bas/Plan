# B_CTX001: Importaciones principales y obtención de contexto para el forecast inverso
# # ∂B_CTX001/∂B0
import streamlit as st
import pandas as pd
from config.contexto import obtener_anio
from utils.db import (
    _run_home_select,
)    




# B_RUN001: Ejecutor principal – Visualización y navegación de módulos de Quimick
# # ∂B_RUN001/∂B0
def run():
    
    st.markdown("""
        <style>
            .block-container {
                padding-top: 1rem !important;
            }
        </style>
    """, unsafe_allow_html=True)
    
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

 
    anio = obtener_anio()

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
                if st.button("✍️ Ir", key=f"btn_{row['SlpCode']}"):
                    st.query_params.update(modulo="ventas", vendedor=row['SlpCode'])
                    st.rerun()
            with col2:
                st.markdown(f"**{row['SlpCode']} – {row['SlpName']}**")

    except Exception as e:
        st.warning(f"❌ No se pudo cargar la lista de vendedores con forecast: {e}")
