# B_TRAZ001: Visualización de trazabilidad forecast por cliente y SKU
# # ∂B_TRAZ001/∂B0
import streamlit as st
import pandas as pd

from core.consultas_forecast import (
    obtener_forecast_historico,
    obtener_stock,
    obtener_ordenes_venta,
    obtener_historico_ventas,
)


# B_TRAZ002: Función principal de traza detallada (forecast, stock, OV, ventas)
# # ∂B_TRAZ002/∂B0
def visualizar_traza(slp_code: int, card_code: str):
    st.markdown("### 🔎 Trazabilidad completa por SKU")

    # --- Forecast actual ---
    st.markdown("#### 📊 Forecast actual (por SKU, tipo y línea)")
    df_forecast = obtener_forecast_historico(slp_code, card_code)
    if df_forecast.empty:
        st.info("No hay datos de forecast disponibles para este cliente.")
    else:
        df_forecast["Mes"] = pd.to_datetime(
            df_forecast["FechEntr"], format="%Y-%m-%d", errors="coerce"
        ).dt.month
        tabla = df_forecast.pivot_table(
            index=["ItemCode", "TipoForecast", "OcrCode3"],
            columns="Mes",
            values="Cant",
            aggfunc="sum",
            fill_value=0,
        ).reset_index()
        st.dataframe(tabla, use_container_width=True)

    # --- Stock actual ---
    st.markdown("#### 🏷️ Stock disponible")
    itemcodes = (
        df_forecast["ItemCode"].unique().tolist() if not df_forecast.empty else []
    )
    df_stock = obtener_stock(itemcodes)
    if df_stock.empty:
        st.info("No hay stock asociado a los SKUs del cliente.")
    else:
        st.dataframe(df_stock, use_container_width=True)

    # --- Órdenes de venta ---
    st.markdown("#### 📦 Órdenes de Venta (O/C y C)")
    df_ov = obtener_ordenes_venta(card_code, itemcodes)
    if df_ov.empty:
        st.info("No hay órdenes de venta para este cliente/SKU.")
    else:
        st.dataframe(df_ov, use_container_width=True)

    # --- Ventas históricas ---
    st.markdown("#### 📈 Ventas históricas")
    df_ventas = obtener_historico_ventas(card_code)
    if df_ventas.empty:
        st.info("No hay ventas históricas para este cliente.")
    else:
        st.dataframe(df_ventas, use_container_width=True)
