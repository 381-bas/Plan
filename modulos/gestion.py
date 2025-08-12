# B_CTX001: Importaciones principales del módulo de Gestión Comercial y dependencias core
# # ∂B_CTX001/∂B0
import streamlit as st
import pandas as pd
from datetime import datetime
from core.consultas_forecast import (
    DB_PATH,
    obtener_forecast_mes,
    obtener_ventas_mes,
    obtener_nombre_vendedor,
    obtener_precios_unitarios
)
from components.traza import visualizar_traza
from utils.db import (
    _run_gestion_select
)



# B_RUN001: Función principal run() para gestión y visualización comercial
# # ∂B_RUN001/∂B0
def run():
    st.markdown("#### 📊 Módulo de Gestión Comercial")

    # B_PRM001: Parámetros de análisis - selección de mes y año
    # # ∂B_PRM001/∂B0
    st.sidebar.header("🗓️ Parámetros de análisis")
    mes_analisis = st.sidebar.selectbox(
        "Mes de análisis (forecast vigente):",
        options=[f"{m:02d}" for m in range(1, 13)],
        index=datetime.now().month - 1
    )
    anio_actual = datetime.now().year
    mes_anterior = f"{int(mes_analisis) - 1:02d}" if int(mes_analisis) > 1 else "12"
    anio_mes_anterior = anio_actual if mes_analisis != "01" else anio_actual - 1

    # B_PRM002: Parámetros de filtrado lateral - vendedores, clientes, líneas
    # # ∂B_PRM002/∂B0
    st.sidebar.markdown("---")
    df_vendedores = obtener_nombre_vendedor(DB_PATH)
    vendedores = df_vendedores.sort_values("SlpCode")["SlpName"].dropna().unique().tolist()
    clientes_filtrados = _run_gestion_select(
        "SELECT DISTINCT CardCode FROM Forecast_Detalle"
    )["CardCode"].tolist()


    # Actualizado: líneas disponibles por OcrCode3
    ocrcode3_disponibles = ["Trd-Alim", "Trd-Farm", "Trd-Cosm", "Pta-Nutr", "Pta-Alim"]

    vendedor_filtro = st.sidebar.selectbox("Filtrar por Vendedor:", options=["Todos"] + vendedores)
    cliente_filtro = st.sidebar.selectbox("Filtrar por Cliente:", options=["Todos"] + clientes_filtrados)
    ocrcode3_filtro = st.sidebar.selectbox("Filtrar por Línea de Negocio (OcrCode3):", options=["Todas"] + ocrcode3_disponibles)

    # B_UIX001: Configuración de tabs para visualización de datos
    # # ∂B_UIX001/∂B0
    tab1, tab2, tab3, tab4 = st.tabs(["📋 Resumen Ejecutivo", "📈 Detalle Cliente/SKU", "📦 Stock y OV", "🔎 Traza por SKU"])

    # B_UIX002: Tab 1 – Resumen ejecutivo ventas por vendedor y línea
    # # ∂B_UIX002/∂B0
    with tab1:
        st.subheader("👤 Ventas por Vendedor y Línea de Negocio")
        try:
            df_forecast = obtener_forecast_mes(DB_PATH, anio_actual, int(mes_analisis))
            df_ventas = obtener_ventas_mes(DB_PATH, anio_mes_anterior, int(mes_anterior))
            nombres_vendedores = obtener_nombre_vendedor(DB_PATH)
            df_precios = obtener_precios_unitarios(DB_PATH)

            if df_forecast.empty and df_ventas.empty:
                st.info("No hay datos disponibles para el mes seleccionado.")
                return

            df_forecast = df_forecast.merge(df_precios, on="ItemCode", how="left")
            df_forecast["PrecioUnitario"] = df_forecast["PrecioUnitario"].fillna(0)
            df_forecast["Total"] = df_forecast["Total"].fillna(0)
            df_forecast["Monto"] = df_forecast["Total"] * df_forecast["PrecioUnitario"]

            if vendedor_filtro != "Todos":
                slpcode_sel = nombres_vendedores[nombres_vendedores["SlpName"] == vendedor_filtro]["SlpCode"].values[0]
                df_forecast = df_forecast[df_forecast["SlpCode"] == slpcode_sel]
                df_ventas = df_ventas[df_ventas["SlpCode"] == slpcode_sel]

            if cliente_filtro != "Todos":
                df_forecast = df_forecast[df_forecast["CardCode"] == cliente_filtro]
                df_ventas = df_ventas[df_ventas["CardCode"] == cliente_filtro]

            if ocrcode3_filtro != "Todas":
                df_forecast = df_forecast[df_forecast["OcrCode3"] == ocrcode3_filtro]
                df_ventas = df_ventas[df_ventas["OcrCode3"] == ocrcode3_filtro]


            resumen_vendedor = df_forecast.groupby("SlpCode")["Monto"].sum().reset_index()
            resumen_vendedor.columns = ["SlpCode", "Venta Mes Actual"]

            ventas_vendedor = df_ventas.groupby("SlpCode")["Total"].sum().reset_index()
            ventas_vendedor.columns = ["SlpCode", "Venta Mes Anterior"]

            tabla_vendedor = pd.merge(resumen_vendedor, ventas_vendedor, on="SlpCode", how="outer").fillna(0)
            tabla_vendedor = tabla_vendedor[(tabla_vendedor["Venta Mes Actual"] != 0) | (tabla_vendedor["Venta Mes Anterior"] != 0)]
            tabla_vendedor = tabla_vendedor.merge(nombres_vendedores, on="SlpCode", how="left")
            tabla_vendedor = tabla_vendedor.rename(columns={"SlpName": "Vendedor"})
            tabla_vendedor["Diferencia"] = tabla_vendedor["Venta Mes Actual"] - tabla_vendedor["Venta Mes Anterior"]

            for col in ["Venta Mes Actual", "Venta Mes Anterior", "Diferencia"]:
                tabla_vendedor[col] = tabla_vendedor[col].apply(lambda x: f"{round(x / 1000, 1):,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))

            tabla_vendedor = tabla_vendedor[["Vendedor", "Venta Mes Actual", "Venta Mes Anterior", "Diferencia"]]

            resumen_linea = df_forecast.groupby("OcrCode3")["Monto"].sum().reset_index()
            resumen_linea.columns = ["OcrCode3", "Venta Mes Actual"]

            ventas_linea = df_ventas.groupby("OcrCode3")["Total"].sum().reset_index()
            ventas_linea.columns = ["OcrCode3", "Venta Mes Anterior"]

            tabla_linea = pd.merge(resumen_linea, ventas_linea, on="OcrCode3", how="outer").fillna(0)
            tabla_linea = tabla_linea[(tabla_linea["Venta Mes Actual"] != 0) | (tabla_linea["Venta Mes Anterior"] != 0)]
            tabla_linea["Diferencia"] = tabla_linea["Venta Mes Actual"] - tabla_linea["Venta Mes Anterior"]

            for col in ["Venta Mes Actual", "Venta Mes Anterior", "Diferencia"]:
                tabla_linea[col] = tabla_linea[col].apply(lambda x: f"{round(x / 1000, 1):,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))

            tabla_linea["OcrCode3"] = tabla_linea["OcrCode3"].fillna("-")
            tabla_linea = tabla_linea[["OcrCode3", "Venta Mes Actual", "Venta Mes Anterior", "Diferencia"]]

            col1, col2 = st.columns([1, 1])
            with col1:
                st.markdown("#### 📋 Ventas por Vendedor")
                st.dataframe(tabla_vendedor, use_container_width=True, height=260)
            with col2:
                st.markdown("#### 🏷️ Ventas por Línea de Negocio")
                st.dataframe(tabla_linea, use_container_width=True, height=260)

        except Exception as e:
            st.error(f"❌ Error al procesar los datos: {e}")

    # B_UIX003: Tab 2 – Ventas detalladas por cliente y SKU
    # # ∂B_UIX003/∂B0
    with tab2:
        st.markdown("#### 🧾 Ventas por Cliente y SKU")
        resumen_cliente = df_forecast.groupby(["CardCode", "ItemCode"])["Monto"].sum().reset_index()
        resumen_cliente.columns = ["CardCode", "ItemCode", "Venta Mes Actual"]

        ventas_cliente = df_ventas.groupby(["CardCode", "ItemCode"])["Total"].sum().reset_index()
        ventas_cliente.columns = ["CardCode", "ItemCode", "Venta Mes Anterior"]

        tabla_cliente = pd.merge(resumen_cliente, ventas_cliente, on=["CardCode", "ItemCode"], how="outer").fillna(0)
        tabla_cliente["Diferencia"] = tabla_cliente["Venta Mes Actual"] - tabla_cliente["Venta Mes Anterior"]

        try:
            nombres_clientes = _run_gestion_select("SELECT CardCode, CardName FROM OCRD")
            nombres_productos = _run_gestion_select("SELECT ItemCode, ItemName FROM OITM")

            tabla_cliente = tabla_cliente.merge(nombres_clientes, on="CardCode", how="left")
            tabla_cliente = tabla_cliente.merge(nombres_productos, on="ItemCode", how="left")
        except Exception as e:
            st.warning(f"⚠️ No se pudieron cargar nombres descriptivos: {e}")

        columnas_orden = ["CardCode", "CardName", "ItemCode", "ItemName", "Venta Mes Actual", "Venta Mes Anterior", "Diferencia"]
        tabla_cliente = tabla_cliente[columnas_orden]

        clientes = tabla_cliente["CardCode"].unique()
        for cliente in sorted(clientes):
            df_cliente = tabla_cliente[tabla_cliente["CardCode"] == cliente].copy()
            nombre_cliente = df_cliente["CardName"].iloc[0] if pd.notnull(df_cliente["CardName"].iloc[0]) else cliente
            total_actual = df_cliente["Venta Mes Actual"].sum()
            total_anterior = df_cliente["Venta Mes Anterior"].sum()
            total_diferencia = total_actual - total_anterior

            def formato_miles(valor):
                return f"{round(valor / 1000, 1):,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")

            total_actual_str = formato_miles(total_actual)
            total_anterior_str = formato_miles(total_anterior)
            total_diferencia_str = formato_miles(total_diferencia)
            diferencia_display = f"❗ {total_diferencia_str}" if total_diferencia < 0 else total_diferencia_str

            titulo = f"📦 Cliente: {cliente} - {nombre_cliente} (Actual: {total_actual_str} | Anterior: {total_anterior_str} | Dif: {diferencia_display})"
            with st.expander(titulo):
                df_view = df_cliente.drop(columns=["CardCode", "CardName"])
                for col in ["Venta Mes Actual", "Venta Mes Anterior", "Diferencia"]:
                    df_view[col] = df_view[col].apply(lambda x: f"{round(x / 1000, 1):,.1f}".replace(",", "X").replace(".", ",").replace("X", "."))
                df_view["Diferencia"] = df_view["Diferencia"].apply(lambda x: f"❗ {x}" if "-" in x else x)

                st.dataframe(df_view, use_container_width=True)

    # B_UIX004: Tab 4 – Vista de Traza para desarrollo
    # # ∂B_UIX004/∂B0
    with tab4:
        st.markdown("#### 🔎 Vista de Traza para Desarrollo")
        try:
            visualizar_traza(slp_code=999, card_code="SIM999")
        except Exception as e:
            st.warning(f"No se pudo cargar traza de prueba: {e}")
