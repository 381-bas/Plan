# B_TAB001: Importaciones principales y configuración de DB para forecast_tablas
# # ∂B_TAB001/∂B0
import streamlit as st
import pandas as pd
from utils.db import (
    run_query,
    _run_tab_select,
    DB_PATH
)




# B_TAB002: Obtención del set de ítems existentes desde OITM
# # ∂B_TAB002/∂B0
def obtener_items_existentes(db_path=DB_PATH):
    query = "SELECT DISTINCT ItemCode FROM OITM"
    df = run_query(query, db_path)
    return set(df["ItemCode"].astype(str).tolist())

# B_TAB003: Consulta y pivoteo de forecast detalle anual para vendedores seleccionados
# # ∂B_TAB003/∂B0
def obtener_forecast_detalle(anio, slpcodes=None):
    query_base = """
        SELECT 
            SlpCode,
            ItemCode,
            TipoForecast,
            OcrCode3,
            strftime('%m', FechEntr) as Mes,
            SUM(Cant) as Total
        FROM Forecast_Detalle
        WHERE strftime('%Y', FechEntr) = ?
    """
    params = [str(anio)]

    if slpcodes:
        placeholders = ','.join(['?'] * len(slpcodes))
        query_base += f" AND SlpCode IN ({placeholders})"
        params.extend(slpcodes)

    query_base += " GROUP BY SlpCode, ItemCode, TipoForecast, OcrCode3, Mes"

    df = run_query(query_base, DB_PATH, tuple(params))

    # Pivot para dejar una columna por mes
    df_pivot = df.pivot_table(
        index=["SlpCode", "ItemCode", "TipoForecast", "OcrCode3"],
        columns="Mes",
        values="Total",
        fill_value=0
    ).reset_index()

    # Asegurar columnas de 1 a 12 como texto
    df_pivot.columns.name = None
    df_pivot = df_pivot.rename(columns={str(i).zfill(2): str(i) for i in range(1, 13)})
    for i in range(1, 13):
        if str(i) not in df_pivot.columns:
            df_pivot[str(i)] = 0

    # Ordenar columnas por mes
    columnas_ordenadas = ["SlpCode", "ItemCode", "TipoForecast", "OcrCode3"] + [str(i) for i in range(1, 13)]
    df_pivot = df_pivot[columnas_ordenadas]

    return df_pivot

# B_TAB004: Visualización agregada y clasificación de forecast por línea y existencia
# # ∂B_TAB004/∂B0
def mostrar_forecast_agregado():
    # Inputs básicos
    anio = st.selectbox("Año:", options=[2025], index=0)

    vendedores_df = _run_tab_select(
        "SELECT DISTINCT SlpCode FROM Forecast_Detalle ORDER BY SlpCode"
    )
    lista_vendedores = vendedores_df["SlpCode"].tolist()

    seleccion_vendedores = st.multiselect("SlpCode (vendedor):", options=lista_vendedores, default=lista_vendedores)

    slpcodes = seleccion_vendedores if seleccion_vendedores else None
    df = obtener_forecast_detalle(anio, slpcodes)

    if df.empty:
        st.warning("⚠️ No se encontraron registros.")
        return

    # Clasificar productos
    items_existentes = obtener_items_existentes()
    df["Item"] = df["ItemCode"].apply(lambda x: "Existente" if x in items_existentes else "Desarrollo")

    # Renombrar campo OcrCode3 → Línea
    df = df.rename(columns={"OcrCode3": "Línea"})

    # Agrupar por Línea, TipoForecast, Item (sin mostrar SlpCode)
    columnas_group = ["Línea", "TipoForecast", "Item"]
    columnas_meses = [str(i) for i in range(1, 13)]
    df_vista = df.groupby(columnas_group)[columnas_meses].sum().reset_index()

    # Agregar fila con total mensual
    totales = df_vista[columnas_meses].sum().to_frame().T
    totales.insert(0, "Item", "Total")
    totales.insert(0, "TipoForecast", "")
    totales.insert(0, "Línea", "")
    df_vista = pd.concat([df_vista, totales], ignore_index=True)

    # Altura dinámica basada en cantidad de filas
    row_height = 35
    max_height = 800
    altura = min(len(df_vista) * row_height + 35, max_height)

    st.dataframe(df_vista, use_container_width=True, height=altura)

# B_TAB005: Ejecutor principal de visualización de forecast por línea y existencia
# # ∂B_TAB005/∂B0
def run():
    tabs = st.tabs(["📦 Forecast por Línea y Existencia"])

    with tabs[0]:
        mostrar_forecast_agregado()

