# B_VIN001: Inicio simbiÃ³tico y dominio SCANNER para ventas.py
# # âˆ‚B_VIN001/âˆ‚B0
"""
Este archivo inicia bajo dominio total de SCANNER.
Toda funciÃ³n serÃ¡ estructural, reversible y trazable.
"""

# B_VIN002: Importaciones fundacionales y dependencias funcionales para ventas
# # âˆ‚B_VIN002/âˆ‚B0
import streamlit as st
import pandas as pd
from streamlit import column_config

from config.contexto import obtener_anio  # âˆ‚
from core.consultas_forecast import (
    obtener_clientes,  # âˆ‚B
    obtener_forecast_editable,  # âˆ‚B
)
from utils.alertas import (
    render_alertas_forecast,
)
from utils.repositorio_forecast.repositorio_forecast_editor import (
    obtener_buffer_cliente,  # âˆ‚B
    inicializar_buffer_cliente,  # âˆ‚B
    validar_forecast_dataframe,  # âˆ‚B
    sincronizar_buffer_edicion,  # âˆ‚B
    actualizar_buffer_global,  # âˆ‚B
    sincronizar_buffer_local,  # âˆ‚B
)
from utils.utils_buffers import (
    guardar_todos_los_clientes_editados,
    sincronizar_para_guardado_final,
)
from utils.db import DB_PATH
from services.sync import guardar_temp_local
from modulos.ventas_facturas_snippet import mostrar_facturas


# B_HDF001: NormalizaciÃ³n profunda de DataFrame para comparaciÃ³n estructural
# # âˆ‚B_HDF001/âˆ‚B0
def normalizar_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_index(axis=0).sort_index(axis=1).astype("float64").fillna(0)


# B_HDF002: GeneraciÃ³n de hash semÃ¡ntico robusto para buffers editables
# # âˆ‚B_HDF002/âˆ‚B0
def hash_df(df: pd.DataFrame) -> int:
    """Hash estable solo de columnas numÃ©ricas (ignora texto)."""
    df_num = df.select_dtypes(include=["number"]).copy()
    # Reordena para garantizar consistencia
    df_num = df_num.sort_index(axis=0).sort_index(axis=1)
    return pd.util.hash_pandas_object(df_num, index=True).sum()


# B_VFO001: Editor visual controlado y selecciÃ³n de cliente para forecast editable
# # âˆ‚B_VFO001/âˆ‚B0
def vista_forecast(slpcode, cardcode):
    # 1ï¸âƒ£  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  HEADER UI  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    st.markdown(
        """
        <style>
            .block-container { padding-top: 4rem !important; }
            .titulo-ajustado  { margin: .5rem 0 1rem; font-size: 1.2rem; font-weight: 500; }
        </style>
        <div class="titulo-ajustado">ðŸ§¬ Editor Forecast Cantidad / Precio</div>
    """,
        unsafe_allow_html=True,
    )

    # -----------------------------------------------------------------
    # 2ï¸âƒ£  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  ValidaciÃ³n de query-param / vendedor  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # (mantenemos compatibilidad con llamada directa por parÃ¡metro)
    slpcode_qs = st.query_params.get("vendedor", slpcode)
    try:
        slpcode = int(slpcode_qs)
    except Exception:
        st.error("CÃ³digo de vendedor invÃ¡lido")
        st.stop()

    # -----------------------------------------------------------------
    # 3ï¸âƒ£  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  Carga inicial de clientes y forecast  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    clientes = obtener_clientes(slpcode).sort_values("Nombre")
    if clientes.empty:
        st.info("Este vendedor no tiene clientes activos.")
        st.stop()

    # NUEVO: Filtros horizontales Cliente + Producto
    col1, col2 = st.columns([2, 2])

    with col1:
        cardcode = st.selectbox(
            "Cliente:",
            clientes["CardCode"],
            format_func=lambda x: f"{x} - {clientes.loc[clientes['CardCode'] == x, 'Nombre'].values[0]}",
            key="cliente_selectbox",
        )

    anio = obtener_anio()
    df_forecast = obtener_forecast_editable(slpcode, cardcode, anio=anio)
    if df_forecast.empty:
        st.info("âš ï¸ Forecast vacÃ­o para este cliente/aÃ±o.")
        st.stop()

    # Continuar despuÃ©s de obtener df_forecast
    # -----------------------------------------------------------------
    # 4ï¸âƒ£  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  Buffer de sesiÃ³n (DataFrame completo)  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    key_buffer = f"forecast_buffer_cliente_{cardcode}"
    if key_buffer not in st.session_state:
        inicializar_buffer_cliente(key_buffer, df_forecast)

    df_buffer = obtener_buffer_cliente(key_buffer).reset_index()
    df_buffer = sincronizar_buffer_edicion(df_buffer, key_buffer)

    # 5ï¸âƒ£  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  Merge de Precios (si existe)  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if "PrecioUN" in df_buffer.columns:
        precios = (
            df_buffer[df_buffer["MÃ©trica"] == "Precio"]
            .groupby(["ItemCode", "TipoForecast"])["PrecioUN"]
            .first()
            .reset_index()
        )
        df_buffer = df_buffer.drop(columns=["PrecioUN"]).merge(
            precios, on=["ItemCode", "TipoForecast"], how="left"
        )

    with col2:
        itemcode_filtro = st.selectbox(
            "ðŸ” Filtrar producto:",
            ["Todos"] + sorted(df_buffer["ItemCode"].unique().tolist()),
            key=f"filtro_producto_{cardcode}",
        )

    # -----------------------------------------------------------------
    # 6ï¸âƒ£  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  Filtro de producto (UI)  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    df_filtrado = (
        df_buffer[df_buffer["ItemCode"] == itemcode_filtro].copy()
        if itemcode_filtro != "Todos"
        else df_buffer.copy()
    )

    campos_fijos = [
        "ItemCode",
        "ItemName",
        "TipoForecast",
        "OcrCode3",
        "DocCur",
        "MÃ©trica",
    ]
    columnas_ordenadas = campos_fijos + [
        c for c in df_filtrado.columns if c not in campos_fijos
    ]
    df_filtrado = df_filtrado[columnas_ordenadas].sort_values(
        ["ItemCode", "TipoForecast", "MÃ©trica"]
    )

    # -----------------------------------------------------------------
    # 7ï¸âƒ£  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  ConfiguraciÃ³n del DataEditor  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    column_config_forecast = {
        "ItemCode": column_config.TextColumn(label="Cod"),
        "TipoForecast": column_config.TextColumn(label="Tipo"),
        "OcrCode3": column_config.TextColumn(label="Linea"),
        "DocCur": column_config.TextColumn(label="$"),
    }
    for mes in range(1, 13):
        col = f"{mes:02d}"
        column_config_forecast[col] = column_config.NumberColumn(
            label=col,
            disabled=mes <= 6,  # bloqueo hasta junio
        )

    df_editado = st.data_editor(
        df_filtrado,
        key=f"editor_forecast_{cardcode}",
        use_container_width=True,
        num_rows="fixed",  # "dynamic" para agregar ItemÂ´s nuevos a la tabla
        height=len(df_filtrado) * 35 + 40,  # sin lÃ­mite superior
        column_order=columnas_ordenadas,
        column_config=column_config_forecast,
    )

    # -----------------------------------------------------------------
    # 8ï¸âƒ£  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  SincronizaciÃ³n y detecciÃ³n de cambios  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # ~~Se eliminan validaciones dupes y hashes inconsistentes~~
    df_actualizado, hay_cambios = sincronizar_buffer_local(df_buffer, df_editado)

    print(f"[DEBUG-VISTA] Cliente actual: {cardcode}")
    print(f"[DEBUG-VISTA] hay_cambios_real: {hay_cambios}")

    hash_key = f"{key_buffer}_hash"
    try:
        hash_actual = hash_df(df_actualizado.sort_index())
    except Exception as e:
        print(f"[ERROR-VISTA] No se pudo calcular hash_actual: {e}")
        hash_actual = 0
    hash_previo = st.session_state.get(hash_key)

    # -----------------------------------------------------------------
    # 9ï¸âƒ£  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  Manejo de flujo segÃºn cambios  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if not hay_cambios and hash_actual == hash_previo:
        pass
    else:
        if hay_cambios:
            # 9.1  Actualizar buffer en sesiÃ³n
            st.session_state[key_buffer] = df_actualizado.set_index(
                ["ItemCode", "TipoForecast", "MÃ©trica"]
            )

            # 9.2  Backup y buffer global
            guardar_temp_local(key_buffer, df_actualizado)
            actualizar_buffer_global(df_actualizado, key_buffer)

            # 9.3  Marcar cliente como editado
            editados = st.session_state.get("clientes_editados", set())
            editados.add(cardcode)
            st.session_state["clientes_editados"] = editados

            st.success("âœ… Cambios registrados exitosamente")
            st.session_state[hash_key] = hash_actual
            st.rerun()
        else:
            st.session_state[hash_key] = hash_actual

    # -----------------------------------------------------------------
    # ðŸ”Ÿ  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€  ValidaciÃ³n final & opciones de guardado  â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        validar_forecast_dataframe(df_editado)
    except Exception as e:
        st.error(f"âŒ Error de validaciÃ³n estructural: {e}")
        st.stop()

    hay_editados = bool(st.session_state.get("clientes_editados"))

    if not hay_editados:
        st.button("ðŸ’¾ Guardar forecast en base de datos", disabled=True)
    else:
        if st.button("ðŸ’¾ Guardar forecast en base de datos"):
            try:
                sincronizar_para_guardado_final(
                    key_buffer=key_buffer, df_editado=df_editado
                )
                guardar_todos_los_clientes_editados(anio, DB_PATH)
            except Exception as e:
                st.error(f"âŒ Error durante el guardado: {e}")


# B_STK001: VisualizaciÃ³n de stock disponible para cliente/usuario
# # âˆ‚B_STK001/âˆ‚B0
def vista_stock(slpcode, cardcode):
    st.markdown("### ðŸ“¦ Stock disponible")
    st.info("AquÃ­ se mostrarÃ¡ el stock actual por SKU.")


# B_HST001: VisualizaciÃ³n de ventas histÃ³ricas para cliente/usuario
# # âˆ‚B_HST001/âˆ‚B0
def vista_historico(slpcode, cardcode):
    st.markdown("### ðŸ“ˆ Ventas HistÃ³ricas")
    st.info("AquÃ­ se mostrarÃ¡n las ventas.")


# B_AYD001: VisualizaciÃ³n de ayuda e instrucciones para el usuario
# # âˆ‚B_AYD001/âˆ‚B0
def vista_ayuda():
    st.markdown("### ðŸ§  Ayuda e Instrucciones")
    st.info(
        """
    - Puedes editar cantidades y precios directamente.
    - Usa los filtros de producto si hay muchos Ã­tems.
    - Verifica que 'Firme' y 'Proyectado' estÃ©n bien separados.
    - Al finalizar, guarda los cambios desde el botÃ³n inferior.
    """
    )


# B_RUN001: Ejecutor principal de tabs en ventas.py
# # âˆ‚B_RUN001/âˆ‚B0
def run():
    slpcode = st.query_params.get("vendedor", 999)
    try:
        slpcode = int(slpcode)
    except Exception as e:
        print(f"[SYMBIOS][ventas] Error en bloque lÃ­nea 275: {e}")
        raise

    tabs = st.tabs(
        [
            "ðŸ“‹ Forecast",
            "ðŸ“¦ Stock",
            "ðŸ“ˆ HistÃ³rico",
            "ðŸ§  Ayuda",
            "ðŸš¨ Alertas Forecast",
            "ðŸ“‘ Facturas",
        ]
    )

    with tabs[0]:
        vista_forecast(slpcode, None)
    with tabs[1]:
        vista_stock(slpcode, None)
    with tabs[2]:
        vista_historico(slpcode, None)
    with tabs[3]:
        vista_ayuda()
    with tabs[4]:
        render_alertas_forecast(slpcode)
    with tabs[5]:
        mostrar_facturas()
