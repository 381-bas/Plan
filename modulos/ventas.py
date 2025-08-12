# B_VIN001: Inicio simbiótico y dominio SCANNER para ventas.py
# # ∂B_VIN001/∂B0
"""
Este archivo inicia bajo dominio total de SCANNER.
Toda función será estructural, reversible y trazable.
"""

# B_VIN002: Importaciones fundacionales y dependencias funcionales para ventas
# # ∂B_VIN002/∂B0
import streamlit as st
import pandas as pd
from streamlit import column_config

from config.contexto import obtener_anio  # ∂
from core.consultas_forecast import (
    obtener_clientes,  # ∂B
    obtener_forecast_editable,  # ∂B
)
from utils.alertas import (
    render_alertas_forecast,
)
from utils.repositorio_forecast.repositorio_forecast_editor import (
    obtener_buffer_cliente,  # ∂B
    inicializar_buffer_cliente,  # ∂B
    validar_forecast_dataframe,  # ∂B
    sincronizar_buffer_edicion,  # ∂B
    actualizar_buffer_global,  # ∂B
    sincronizar_buffer_local,  # ∂B
)
from utils.utils_buffers import (
    guardar_todos_los_clientes_editados,
    sincronizar_para_guardado_final,
)
from utils.db import DB_PATH
from services.sync import guardar_temp_local
from modulos.ventas_facturas_snippet import mostrar_facturas


# B_HDF001: Normalización profunda de DataFrame para comparación estructural
# # ∂B_HDF001/∂B0
def normalizar_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_index(axis=0).sort_index(axis=1).astype("float64").fillna(0)


# B_HDF002: Generación de hash semántico robusto para buffers editables
# # ∂B_HDF002/∂B0
def hash_df(df: pd.DataFrame) -> int:
    """Hash estable solo de columnas numéricas (ignora texto)."""
    df_num = df.select_dtypes(include=["number"]).copy()
    # Reordena para garantizar consistencia
    df_num = df_num.sort_index(axis=0).sort_index(axis=1)
    return pd.util.hash_pandas_object(df_num, index=True).sum()


# B_VFO001: Editor visual controlado y selección de cliente para forecast editable
# # ∂B_VFO001/∂B0
def vista_forecast(slpcode, cardcode):
    # 1️⃣  ─────────────────────  HEADER UI  ─────────────────────
    st.markdown(
        """
        <style>
            .block-container { padding-top: 4rem !important; }
            .titulo-ajustado  { margin: .5rem 0 1rem; font-size: 1.2rem; font-weight: 500; }
        </style>
        <div class="titulo-ajustado">🧬 Editor Forecast Cantidad / Precio</div>
    """,
        unsafe_allow_html=True,
    )

    # -----------------------------------------------------------------
    # 2️⃣  ───────────  Validación de query-param / vendedor  ───────────
    # (mantenemos compatibilidad con llamada directa por parámetro)
    slpcode_qs = st.query_params.get("vendedor", slpcode)
    try:
        slpcode = int(slpcode_qs)
    except Exception:
        st.error("Código de vendedor inválido")
        st.stop()

    # -----------------------------------------------------------------
    # 3️⃣  ───────────  Carga inicial de clientes y forecast  ───────────
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
        st.info("⚠️ Forecast vacío para este cliente/año.")
        st.stop()

    # Continuar después de obtener df_forecast
    # -----------------------------------------------------------------
    # 4️⃣  ───────────  Buffer de sesión (DataFrame completo)  ───────────
    key_buffer = f"forecast_buffer_cliente_{cardcode}"
    if key_buffer not in st.session_state:
        inicializar_buffer_cliente(key_buffer, df_forecast)

    df_buffer = obtener_buffer_cliente(key_buffer).reset_index()
    df_buffer = sincronizar_buffer_edicion(df_buffer, key_buffer)

    # 5️⃣  ───────────  Merge de Precios (si existe)  ───────────
    if "PrecioUN" in df_buffer.columns:
        precios = (
            df_buffer[df_buffer["Métrica"] == "Precio"]
            .groupby(["ItemCode", "TipoForecast"])["PrecioUN"]
            .first()
            .reset_index()
        )
        df_buffer = df_buffer.drop(columns=["PrecioUN"]).merge(
            precios, on=["ItemCode", "TipoForecast"], how="left"
        )

    with col2:
        itemcode_filtro = st.selectbox(
            "🔍 Filtrar producto:",
            ["Todos"] + sorted(df_buffer["ItemCode"].unique().tolist()),
            key=f"filtro_producto_{cardcode}",
        )

    # -----------------------------------------------------------------
    # 6️⃣  ───────────  Filtro de producto (UI)  ───────────

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
        "Métrica",
    ]
    columnas_ordenadas = campos_fijos + [
        c for c in df_filtrado.columns if c not in campos_fijos
    ]
    df_filtrado = df_filtrado[columnas_ordenadas].sort_values(
        ["ItemCode", "TipoForecast", "Métrica"]
    )

    # -----------------------------------------------------------------
    # 7️⃣  ───────────  Configuración del DataEditor  ───────────
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
        num_rows="fixed",  # "dynamic" para agregar Item´s nuevos a la tabla
        height=len(df_filtrado) * 35 + 40,  # sin límite superior
        column_order=columnas_ordenadas,
        column_config=column_config_forecast,
    )

    # -----------------------------------------------------------------
    # 8️⃣  ───────────  Sincronización y detección de cambios  ───────────
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
    # 9️⃣  ───────────  Manejo de flujo según cambios  ───────────
    if not hay_cambios and hash_actual == hash_previo:
        pass
    else:
        if hay_cambios:
            # 9.1  Actualizar buffer en sesión
            st.session_state[key_buffer] = df_actualizado.set_index(
                ["ItemCode", "TipoForecast", "Métrica"]
            )

            # 9.2  Backup y buffer global
            guardar_temp_local(key_buffer, df_actualizado)
            actualizar_buffer_global(df_actualizado, key_buffer)

            # 9.3  Marcar cliente como editado
            editados = st.session_state.get("clientes_editados", set())
            editados.add(cardcode)
            st.session_state["clientes_editados"] = editados

            st.success("✅ Cambios registrados exitosamente")
            st.session_state[hash_key] = hash_actual
            st.rerun()
        else:
            st.session_state[hash_key] = hash_actual

    # -----------------------------------------------------------------
    # 🔟  ───────────  Validación final & opciones de guardado  ───────────
    try:
        validar_forecast_dataframe(df_editado)
    except Exception as e:
        st.error(f"❌ Error de validación estructural: {e}")
        st.stop()

    hay_editados = bool(st.session_state.get("clientes_editados"))

    if not hay_editados:
        st.button("💾 Guardar forecast en base de datos", disabled=True)
    else:
        if st.button("💾 Guardar forecast en base de datos"):
            try:
                sincronizar_para_guardado_final(
                    key_buffer=key_buffer, df_editado=df_editado
                )
                guardar_todos_los_clientes_editados(anio, DB_PATH)
            except Exception as e:
                st.error(f"❌ Error durante el guardado: {e}")


# B_STK001: Visualización de stock disponible para cliente/usuario
# # ∂B_STK001/∂B0
def vista_stock(slpcode, cardcode):
    st.markdown("### 📦 Stock disponible")
    st.info("Aquí se mostrará el stock actual por SKU.")


# B_HST001: Visualización de ventas históricas para cliente/usuario
# # ∂B_HST001/∂B0
def vista_historico(slpcode, cardcode):
    st.markdown("### 📈 Ventas Históricas")
    st.info("Aquí se mostrarán las ventas.")


# B_AYD001: Visualización de ayuda e instrucciones para el usuario
# # ∂B_AYD001/∂B0
def vista_ayuda():
    st.markdown("### 🧠 Ayuda e Instrucciones")
    st.info(
        """
    - Puedes editar cantidades y precios directamente.
    - Usa los filtros de producto si hay muchos ítems.
    - Verifica que 'Firme' y 'Proyectado' estén bien separados.
    - Al finalizar, guarda los cambios desde el botón inferior.
    """
    )


# B_RUN001: Ejecutor principal de tabs en ventas.py
# # ∂B_RUN001/∂B0
def run():
    slpcode = st.query_params.get("vendedor", 999)
    try:
        slpcode = int(slpcode)
    except Exception as e:
        print(f"[SYMBIOS][ventas] Error en bloque línea 275: {e}")
        raise

    tabs = st.tabs(
        [
            "📋 Forecast",
            "📦 Stock",
            "📈 Histórico",
            "🧠 Ayuda",
            "🚨 Alertas Forecast",
            "📑 Facturas",
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
