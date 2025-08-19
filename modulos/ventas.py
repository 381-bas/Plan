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
    validar_forecast_dataframe,  # ∂B
    sincronizar_buffer_edicion,  # ∂B
    sincronizar_buffer_local,  # ∂B
)
from utils.utils_buffers import (
    guardar_todos_los_clientes_editados,
    sincronizar_para_guardado_final,
)
from utils.db import DB_PATH
from modulos.ventas_facturas_snippet import mostrar_facturas


<<<<<<< HEAD
# ── Helper: detectar excepciones de rerun de Streamlit ───────────────
def _es_rerun(e: Exception) -> bool:
    try:
        # Compat con distintas versiones de Streamlit
        from streamlit.runtime.scriptrunner import RerunException, RerunData

        return isinstance(e, (RerunException, RerunData))
    except Exception:
        return False
=======
# --- PATCH A: Bootstrap de sesión para el editor (idempotente)
def _ensure_session_keys(key_buffer: str, df_source=None):
    import pandas as pd

    MESES = [f"{m:02d}" for m in range(1, 13)]
    cols_base = [
        "ItemCode",
        "ItemName",
        "TipoForecast",
        "OcrCode3",
        "DocCur",
        "Métrica",
    ]
    # MODIFICACIÓN: Cambiar el orden de los índices a ['TipoForecast', 'Métrica', 'ItemCode']
    skeleton = pd.DataFrame(columns=cols_base + MESES).set_index(
        ["TipoForecast", "Métrica", "ItemCode"]
    )

    print(f"[DEBUG-ENSURE] key_buffer: {key_buffer}")  # ADDED
    print(f"[DEBUG-ENSURE] df_source is None: {df_source is None}")  # ADDED

    # Crear buffer base si no existe
    if key_buffer not in st.session_state:
        if (
            df_source is not None
            and hasattr(df_source, "empty")
            and not df_source.empty
        ):
            print("[DEBUG-ENSURE] df_source no es None ni está vacío")  # ADDED
            df = df_source.copy()
            print(f"[DEBUG-ENSURE] df_source cols: {df.columns.tolist()}")  # ADDED
            # Garantizar meses 01..12
            for m in MESES:
                if m not in df.columns:
                    df[m] = 0.0
            # Si faltan claves mínimas, usar esqueleto
            if not {"ItemCode", "TipoForecast", "Métrica"}.issubset(df.columns):
                print("[DEBUG-ENSURE] Faltan claves mínimas, usando esqueleto")  # ADDED
                st.session_state[key_buffer] = skeleton.copy()
                print(f"[DEBUG-ENSURE] skeleton index: {skeleton.index.names}")  # ADDED
            else:
                print("[DEBUG-ENSURE] Claves mínimas presentes")  # ADDED
                # MODIFICACIÓN: Cambiar el orden de los índices a ['TipoForecast', 'Métrica', 'ItemCode']
                df = df.set_index(["TipoForecast", "Métrica", "ItemCode"])
                print(f"[DEBUG-ENSURE] df index: {df.index.names}")  # ADDED
                st.session_state[key_buffer] = df
        else:
            print(
                "[DEBUG-ENSURE] df_source es None o está vacío, usando esqueleto"
            )  # ADDED
            st.session_state[key_buffer] = skeleton.copy()
            print(f"[DEBUG-ENSURE] skeleton index: {skeleton.index.names}")  # ADDED

    # Buffers derivados (siempre planos)
    base = st.session_state[key_buffer]
    if isinstance(base.index, pd.MultiIndex) or base.index.names != [None]:
        base = base.reset_index()

    st.session_state.setdefault(f"{key_buffer}_editado", base.copy())
    st.session_state.setdefault(f"{key_buffer}_prev", base.copy())

    # Soporte para bandera de refresco post-guardado (no obliga)
    st.session_state.setdefault(f"__fresh_from_db__{key_buffer}", False)

    # Conjunto de editados global (evita KeyError)
    st.session_state.setdefault("clientes_editados", set())
>>>>>>> 15e7611 (docs(ventas.py): comenta manejo de RerunData y notas B_ROUT001 (sin cambio de lógica))


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
    import pandas as pd

    MESES = [f"{m:02d}" for m in range(1, 13)]

    # 1) Header
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

    # 2) Validación vendedor
    slpcode_qs = st.query_params.get("vendedor", slpcode)
    try:
        slpcode = int(slpcode_qs)
    except Exception:
        st.error("Código de vendedor inválido")
        st.stop()
    st.session_state.setdefault("clientes_editados", set())

    # 3) Carga clientes + forecast
    clientes = obtener_clientes(slpcode).sort_values("Nombre")
    if clientes.empty:
        st.info("Este vendedor no tiene clientes activos.")
        st.stop()

    col1, col2 = st.columns([2, 2])
    with col1:
        cardcode = st.selectbox(
            "Cliente:",
            clientes["CardCode"],
            format_func=lambda x: f"{x} - {clientes.loc[clientes['CardCode']==x,'Nombre'].values[0]}",
            key="cliente_selectbox",
        )

    anio = obtener_anio()
    df_forecast = obtener_forecast_editable(slpcode, cardcode, anio=anio)
    if df_forecast.empty:
        st.info("⚠️ Forecast vacío para este cliente/año.")
        st.stop()

    # 4) Buffer de sesión
    key_buffer = f"forecast_buffer_cliente_{cardcode}"
    _ensure_session_keys(key_buffer, df_source=df_forecast)
    df_buffer = st.session_state[key_buffer]
    # salida plana, por si acaso
    if isinstance(
        df_buffer.index, (pd.MultiIndex, pd.Index)
    ) and df_buffer.index.names != [None]:
        df_buffer = df_buffer.reset_index()

    # Defensas de meses
    for m in MESES:
        if m not in df_buffer.columns:
            df_buffer[m] = 0.0

    # Sincronizar con editado
    df_buffer = sincronizar_buffer_edicion(df_buffer, key_buffer)
    if isinstance(
        df_buffer.index, (pd.MultiIndex, pd.Index)
    ) and df_buffer.index.names != [None]:
        df_buffer = df_buffer.reset_index()

    with col2:
        itemcode_filtro = st.selectbox(
            "🔍 Filtrar producto:",
            ["Todos"] + sorted(df_buffer["ItemCode"].astype(str).unique().tolist()),
            key=f"filtro_producto_{cardcode}",
        )

    # 5) Filtro y orden
    df_filtrado = (
        df_buffer[df_buffer["ItemCode"].astype(str) == str(itemcode_filtro)].copy()
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
    extras = [c for c in df_filtrado.columns if c not in campos_fijos]
    columnas_ordenadas = campos_fijos + extras

    df_filtrado = df_filtrado[columnas_ordenadas].sort_values(
        ["ItemCode", "TipoForecast", "Métrica"]
    )

<<<<<<< HEAD
    # -----------------------------------------------------------------
    # 7️⃣  ───────────  Configuración del Editor (sin autosave/persist) ───────────
=======
    # 6) Editor
>>>>>>> 15e7611 (docs(ventas.py): comenta manejo de RerunData y notas B_ROUT001 (sin cambio de lógica))
    column_config_forecast = {
        "ItemCode": column_config.TextColumn(label="Cod"),
        "TipoForecast": column_config.TextColumn(label="Tipo"),
        "OcrCode3": column_config.TextColumn(label="Linea"),
        "DocCur": column_config.TextColumn(label="$"),
        "Métrica": column_config.TextColumn(label="Métrica"),
    }
    for mes in range(1, 13):
        col = f"{mes:02d}"
        column_config_forecast[col] = column_config.NumberColumn(
            label=col,
            disabled=mes <= 6,  # si aplica tu política
        )

    df_editado = st.data_editor(
        df_filtrado,
        key=f"editor_forecast_{cardcode}",
        use_container_width=True,
<<<<<<< HEAD
        num_rows="fixed",  # "dynamic" si en el futuro habilitas agregar filas
        height=len(df_filtrado) * 35 + 40,  # ajusta si lo prefieres
=======
        num_rows="fixed",
        height=len(df_filtrado) * 35 + 40 if len(df_filtrado) > 0 else 200,
>>>>>>> 15e7611 (docs(ventas.py): comenta manejo de RerunData y notas B_ROUT001 (sin cambio de lógica))
        column_order=columnas_ordenadas,
        column_config=column_config_forecast,
    )

<<<<<<< HEAD
    # -----------------------------------------------------------------
    # 8️⃣  ───────────  Detección de cambios (solo staging en sesión) ───────────
=======
    # 7) Detección de cambios + guardado temporal/flag
>>>>>>> 15e7611 (docs(ventas.py): comenta manejo de RerunData y notas B_ROUT001 (sin cambio de lógica))
    df_actualizado, hay_cambios = sincronizar_buffer_local(df_buffer, df_editado)

    hash_key = f"{key_buffer}_hash"
    try:
        hash_actual = hash_df(df_actualizado.sort_index())
    except Exception:
        hash_actual = 0

<<<<<<< HEAD
    if hash_key not in st.session_state:
        # Primera huella: evita marcar “cambios” al cargar por primera vez
        st.session_state[hash_key] = hash_actual

    hash_previo = st.session_state[hash_key]
    hay_nuevos = bool(hay_cambios and hash_actual != hash_previo)

    # Si hay diferencias reales, SOLO "etapear" en memoria (sin backup ni éxito)
    if hay_nuevos:
        # 9.1  Actualizar buffer de sesión (staging)
        st.session_state[key_buffer] = df_actualizado.set_index(
            ["ItemCode", "TipoForecast", "Métrica"]
        )

        # 9.2  Marcar cliente como “editado” para el guardado final multi-cliente
        editados = st.session_state.get("clientes_editados", set())
        editados.add(cardcode)
        st.session_state["clientes_editados"] = editados

        # 9.3  Actualizar huella para evitar loop de detección
        st.session_state[hash_key] = hash_actual

        # 9.4  Señal sutil de estado (sin éxito/persistencia)
        st.caption(
            "📝 Cambios en preparación (se guardarán con «💾 Guardar forecast en base de datos»)."
        )
    else:
        # Si no hay nuevos cambios, puedes opcionalmente mostrar la selección actual
        st.caption(
            "✏️ Edita y luego usa «💾 Guardar forecast en base de datos» para persistir."
        )

    # -----------------------------------------------------------------
    # 🔟  ───────────  Validación final & opciones de guardado  ───────────
=======
    if hay_cambios or hash_actual != hash_previo:
        # defensas de claves/meses
        for col, default in [
            ("ItemCode", ""),
            ("TipoForecast", ""),
            ("Métrica", "Cantidad"),
        ]:
            if col not in df_actualizado.columns:
                df_actualizado[col] = default
        for m in MESES:
            if m not in df_actualizado.columns:
                df_actualizado[m] = 0.0

        st.session_state[key_buffer] = df_actualizado.set_index(
            ["ItemCode", "TipoForecast", "Métrica"]
        )
        guardar_temp_local(key_buffer, df_actualizado)
        actualizar_buffer_global(df_actualizado, key_buffer)

        editados = st.session_state.get("clientes_editados", set())
        editados.add(cardcode)
        st.session_state["clientes_editados"] = editados
        st.success("✅ Cambios registrados exitosamente")
        st.session_state[hash_key] = hash_actual
        st.rerun()

    # 8) Validaciones + botón guardar
>>>>>>> 15e7611 (docs(ventas.py): comenta manejo de RerunData y notas B_ROUT001 (sin cambio de lógica))
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

    try:
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

<<<<<<< HEAD
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
    except Exception as e:
        # Evita mostrar Rerun/Redirect internos como error de usuario
        if _es_rerun(e):
            raise
        st.error(f"❌ No se pudo cargar la lista de vendedores con forecast: {e}")
        st.stop()
=======
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


# Agregar esta función de diagnóstico
def _debug_dataframe_state(df, label=""):
    """Función auxiliar para diagnóstico completo del estado de un DataFrame"""
    print(f"\n[DEEP-DEBUG] {label} {'='*50}")
    print(f"Tipo de DataFrame: {type(df)}")
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"Index type: {type(df.index)}")
    if isinstance(df.index, pd.MultiIndex):
        print(f"Index names: {df.index.names}")
        print(
            f"Index levels: {[list(df.index.get_level_values(i)) for i in range(df.index.nlevels)]}"
        )
    print(f"First few rows:\n{df.head()}\n")
    print("=" * 70)
>>>>>>> 15e7611 (docs(ventas.py): comenta manejo de RerunData y notas B_ROUT001 (sin cambio de lógica))
