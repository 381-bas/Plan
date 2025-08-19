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
    slpcode_qs = st.query_params.get("vendedor", slpcode)
    try:
        slpcode = int(slpcode_qs)
    except Exception:
        st.error("Código de vendedor inválido")
        st.stop()

    # Evitar KeyError más adelante
    st.session_state.setdefault("clientes_editados", set())

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
    print("[DEBUG-VISTA] df_forecast cols:", df_forecast.columns.tolist())
    if df_forecast.empty:
        st.info("⚠️ Forecast vacío para este cliente/año.")
        st.stop()

    # -----------------------------------------------------------------
    # 4️⃣  ───────────  Buffer de sesión (DataFrame completo)  ───────────
    key_buffer = f"forecast_buffer_cliente_{cardcode}"

    # Después de obtener df_forecast
    print("[DEBUG-VISTA] Estado inicial de df_forecast:")
    _debug_dataframe_state(df_forecast, "FORECAST INICIAL")

    # Antes de _ensure_session_keys
    print("[DEBUG-VISTA] Antes de _ensure_session_keys")

    _ensure_session_keys(key_buffer, df_source=df_forecast)

    print("[DEBUG-VISTA] Estado después de _ensure_session_keys:")
    _debug_dataframe_state(st.session_state[key_buffer], "BUFFER POST ENSURE")

    # Obtener buffer
    df_buffer = st.session_state[key_buffer]
    print("[DEBUG-VISTA] Estado antes de sincronizar:")
    _debug_dataframe_state(df_buffer, "PRE-SYNC")

    # Antes de sincronizar
    print("[DEBUG-VISTA] Preparando sincronización...")
    print(
        f"[DEBUG-VISTA] Columnas disponibles en índice: {[n for n in df_buffer.index.names if n is not None]}"
    )
    print("[DEBUG-VISTA] Valores únicos por nivel de índice:")
    for idx_name in df_buffer.index.names:
        if idx_name:
            print(
                f"- {idx_name}: {df_buffer.index.get_level_values(idx_name).unique().tolist()}"
            )

    # Llamar a sincronizar_buffer_edicion
    df_buffer = sincronizar_buffer_edicion(df_buffer, key_buffer)

    # 🔒 Garantía de salida plana (claves como columnas)
    if isinstance(
        df_buffer.index, (pd.MultiIndex, pd.Index)
    ) and df_buffer.index.names != [None]:
        df_buffer = df_buffer.reset_index()

    print("[DEBUG-VISTA] Estado después de sincronizar:")
    _debug_dataframe_state(df_buffer, "POST-SYNC")

    # -----------------------------------------------------------------
    # 5️⃣  ───────────  (REMOVIDO) Merge de Precios por 'Métrica'  ──────
    # ❌ Antes: agrupaba df_buffer[df_buffer["Métrica"]=="Precio"] y re-mezclaba PrecioUN
    # ✅ Ahora: conservar PrecioUN/DocCur como vienen por fila; defaults defensivos
    if "PrecioUN" in df_buffer.columns:
        df_buffer["PrecioUN"] = df_buffer["PrecioUN"].fillna(0.0)
    if "DocCur" in df_buffer.columns:
        df_buffer["DocCur"] = df_buffer["DocCur"].fillna("CLP")

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

    # Insertar PrecioUN (si existe) después de DocCur, en solo lectura
    extras = [c for c in df_filtrado.columns if c not in campos_fijos + ["PrecioUN"]]
    columnas_ordenadas = (
        campos_fijos
        + (["PrecioUN"] if "PrecioUN" in df_filtrado.columns else [])
        + extras
    )

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
    if "PrecioUN" in df_filtrado.columns:
        column_config_forecast["PrecioUN"] = column_config.NumberColumn(
            label="PrecioUN", disabled=True
        )

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
        height=len(df_filtrado) * 35 + 40 if len(df_filtrado) > 0 else 200,
        column_order=columnas_ordenadas,
        column_config=column_config_forecast,
    )

    # -----------------------------------------------------------------
    # 8️⃣  ───────────  Sincronización y detección de cambios  ───────────
    df_actualizado, hay_cambios = sincronizar_buffer_local(df_buffer, df_editado)

    print("[DEBUG-VISTA] df_actualizado cols:", df_actualizado.columns.tolist())

    # --- RECUPERAR CLAVES SI ESTÁN EN EL ÍNDICE ---
    try:
        idx_names = []
        if isinstance(df_actualizado.index, pd.MultiIndex):
            idx_names = [n or "" for n in df_actualizado.index.names]
        elif df_actualizado.index.name:
            idx_names = [df_actualizado.index.name]

        required = {"ItemCode", "TipoForecast", "Métrica"}
        cols_set = set(df_actualizado.columns)

        if not required.issubset(cols_set):
            if required.issubset(set(idx_names)):
                print(
                    "[DEBUG-VISTA] Recuperando claves desde el índice -> reset_index()"
                )
                df_actualizado = df_actualizado.reset_index()
            else:
                print(
                    "[DEBUG-VISTA] Claves ausentes, se intentan tomar desde df_buffer/por default"
                )
                for c, default in [
                    ("ItemCode", ""),
                    ("TipoForecast", ""),
                    ("Métrica", "Cantidad"),
                ]:
                    if c not in df_actualizado.columns:
                        if c in df_buffer.columns and len(df_buffer) == len(
                            df_actualizado
                        ):
                            df_actualizado[c] = df_buffer[c].values
                        else:
                            df_actualizado[c] = default

        # Asegurar meses 01..12 (si algo los removió)
        MESES = [f"{m:02d}" for m in range(1, 13)]
        for m in MESES:
            if m not in df_actualizado.columns:
                df_actualizado[m] = 0.0

        print(
            "[DEBUG-VISTA] (post-recover) df_actualizado cols:",
            df_actualizado.columns.tolist(),
        )
        if (
            isinstance(df_actualizado.index, pd.MultiIndex)
            or df_actualizado.index.name is not None
        ):
            print("[DEBUG-VISTA] (post-recover) reset_index por índice no vacío")
            df_actualizado = df_actualizado.reset_index()
    except Exception as e:
        print(f"[DEBUG-VISTA] Error en recuperación de claves: {e}")

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
            # --- DEFENSAS ANTES DE set_index ---
            # 1) Columnas clave: garantizarlas
            for col, default in [
                ("ItemCode", ""),
                ("TipoForecast", ""),
                ("Métrica", "Cantidad"),
            ]:
                if col not in df_actualizado.columns:
                    df_actualizado[col] = default

            # 2) Asegurar meses 01..12 (por si el sincronizador quitó alguno)
            MESES = [f"{m:02d}" for m in range(1, 13)]
            for m in MESES:
                if m not in df_actualizado.columns:
                    df_actualizado[m] = 0.0

            # 3) Tipos esperados mínimos (evita floats raros en claves)
            df_actualizado["ItemCode"] = df_actualizado["ItemCode"].astype(str)
            df_actualizado["TipoForecast"] = df_actualizado["TipoForecast"].astype(str)
            df_actualizado["Métrica"] = df_actualizado["Métrica"].astype(str)

            try:
                print(
                    "[DEBUG-VISTA] index names antes de set_index:",
                    getattr(df_actualizado.index, "names", df_actualizado.index.name),
                )
            except Exception as e:
                print("[DEBUG-VISTA] (no index names) err:", e)

            # 4) Finalmente, indexar en el orden correcto
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
