# B_VIN002: Importaciones fundacionales y dependencias funcionales para ventas
# # ∂B_VIN002/∂B0
import streamlit as st
import pandas as pd
from streamlit import column_config

from config.contexto import obtener_anio  # ∂
from core.consultas_forecast import (
    obtener_clientes,  # ∂B
)

# modulos/ventas.py (arriba, imports)
from core.historico import vista_historico
from core.stock import vista_stock

from modulos.editor_forecast import (
    obtener_forecast_editable,  # ∂B
    inicializar_buffer_cliente,  # ∂B
    sincronizar_buffer_edicion,
    validar_forecast_dataframe,  # ∂B
    sincronizar_buffer_local,  # ∂B
    sincronizar_para_guardado_final,
    guardar_todos_los_clientes_editados,
)

from utils.alertas import (
    render_alertas_forecast,
)
from utils.repositorio_forecast.repositorio_forecast_editor import (
    obtener_buffer_cliente,  # ∂B
)
from utils.db import DB_PATH
from modulos.ventas_facturas_snippet import mostrar_facturas


# ── Helper: detectar excepciones de rerun de Streamlit ───────────────
def _es_rerun(e: BaseException) -> bool:
    """
    Detecta si la excepción corresponde a un rerun/control de flujo de Streamlit.
    Compatible con distintas versiones; incluye fallback por nombre de clase.
    No imprime logs para evitar ruido: el caller decide loguear si corresponde.
    """
    try:
        from streamlit.runtime.scriptrunner import RerunException, RerunData

        if isinstance(e, (RerunException, RerunData)):
            return True
    except Exception:
        # Si no se pudieron importar las clases, seguimos con el fallback por nombre
        pass

    # Fallback robusto por nombre de clase (útil si cambian rutas o versiones)
    cls = e.__class__.__name__
    return cls in ("RerunException", "RerunData", "StopException")


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
    # ── LOG INICIO ────────────────────────────────────────────────────────────────
    print(f"[VISTA.FORECAST.INFO] start slpcode={slpcode} cardcode={cardcode}")

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

    # 2️⃣  ───────────  Validación de query-param / vendedor  ───────────
    slpcode_qs = st.query_params.get("vendedor", slpcode)
    try:
        slpcode = int(slpcode_qs)
        print(f"[VISTA.FORECAST.INFO] vendedor={slpcode}")
    except Exception:
        st.error("Código de vendedor inválido")
        st.stop()

    # 3️⃣  ───────────  Carga inicial de clientes y forecast  ───────────
    clientes = obtener_clientes(slpcode).sort_values("Nombre")
    print(f"[VISTA.FORECAST.INFO] clientes={len(clientes)}")
    if clientes.empty:
        st.info("Este vendedor no tiene clientes activos.")
        st.stop()

    col1, col2 = st.columns([2, 2])
    with col1:
        cardcode = st.selectbox(
            "Cliente:",
            clientes["CardCode"],
            format_func=lambda x: f"{x} - {clientes.loc[clientes['CardCode'] == x, 'Nombre'].values[0]}",
            key="cliente_selectbox",
        )
    print(f"[VISTA.FORECAST.INFO] cardcode seleccionado={cardcode}")

    anio = obtener_anio()
    df_forecast = obtener_forecast_editable(slpcode, cardcode, anio=anio)
    print(f"[VISTA.FORECAST.INFO] forecast shape={df_forecast.shape} anio={anio}")
    if df_forecast.empty:
        st.info("⚠️ Forecast vacío para este cliente/año.")
        st.stop()

    # 4️⃣  ───────────  Buffer de sesión (DataFrame completo)  ───────────
    key_buffer = f"forecast_buffer_{cardcode}"
    if key_buffer not in st.session_state:
        inicializar_buffer_cliente(key_buffer, df_forecast)
        print(
            f"[VISTA.FORECAST.INFO] buffer={key_buffer} action=create shape={df_forecast.shape}"
        )
    else:
        print(
            f"[VISTA.FORECAST.INFO] buffer={key_buffer} action=reuse shape={st.session_state[key_buffer].shape}"
        )

    df_buffer = obtener_buffer_cliente(key_buffer).reset_index()
    df_buffer = sincronizar_buffer_edicion(df_buffer, key_buffer)

    # 5️⃣  ───────────  Merge de Precios (si existe)  ───────────
    if "PrecioUN" in df_buffer.columns:
        print("[VISTA.FORECAST.INFO] precios.detectado=True")
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

    # 6️⃣  ───────────  Filtro de producto (UI)  ───────────
    df_filtrado = (
        df_buffer[df_buffer["ItemCode"] == itemcode_filtro].copy()
        if itemcode_filtro != "Todos"
        else df_buffer.copy()
    )
    print(
        f"[VISTA.FORECAST.INFO] filtro itemcode={itemcode_filtro!r} filas={len(df_filtrado)}"
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

    # 7️⃣  ───────────  Configuración del Editor ───────────
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
            disabled=mes <= 7,  # bloqueo hasta julio
        )

    df_editado = st.data_editor(
        df_filtrado,
        key=f"editor_forecast_{cardcode}",
        use_container_width=True,
        num_rows="fixed",
        height=len(df_filtrado) * 35 + 40,
        column_order=columnas_ordenadas,
        column_config=column_config_forecast,
    )

    # 8️⃣  ───────────  Detección de cambios (staging en sesión) ───────────
    df_actualizado, hay_cambios = sincronizar_buffer_local(df_buffer, df_editado)

    # Hash de control para evitar loops de "cambios"
    hash_key = f"{key_buffer}_hash"
    try:
        hash_actual = hash_df(df_actualizado.sort_index())
    except Exception as e:
        print(f"[VISTA.FORECAST.ERROR] hash_df: {e}")
        hash_actual = 0

    if hash_key not in st.session_state:
        st.session_state[hash_key] = hash_actual

    hash_previo = st.session_state[hash_key]
    hay_nuevos = bool(hay_cambios and hash_actual != hash_previo)
    print(
        f"[VISTA.FORECAST.INFO] cambios_detectados={hay_cambios} hash_changed={hay_nuevos}"
    )

    if hay_nuevos:
        # 9.1 staging en buffer
        st.session_state[key_buffer] = df_actualizado.set_index(
            ["ItemCode", "TipoForecast", "Métrica"]
        )
        # 9.2 marcar cliente editado
        editados = st.session_state.get("clientes_editados", set())
        editados.add(cardcode)
        st.session_state["clientes_editados"] = editados
        # 9.3 actualizar huella
        st.session_state[hash_key] = hash_actual
        # 9.4 pista visual
        st.caption(
            "📝 Cambios en preparación (se guardarán con «💾 Guardar forecast en base de datos»)."
        )
    else:
        st.caption(
            "✏️ Edita y luego usa «💾 Guardar forecast en base de datos» para persistir."
        )

    # 🔟  ───────────  Validación final & guardado  ───────────
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
                print(
                    f"[SAVE.INFO] start cardcode={cardcode} editados={st.session_state.get('clientes_editados', set())}"
                )
                sincronizar_para_guardado_final(
                    key_buffer=key_buffer, df_editado=df_editado
                )
                print(f"[SAVE.INFO] synced key={key_buffer} shape={df_editado.shape}")
                guardar_todos_los_clientes_editados(anio, DB_PATH)
                print("[SAVE.INFO] done")
                print(
                    f"[SAVE.INFO] editados_post={st.session_state.get('clientes_editados', set())}"
                )
            except Exception as e:
                print(f"[SAVE.ERROR] {type(e).__name__}: {e}")
                st.error(f"❌ Error durante el guardado: {e}")


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
    import time

    t0 = time.perf_counter()
    print("[RUN.INFO] start")

    # Origen de SlpCode: querystring o session_state
    slp_qs = st.query_params.get("vendedor")
    slp_ss = st.session_state.get("SlpCode")

    slpcode, source = None, None
    if slp_qs not in (None, ""):
        try:
            slpcode = int(slp_qs)
            source = "query"
        except Exception:
            print(f"[RUN.WARN] vendedor_query_invalido value={slp_qs!r}")
    if slpcode is None and slp_ss not in (None, ""):
        try:
            slpcode = int(slp_ss)
            source = "session"
        except Exception:
            print(f"[RUN.WARN] slpcode_session_invalido value={slp_ss!r}")

    if slpcode is None:
        print("[RUN.WARN] slpcode=None; requiere selección de vendedor")
        st.error("Seleccione un vendedor desde el Home.")
        st.stop()

    # Contexto actual de query params
    qp = st.query_params.to_dict()
    qp_mod, qp_vend = qp.get("modulo"), qp.get("vendedor")
    print(
        f"[RUN.INFO] context slpcode={slpcode} source={source} qp.modulo={qp_mod!r} qp.vendedor={qp_vend!r}"
    )

    # Sincronizar query params solo si difieren
    if qp_mod != "ventas" or str(qp_vend) != str(slpcode):
        print(f"[RUN.REROUTE] query_params.update modulo='ventas' vendedor='{slpcode}'")
        st.query_params.update(modulo="ventas", vendedor=str(slpcode))
        st.rerun()

    try:
        print("[RUN.INFO] tabs.build")
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
            t = time.perf_counter()
            print("[RUN.INFO] tab.enter=forecast")
            vista_forecast(slpcode, None)
            print(f"[RUN.INFO] tab.exit=forecast elapsed={time.perf_counter()-t:.3f}s")

        with tabs[1]:
            t = time.perf_counter()
            print("[RUN.INFO] tab.enter=stock")
            vista_stock(slpcode, None)
            print(f"[RUN.INFO] tab.exit=stock elapsed={time.perf_counter()-t:.3f}s")

        with tabs[2]:
            t = time.perf_counter()
            print("[RUN.INFO] tab.enter=historico")
            vista_historico(slpcode, None)
            print(f"[RUN.INFO] tab.exit=historico elapsed={time.perf_counter()-t:.3f}s")

        with tabs[3]:
            t = time.perf_counter()
            print("[RUN.INFO] tab.enter=ayuda")
            vista_ayuda()
            print(f"[RUN.INFO] tab.exit=ayuda elapsed={time.perf_counter()-t:.3f}s")

        with tabs[4]:
            t = time.perf_counter()
            print("[RUN.INFO] tab.enter=alertas_forecast")
            render_alertas_forecast(slpcode)
            print(
                f"[RUN.INFO] tab.exit=alertas_forecast elapsed={time.perf_counter()-t:.3f}s"
            )

        with tabs[5]:
            t = time.perf_counter()
            print("[RUN.INFO] tab.enter=facturas")
            mostrar_facturas()
            print(f"[RUN.INFO] tab.exit=facturas elapsed={time.perf_counter()-t:.3f}s")

        print(f"[RUN.INFO] end elapsed={time.perf_counter()-t0:.3f}s")

    except BaseException as e:
        # Tratar rerun/stop como controlados (sin ensuciar logs ni UI)
        cls = e.__class__.__name__
        if cls in ("RerunException", "RerunData", "StopException") or _es_rerun(e):
            print(f"[RUN.RERUN] controlado class={cls}")
            raise

        print(f"[RUN.ERROR] {cls}: {e}")
        st.error(f"No se pudo cargar la vista de ventas: {e}")
        st.stop()
