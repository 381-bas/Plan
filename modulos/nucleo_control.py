# B_CTX001: Importaciones principales, utilidades y configuración de entorno para núcleo de control
# # ∂B_CTX001/∂B0
import streamlit as st
from utils.logs.log_operativo import registrar_log_accion
from core import forecast_tablas
from utils.db import (
    _run_product_select,
    _run_product_insert,
    _run_client_select,
    _run_client_insert,
    _run_vendor_select,
    _run_vendor_insert,
    _run_reasig_select,
    _duplicar_forecast_reasignacion,
)
from services.snapshot_schema import (
    actualizar_snapshot_realidad,
    generar_snapshot_completo,
)


# B_RUN001: Función principal run() – núcleo de control con tabs de gestión, reasignación y visualización
# # ∂B_RUN001/∂B0
def run():
    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 2rem !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    tabs = st.tabs(
        [
            "📋 Crear / Visualizar Registros",
            "🔁 Reasignar Forecast Comercial",
            "📊 Tablas Forecast",
            "📸 Snapshot",
        ]
    )

    # B_UIX001: Tab de creación y visualización de productos, clientes y vendedores
    # # ∂B_UIX001/∂B0
    with tabs[0]:
        mostrar_tab_crear_y_ver()

    # B_UIX002: Tab de reasignación de forecast comercial
    # # ∂B_UIX002/∂B0
    with tabs[1]:
        mostrar_tab_reasignacion()

    # B_UIX003: Tab para visualización de tablas forecast
    # # ∂B_UIX003/∂B0
    with tabs[2]:
        forecast_tablas.run()

    # Agrega una nueva pestaña al final de tu lista de tabs
    with tabs[3]:
        st.header("📸 Snapshot Forecast vs Realidad")

        col1, col2 = st.columns([1, 2])
        with col1:
            if st.button("📸 Generar Snapshot Forecast"):
                df_no_forecast = generar_snapshot_completo()

                if not df_no_forecast.empty:
                    st.warning(
                        f"⚠️ {len(df_no_forecast)} clientes con ventas no tienen forecast:"
                    )
                    st.dataframe(df_no_forecast, use_container_width=True, height=250)
                else:
                    st.success(
                        "✅ Todos los clientes con ventas tienen forecast asignado."
                    )

                st.success("Snapshot generado desde Forecast.")

        with col2:
            if st.button("📊 Actualizar con Ventas Reales"):
                actualizar_snapshot_realidad()
                st.success("Actualización con datos de OINV completada.")


# B_MNT001: Crear y ver productos, clientes y vendedores en núcleo de control
# # ∂B_MNT001/∂B0
def mostrar_tab_crear_y_ver():

    # B_MNT002: Gestión de productos – creación y visualización
    # # ∂B_MNT002/∂B0
    with st.expander("📦 Productos – Crear / Ver"):
        productos_df = _run_product_select("SELECT * FROM OITM ORDER BY ItemCode")

        with st.form("form_crear_producto"):
            st.markdown(" ➕ Crear nuevo producto")
            itemcode = st.text_input("Código Producto (ItemCode)")
            itemname = st.text_input("Nombre del Producto")

            itmsgrpnam = st.selectbox(
                "Línea de Negocio",
                ["Trd-Alim", "Trd-Farm", "Trd-Cosm", "Pta-Nutr", "Pta-Alim"],
            )

            submitted = st.form_submit_button("Guardar Producto")

            if submitted:
                if itemcode in productos_df["ItemCode"].values:
                    st.warning("❌ Este ItemCode ya existe.")
                elif not itemcode or not itemname:
                    st.warning("❗ Todos los campos son obligatorios.")
                else:
                    try:
                        _run_product_insert(
                            "INSERT INTO OITM (ItemCode, ItemName, ItmsGrpNam, validFor) VALUES (?, ?, ?, 'Y')",
                            (itemcode, itemname, itmsgrpnam),
                        )
                        st.success("✅ Producto creado exitosamente")
                        # refrescar listado
                        productos_df = _run_product_select(
                            "SELECT * FROM OITM ORDER BY ItemCode"
                        )
                    except Exception as e:
                        st.error(f"Error al guardar: {e}")

        st.markdown("### 📋 Productos existentes")
        st.dataframe(productos_df, use_container_width=True, height=250)

    # B_MNT003: Gestión de clientes – creación y visualización
    # # ∂B_MNT003/∂B0
    with st.expander("🏪 Clientes – Crear / Ver"):

        # --- SELECT iniciales usando wrapper ---------------------------------
        clientes = _run_client_select("SELECT * FROM OCRD ORDER BY CardCode")
        vendedores = _run_client_select(
            "SELECT SlpCode, SlpName FROM OSLP ORDER BY SlpCode"
        )
        # ---------------------------------------------------------------------

        with st.form("form_crear_cliente"):
            st.markdown("➕ Crear nuevo cliente")
            cardcode = st.text_input("Código Cliente (CardCode)")
            cardname = st.text_input("Nombre del Cliente")
            cardtype = st.selectbox(
                "Tipo de cliente (CardType)",
                options=["C", "S", "L"],
                format_func=lambda x: {"C": "Cliente", "S": "Proveedor", "L": "Lead"}[
                    x
                ],
            )
            slpcode = st.selectbox(
                "Vendedor asignado (SlpCode)",
                vendedores["SlpCode"],
                format_func=lambda x: f"{x} – {vendedores[vendedores['SlpCode'] == x]['SlpName'].values[0]}",
            )
            submitted = st.form_submit_button("Guardar Cliente")

            if submitted:
                if cardcode in clientes["CardCode"].values:
                    st.warning("❌ Este CardCode ya existe.")
                elif not cardcode or not cardname:
                    st.warning("❗ Todos los campos son obligatorios.")
                else:
                    try:
                        _run_client_insert(
                            """
                            INSERT INTO OCRD (CardCode, CardName, CardType, SlpCode, validFor)
                            VALUES (?, ?, ?, ?, 'Y')
                            """,
                            (cardcode, cardname, cardtype, slpcode),
                        )
                        st.success("✅ Cliente creado exitosamente")
                    except Exception as e:
                        st.error(f"Error al guardar: {e}")

        st.markdown("### 📋 Clientes existentes")
        st.dataframe(clientes, use_container_width=True, height=250)

    # B_MNT004: Gestión de vendedores – creación y visualización
    # # ∂B_MNT004/∂B0
    with st.expander("👥 Vendedores – Crear / Ver"):
        vendedores_df = _run_vendor_select("SELECT * FROM OSLP ORDER BY SlpCode")

        with st.form("form_crear_vendedor"):
            st.markdown("➕ Crear nuevo vendedor")
            slpcode = st.number_input("Código Vendedor (SlpCode)", min_value=1, step=1)
            slpname = st.text_input("Nombre del Vendedor")
            submitted = st.form_submit_button("Guardar Vendedor")

            if submitted:
                if slpcode in vendedores_df["SlpCode"].values:
                    st.warning("❌ Este SlpCode ya existe.")
                elif not slpcode or not slpname:
                    st.warning("❗ Todos los campos son obligatorios.")
                else:
                    try:
                        _run_vendor_insert(
                            "INSERT INTO OSLP (SlpCode, SlpName) VALUES (?, ?)",
                            (int(slpcode), slpname),
                        )
                        st.success("✅ Vendedor creado exitosamente")
                        vendedores_df = _run_vendor_select(
                            "SELECT * FROM OSLP ORDER BY SlpCode"
                        )
                    except Exception as e:
                        st.error(f"Error al guardar: {e}")

        st.markdown("### 📋 Vendedores existentes")
        st.dataframe(vendedores_df, use_container_width=True, height=250)


# B_RSN001: Reasignar forecast comercial entre vendedores con control de clientes seleccionados
# # ∂B_RSN001/∂B0
def mostrar_tab_reasignacion():

    forecast_slp = _run_reasig_select(
        "SELECT DISTINCT SlpCode FROM Forecast_Detalle ORDER BY SlpCode"
    )
    vendedores = _run_reasig_select(
        "SELECT SlpCode, SlpName FROM OSLP ORDER BY SlpCode"
    )

    slp_dict = dict(zip(vendedores["SlpCode"], vendedores["SlpName"]))

    col1, col2 = st.columns(2)
    with col1:
        slp_origen = st.selectbox(
            "👤 Vendedor origen",
            forecast_slp["SlpCode"],
            format_func=lambda x: f"{x} – {slp_dict.get(x, 'Desconocido')}",
        )
    with col2:
        slp_destino = st.selectbox(
            "👤 Vendedor destino",
            vendedores["SlpCode"],
            format_func=lambda x: f"{x} – {slp_dict.get(x, 'Desconocido')}",
        )

    if slp_origen == slp_destino:
        st.warning("⚠️ El vendedor origen y destino deben ser distintos.")
        return

    preview = _run_reasig_select(
        """
        SELECT DISTINCT d.CardCode, c.CardName
        FROM Forecast_Detalle d
        LEFT JOIN OCRD c ON d.CardCode = c.CardCode
        WHERE d.SlpCode = ?
        """,
        params=(slp_origen,),
    )

    st.markdown(
        f"### 🧒 Clientes asignados al vendedor {slp_origen} – {slp_dict.get(slp_origen, 'Desconocido')}"
    )

    if "seleccionar_todo" not in st.session_state:
        st.session_state.seleccionar_todo = False

    if st.button("✅ Seleccionar todos", key="btn_seleccionar_todo"):
        st.session_state.seleccionar_todo = not st.session_state.seleccionar_todo

    preview["Seleccionar"] = st.session_state.seleccionar_todo

    preview_editado = st.data_editor(
        preview,
        use_container_width=True,
        height=300,
        num_rows="dynamic",
        key=f"editor_{slp_origen}",
    )

    cardcode_seleccionados = preview_editado.query("Seleccionar == True")[
        "CardCode"
    ].tolist()

    if st.button("🔁 Ejecutar reasignación de forecast", key="btn_reasignar"):
        if not cardcode_seleccionados:
            st.warning("⚠️ Debes seleccionar al menos un cliente para reasignar.")
            return

        try:
            _duplicar_forecast_reasignacion(
                slp_origen, slp_destino, cardcode_seleccionados
            )

            registrar_log_accion(
                usuario=st.session_state.get("usuario", "Desconocido"),
                accion="reasignacion_forecast",
                modulo="nucleo_control",
                detalle=(
                    f"Copiado de SlpCode {slp_origen} → {slp_destino} – "
                    f"{len(cardcode_seleccionados)} clientes"
                ),
            )

            st.success("✅ Reasignación completada exitosamente")

        except Exception as e:
            st.error(f"❌ Error al reasignar forecast: {e}")
