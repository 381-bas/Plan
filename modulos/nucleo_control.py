# B_CTX001: Importaciones principales, utilidades y configuraciÃ³n de entorno para nÃºcleo de control
# # âˆ‚B_CTX001/âˆ‚B0
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


# B_RUN001: FunciÃ³n principal run() â€“ nÃºcleo de control con tabs de gestiÃ³n, reasignaciÃ³n y visualizaciÃ³n
# # âˆ‚B_RUN001/âˆ‚B0
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
            "ðŸ“‹ Crear / Visualizar Registros",
            "ðŸ” Reasignar Forecast Comercial",
            "ðŸ“Š Tablas Forecast",
            "ðŸ“¸ Snapshot",
        ]
    )

    # B_UIX001: Tab de creaciÃ³n y visualizaciÃ³n de productos, clientes y vendedores
    # # âˆ‚B_UIX001/âˆ‚B0
    with tabs[0]:
        mostrar_tab_crear_y_ver()

    # B_UIX002: Tab de reasignaciÃ³n de forecast comercial
    # # âˆ‚B_UIX002/âˆ‚B0
    with tabs[1]:
        mostrar_tab_reasignacion()

    # B_UIX003: Tab para visualizaciÃ³n de tablas forecast
    # # âˆ‚B_UIX003/âˆ‚B0
    with tabs[2]:
        forecast_tablas.run()

    # Agrega una nueva pestaÃ±a al final de tu lista de tabs
    with tabs[3]:
        st.header("ðŸ“¸ Snapshot Forecast vs Realidad")

        col1, col2 = st.columns([1, 2])
        with col1:
            if st.button("ðŸ“¸ Generar Snapshot Forecast"):
                df_no_forecast = generar_snapshot_completo()

                if not df_no_forecast.empty:
                    st.warning(
                        f"âš ï¸ {len(df_no_forecast)} clientes con ventas no tienen forecast:"
                    )
                    st.dataframe(df_no_forecast, use_container_width=True, height=250)
                else:
                    st.success(
                        "âœ… Todos los clientes con ventas tienen forecast asignado."
                    )

                st.success("Snapshot generado desde Forecast.")

        with col2:
            if st.button("ðŸ“Š Actualizar con Ventas Reales"):
                actualizar_snapshot_realidad()
                st.success("ActualizaciÃ³n con datos de OINV completada.")


# B_MNT001: Crear y ver productos, clientes y vendedores en nÃºcleo de control
# # âˆ‚B_MNT001/âˆ‚B0
def mostrar_tab_crear_y_ver():

    # B_MNT002: GestiÃ³n de productos â€“ creaciÃ³n y visualizaciÃ³n
    # # âˆ‚B_MNT002/âˆ‚B0
    with st.expander("ðŸ“¦ Productos â€“ Crear / Ver"):
        productos_df = _run_product_select("SELECT * FROM OITM ORDER BY ItemCode")

        with st.form("form_crear_producto"):
            st.markdown(" âž• Crear nuevo producto")
            itemcode = st.text_input("CÃ³digo Producto (ItemCode)")
            itemname = st.text_input("Nombre del Producto")

            itmsgrpnam = st.selectbox(
                "LÃ­nea de Negocio",
                ["Trd-Alim", "Trd-Farm", "Trd-Cosm", "Pta-Nutr", "Pta-Alim"],
            )

            submitted = st.form_submit_button("Guardar Producto")

            if submitted:
                if itemcode in productos_df["ItemCode"].values:
                    st.warning("âŒ Este ItemCode ya existe.")
                elif not itemcode or not itemname:
                    st.warning("â— Todos los campos son obligatorios.")
                else:
                    try:
                        _run_product_insert(
                            "INSERT INTO OITM (ItemCode, ItemName, ItmsGrpNam, validFor) VALUES (?, ?, ?, 'Y')",
                            (itemcode, itemname, itmsgrpnam),
                        )
                        st.success("âœ… Producto creado exitosamente")
                        # refrescar listado
                        productos_df = _run_product_select(
                            "SELECT * FROM OITM ORDER BY ItemCode"
                        )
                    except Exception as e:
                        st.error(f"Error al guardar: {e}")

        st.markdown("### ðŸ“‹ Productos existentes")
        st.dataframe(productos_df, use_container_width=True, height=250)

    # B_MNT003: GestiÃ³n de clientes â€“ creaciÃ³n y visualizaciÃ³n
    # # âˆ‚B_MNT003/âˆ‚B0
    with st.expander("ðŸª Clientes â€“ Crear / Ver"):

        # --- SELECT iniciales usando wrapper ---------------------------------
        clientes = _run_client_select("SELECT * FROM OCRD ORDER BY CardCode")
        vendedores = _run_client_select(
            "SELECT SlpCode, SlpName FROM OSLP ORDER BY SlpCode"
        )
        # ---------------------------------------------------------------------

        with st.form("form_crear_cliente"):
            st.markdown("âž• Crear nuevo cliente")
            cardcode = st.text_input("CÃ³digo Cliente (CardCode)")
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
                format_func=lambda x: f"{x} â€“ {vendedores[vendedores['SlpCode'] == x]['SlpName'].values[0]}",
            )
            submitted = st.form_submit_button("Guardar Cliente")

            if submitted:
                if cardcode in clientes["CardCode"].values:
                    st.warning("âŒ Este CardCode ya existe.")
                elif not cardcode or not cardname:
                    st.warning("â— Todos los campos son obligatorios.")
                else:
                    try:
                        _run_client_insert(
                            """
                            INSERT INTO OCRD (CardCode, CardName, CardType, SlpCode, validFor)
                            VALUES (?, ?, ?, ?, 'Y')
                            """,
                            (cardcode, cardname, cardtype, slpcode),
                        )
                        st.success("âœ… Cliente creado exitosamente")
                    except Exception as e:
                        st.error(f"Error al guardar: {e}")

        st.markdown("### ðŸ“‹ Clientes existentes")
        st.dataframe(clientes, use_container_width=True, height=250)

    # B_MNT004: GestiÃ³n de vendedores â€“ creaciÃ³n y visualizaciÃ³n
    # # âˆ‚B_MNT004/âˆ‚B0
    with st.expander("ðŸ‘¥ Vendedores â€“ Crear / Ver"):
        vendedores_df = _run_vendor_select("SELECT * FROM OSLP ORDER BY SlpCode")

        with st.form("form_crear_vendedor"):
            st.markdown("âž• Crear nuevo vendedor")
            slpcode = st.number_input("CÃ³digo Vendedor (SlpCode)", min_value=1, step=1)
            slpname = st.text_input("Nombre del Vendedor")
            submitted = st.form_submit_button("Guardar Vendedor")

            if submitted:
                if slpcode in vendedores_df["SlpCode"].values:
                    st.warning("âŒ Este SlpCode ya existe.")
                elif not slpcode or not slpname:
                    st.warning("â— Todos los campos son obligatorios.")
                else:
                    try:
                        _run_vendor_insert(
                            "INSERT INTO OSLP (SlpCode, SlpName) VALUES (?, ?)",
                            (int(slpcode), slpname),
                        )
                        st.success("âœ… Vendedor creado exitosamente")
                        vendedores_df = _run_vendor_select(
                            "SELECT * FROM OSLP ORDER BY SlpCode"
                        )
                    except Exception as e:
                        st.error(f"Error al guardar: {e}")

        st.markdown("### ðŸ“‹ Vendedores existentes")
        st.dataframe(vendedores_df, use_container_width=True, height=250)


# B_RSN001: Reasignar forecast comercial entre vendedores con control de clientes seleccionados
# # âˆ‚B_RSN001/âˆ‚B0
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
            "ðŸ‘¤ Vendedor origen",
            forecast_slp["SlpCode"],
            format_func=lambda x: f"{x} â€“ {slp_dict.get(x, 'Desconocido')}",
        )
    with col2:
        slp_destino = st.selectbox(
            "ðŸ‘¤ Vendedor destino",
            vendedores["SlpCode"],
            format_func=lambda x: f"{x} â€“ {slp_dict.get(x, 'Desconocido')}",
        )

    if slp_origen == slp_destino:
        st.warning("âš ï¸ El vendedor origen y destino deben ser distintos.")
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
        f"### ðŸ§’ Clientes asignados al vendedor {slp_origen} â€“ {slp_dict.get(slp_origen, 'Desconocido')}"
    )

    if "seleccionar_todo" not in st.session_state:
        st.session_state.seleccionar_todo = False

    if st.button("âœ… Seleccionar todos", key="btn_seleccionar_todo"):
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

    if st.button("ðŸ” Ejecutar reasignaciÃ³n de forecast", key="btn_reasignar"):
        if not cardcode_seleccionados:
            st.warning("âš ï¸ Debes seleccionar al menos un cliente para reasignar.")
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
                    f"Copiado de SlpCode {slp_origen} â†’ {slp_destino} â€“ "
                    f"{len(cardcode_seleccionados)} clientes"
                ),
            )

            st.success("âœ… ReasignaciÃ³n completada exitosamente")

        except Exception as e:
            st.error(f"âŒ Error al reasignar forecast: {e}")
