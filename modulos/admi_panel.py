# B_CTX001: Importaciones principales y configuración de entorno para el panel administrativo SYMBIOS
# # ∂B_CTX001/∂B0
import streamlit as st
import pandas as pd
from streamlit import column_config
from utils.db import (
    _run_admin_select,
    _run_admin_insert,
)


# B_RUN001: Ejecutor principal del Panel Administrativo con tabs y validación de productos faltantes
# # ∂B_RUN001/∂B0
def run():
    st.title("🛠️ Panel Administrativo – SYMBIOS")

    tabs = st.tabs(
        [
            "🧩 Validar Productos Faltantes",
            "📊 Forecast por Vendedor (PRÓXIMAMENTE)",
            "📉 Resumen Alertas (PRÓXIMAMENTE)",
        ]
    )

    # B_UIX001: Tab para validación de productos faltantes en OITM y vista extendida
    # # ∂B_UIX001/∂B0
    with tabs[0]:
        st.subheader("🧩 Validar Productos Faltantes en OITM")

        forecast_items = _run_admin_select(
            "SELECT DISTINCT ItemCode FROM Forecast_Detalle"
        )
        oitm_items = _run_admin_select("SELECT * FROM OITM")

        codigos_oitm = oitm_items["ItemCode"].unique()
        codigos_faltantes = forecast_items[
            ~forecast_items["ItemCode"].isin(codigos_oitm)
        ]

        if codigos_faltantes.empty:
            st.success("✅ No hay productos faltantes en OITM.")
        else:
            st.warning(
                f"⚠️ Se detectaron {len(codigos_faltantes)} productos sin registrar en OITM."
            )

            # B_UIX002: Vista extendida de forecast vs OITM y preparación de editor editable con sugerencias únicas
            # # ∂B_UIX002/∂B0
            forecast_detalle = _run_admin_select("SELECT * FROM Forecast_Detalle")
            st.markdown("### 🧬 Vista extendida Forecast vs OITM (solo faltantes)")
            faltantes_detalle = forecast_detalle[
                forecast_detalle["ItemCode"].isin(codigos_faltantes["ItemCode"])
            ]
            st.dataframe(faltantes_detalle, use_container_width=True, height=300)

            # Crear plantilla editable con sugerencias reales
            columnas_oitm = oitm_items.columns.tolist()
            df_base = pd.DataFrame(columns=columnas_oitm)
            df_base["ItemCode"] = codigos_faltantes["ItemCode"]
            df_base["ItemName"] = df_base["ItemCode"]
            if "validFor" in df_base.columns:
                df_base["validFor"] = "Y"

            # Obtener sugerencias únicas desde OITM
            uom_options = (
                oitm_items["InvntryUom"].dropna().unique().tolist()
                if "InvntryUom" in oitm_items
                else []
            )
            grp_cod_options = (
                sorted(oitm_items["ItmsGrpCod"].dropna().unique().tolist())
                if "ItmsGrpCod" in oitm_items
                else []
            )
            grp_name_options = (
                sorted(oitm_items["ItmsGrpNam"].dropna().unique().tolist())
                if "ItmsGrpNam" in oitm_items
                else []
            )

            col_config = {}
            if "validFor" in df_base.columns:
                col_config["validFor"] = column_config.SelectboxColumn(
                    label="¿Válido?", options=["Y", "N"], required=True
                )
            if "InvntryUom" in df_base.columns:
                col_config["InvntryUom"] = column_config.SelectboxColumn(
                    label="Unidad de Inventario", options=uom_options, required=False
                )
            if "ItmsGrpCod" in df_base.columns:
                col_config["ItmsGrpCod"] = column_config.SelectboxColumn(
                    label="Código Grupo", options=grp_cod_options, required=False
                )
            if "ItmsGrpNam" in df_base.columns:
                col_config["ItmsGrpNam"] = column_config.SelectboxColumn(
                    label="Nombre Grupo", options=grp_name_options, required=False
                )

            st.markdown("### ✍️ Editor interactivo con sugerencias únicas por campo")
            df_editado = st.data_editor(
                df_base,
                column_config=col_config,
                use_container_width=True,
                num_rows="dynamic",
                height=400,
            )

            # B_INS001: Inserción de productos editados en OITM con control de errores y feedback al usuario
            # # ∂B_INS001/∂B0
            if st.button("➕ Insertar registros editados en OITM"):
                try:
                    for _, row in df_editado.iterrows():
                        placeholders = ", ".join(["?"] * len(df_editado.columns))
                        _run_admin_insert(
                            f"INSERT INTO OITM ({', '.join(df_editado.columns)}) VALUES ({placeholders})",
                            tuple(row),
                        )
                    st.success("✅ Inserción completa realizada con éxito.")
                except Exception as e:
                    st.error(f"❌ Error al insertar productos: {e}")
