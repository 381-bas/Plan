import utils.pickle_adapter  # noqa: F401  # habilita Parquet si BACKUP_FMT=parquet

# B_CTX001: Importaciones principales y configuraciÃ³n de contexto base para SYMBIOS
# # âˆ‚B_CTX001/âˆ‚B0
import streamlit as st
from config.contexto import asignar_usuario_desde_sesion, set_usuario_manual
from utils.logs.log_operativo import registrar_log_accion
from config.router import cargar_modulo_si_valido


# B_UIX001: ConfiguraciÃ³n visual inicial y aplicaciÃ³n de estilo base (blanco y negro)
# # âˆ‚B_UIX001/âˆ‚B0
st.set_page_config(page_title="Plan", layout="wide")


# ---------- Captura de parÃ¡metros URL ----------
params = st.query_params.to_dict()  # â† API estable

# Ej.: http://host:8501/?modulo=ventas&vendedor=3
modulo = params.get("modulo")
vendedor = params.get("vendedor")

if modulo:
    st.session_state["modulo"] = modulo
if vendedor:
    st.session_state["vendedor"] = vendedor

# ---------- Valores por defecto ----------
if "modulo" not in st.session_state:
    st.session_state["modulo"] = "inicio"  # pÃ¡gina home


# Aplicar estilo blanco y negro
ACTIVAR_ESTILO = False

if ACTIVAR_ESTILO:
    st.markdown(
        """
        <style>
            html, body, .block-container {
                background-color: white !important;
                color: black !important;
            }

            .stApp {
                background-color: white !important;
            }
        </style>
    """,
        unsafe_allow_html=True,
    )

# params = st.query_params.to_dict()
# modulo = params.get("modulo")
# vendedor = params.get("vendedor")


modulo = st.session_state["modulo"]
vendedor = st.session_state.get("vendedor")  # puede ser None


# B_CTX002: AsignaciÃ³n de usuario por defecto si no hay sesiÃ³n activa
# # âˆ‚B_CTX002/âˆ‚B0
if "usuario" not in st.session_state:
    set_usuario_manual("Admin", 999, "admin")  # ðŸ‘ˆ usuario por defecto

# B_CTX003: AsignaciÃ³n o reasignaciÃ³n de usuario desde parÃ¡metros query
# # âˆ‚B_CTX003/âˆ‚B0
if vendedor:
    asignar_usuario_desde_sesion(vendedor)

# B_ROUT001: RedirecciÃ³n automÃ¡tica de usuario por rol o ausencia de mÃ³dulo
# # âˆ‚B_ROUT001/âˆ‚B0
if not modulo:
    if st.session_state.get("rol") == "ventas":
        # ðŸ§  Usar siempre la clave correcta y forzar asignaciÃ³n si SlpCode estÃ¡ en default
        slpcode = st.session_state.get("SlpCode", 999)

        if slpcode == 999:
            # ðŸš¨ Detectamos que no hay SlpCode real, intentamos corregir desde query param o abortamos
            vendedor_param = params.get("vendedor")
            if vendedor_param:
                asignar_usuario_desde_sesion(vendedor_param)
                slpcode = st.session_state.get("SlpCode", 999)
            else:
                st.warning(
                    "âš ï¸ No se pudo detectar un SlpCode vÃ¡lido. Usando modo prueba."
                )

        st.query_params.update(modulo="ventas", vendedor=slpcode)
    else:
        st.query_params.update(modulo="inicio")
    st.rerun()

# B_LOG001: Registro de acceso de usuario en logs operativos
# # âˆ‚B_LOG001/âˆ‚B0
registrar_log_accion(
    usuario=st.session_state.get("usuario", "Anonimo"),
    accion="acceso_modulo",
    modulo=modulo or "inicio",
    detalle="Ingreso vÃ­a query_params",
)

# B_ROUT002: Carga dinÃ¡mica del mÃ³dulo si es vÃ¡lido
# # âˆ‚B_ROUT002/âˆ‚B0
cargar_modulo_si_valido(modulo)
