import utils.pickle_adapter  # noqa: F401  # habilita Parquet si BACKUP_FMT=parquet

# B_CTX001: Importaciones principales y configuración de contexto base para SYMBIOS
# # ∂B_CTX001/∂B0
import streamlit as st
from config.contexto import asignar_usuario_desde_sesion, set_usuario_manual
from utils.logs.log_operativo import registrar_log_accion
from config.router import cargar_modulo_si_valido


# B_UIX001: Configuración visual inicial y aplicación de estilo base (blanco y negro)
# # ∂B_UIX001/∂B0
st.set_page_config(page_title="Plan", layout="wide")


# ---------- Captura de parámetros URL ----------
params = st.query_params.to_dict()  # ← API estable

# Ej.: http://host:8501/?modulo=ventas&vendedor=3
modulo = params.get("modulo")
vendedor = params.get("vendedor")

if modulo:
    st.session_state["modulo"] = modulo
if vendedor:
    st.session_state["vendedor"] = vendedor

# ---------- Valores por defecto ----------
if "modulo" not in st.session_state:
    st.session_state["modulo"] = "inicio"  # página home


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


# B_CTX002: Asignación de usuario por defecto si no hay sesión activa
# # ∂B_CTX002/∂B0
if "usuario" not in st.session_state:
    set_usuario_manual("Admin", 999, "admin")  # 👈 usuario por defecto

# B_CTX003: Asignación o reasignación de usuario desde parámetros query
# # ∂B_CTX003/∂B0
if vendedor:
    asignar_usuario_desde_sesion(vendedor)

# B_ROUT001: Redirección automática de usuario por rol o ausencia de módulo
# # ∂B_ROUT001/∂B0
if not modulo:
    if st.session_state.get("rol") == "ventas":
        # 🧠 Usar siempre la clave correcta y forzar asignación si SlpCode está en default
        slpcode = st.session_state.get("SlpCode", 999)

        if slpcode == 999:
            # 🚨 Detectamos que no hay SlpCode real, intentamos corregir desde query param o abortamos
            vendedor_param = params.get("vendedor")
            if vendedor_param:
                asignar_usuario_desde_sesion(vendedor_param)
                slpcode = st.session_state.get("SlpCode", 999)
            else:
                st.warning(
                    "⚠️ No se pudo detectar un SlpCode válido. Usando modo prueba."
                )

        st.query_params.update(modulo="ventas", vendedor=slpcode)
    else:
        st.query_params.update(modulo="inicio")
    st.rerun()

# B_LOG001: Registro de acceso de usuario en logs operativos
# # ∂B_LOG001/∂B0
registrar_log_accion(
    usuario=st.session_state.get("usuario", "Anonimo"),
    accion="acceso_modulo",
    modulo=modulo or "inicio",
    detalle="Ingreso vía query_params",
)

# B_ROUT002: Carga dinámica del módulo si es válido
# # ∂B_ROUT002/∂B0
cargar_modulo_si_valido(modulo)
