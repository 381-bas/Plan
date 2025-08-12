# B_CTX001: Importaciones y variables de contexto predeterminadas
# # ∂B_CTX001/∂B0
import streamlit as st
from datetime import datetime
from session_utils import set_slpcode


MES_ACTUAL = datetime.now().month
ANIO_ACTUAL = datetime.now().year

# B_CTX002: Inicialización del contexto global en session_state
# # ∂B_CTX002/∂B0
def inicializar_contexto(usuario=None, rol="ventas"):
    if "contexto" not in st.session_state:
        st.session_state.contexto = {
            "anio": ANIO_ACTUAL,
            "mes": MES_ACTUAL,
            "usuario": usuario,
            "rol": rol
        }

# B_CTX003: Obtener año desde el contexto global
# # ∂B_CTX003/∂B0
def obtener_anio():
    if "contexto" not in st.session_state:
        inicializar_contexto()
    return st.session_state.contexto.get("anio", ANIO_ACTUAL)

# B_CTX004: Obtener mes desde el contexto global
# # ∂B_CTX004/∂B0
def obtener_mes():
    if "contexto" not in st.session_state:
        inicializar_contexto()
    return st.session_state.contexto.get("mes", MES_ACTUAL)

# B_CTX005: Obtener usuario desde el contexto global
# # ∂B_CTX005/∂B0
def obtener_usuario():
    if "contexto" not in st.session_state:
        inicializar_contexto()
    return st.session_state.contexto.get("usuario", None)

# B_CTX006: Obtener rol desde el contexto global
# # ∂B_CTX006/∂B0
def obtener_rol():
    if "contexto" not in st.session_state:
        inicializar_contexto()
    return st.session_state.contexto.get("rol", "ventas")

# B_CTX007: Modificar año en el contexto global
# # ∂B_CTX007/∂B0
def set_anio(anio):
    st.session_state.contexto["anio"] = anio

# B_CTX008: Modificar mes en el contexto global
# # ∂B_CTX008/∂B0
def set_mes(mes):
    st.session_state.contexto["mes"] = mes

# B_CTX009: Modificar usuario en el contexto global
# # ∂B_CTX009/∂B0
def set_usuario(usuario):
    st.session_state.contexto["usuario"] = usuario

# B_CTX010: Modificar rol en el contexto global
# # ∂B_CTX010/∂B0
def set_rol(rol):
    st.session_state.contexto["rol"] = rol

# B_CTX011: Asignar usuario automáticamente desde parámetro query
# # ∂B_CTX011/∂B0
def asignar_usuario_desde_sesion(vendedor_param):
    """
    Asigna automáticamente un usuario si viene desde query_params (?vendedor=123)
    """
    print(f"[DEBUG] 🔁 asignar_usuario_desde_sesion ejecutado con vendedor = {vendedor_param}")
    
    if vendedor_param:
        st.session_state["usuario"] = f"Vendedor_{vendedor_param}"
        st.session_state["SlpCode"] = int(vendedor_param)
        st.session_state["rol"] = "ventas"

# B_CTX012: Accesor para obtener usuario actual (fallback)
# # ∂B_CTX012/∂B0
def obtener_usuario_actual():
    return st.session_state.get("usuario", "Invitado")

# B_CTX013: Accesor para obtener rol actual (fallback)
# # ∂B_CTX013/∂B0
def obtener_rol_actual():
    return st.session_state.get("rol", "ventas")

# B_CTX014: Accesor para obtener SlpCode actual (fallback)
# # ∂B_CTX014/∂B0
def obtener_slpcode():
    return st.session_state.get("SlpCode", 0)  # ✅ corregido a mayúscula

# B_CTX015: Configuración manual de usuario, SlpCode y rol (para pruebas o backdoor)
# # ∂B_CTX015/∂B0
def set_usuario_manual(nombre: str, slpcode: int, rol: str):
    st.session_state.usuario = nombre
    set_slpcode(int(slpcode))  # canónico + back-compat
    st.session_state.rol = rol

