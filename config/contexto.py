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
            "rol": rol,
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
    Logs: [RUN.INFO]/[RUN.WARN] en una sola línea, sin emojis.
    Retorna: (slp:int|None, changed:bool)
    """
    raw = None if vendedor_param is None else str(vendedor_param).strip()
    if not raw:
        print("[RUN.INFO] asignar_usuario_desde_sesion — vendedor=None set=False")
        return None, False

    try:
        slp = int(raw)
    except (ValueError, TypeError):
        print(
            f"[RUN.WARN] asignar_usuario_desde_sesion — vendedor={raw!r} invalido set=False"
        )
        return None, False

    if slp <= 0:
        print(
            f"[RUN.WARN] asignar_usuario_desde_sesion — vendedor={slp} invalido(<=0) set=False"
        )
        return None, False

    # Evitar escrituras redundantes en session_state:
    prev = (
        st.session_state.get("usuario"),
        st.session_state.get("SlpCode"),
        st.session_state.get("rol"),
    )
    target = (f"Vendedor_{slp}", slp, "ventas")

    if prev == target:
        print(
            f"[RUN.INFO] asignar_usuario_desde_sesion — vendedor={slp} set=reuse usuario=Vendedor_{slp} slpcode={slp} rol=ventas"
        )
        return slp, False

    st.session_state["usuario"] = target[0]
    st.session_state["SlpCode"] = target[1]
    st.session_state["rol"] = target[2]

    print(
        f"[RUN.INFO] asignar_usuario_desde_sesion — vendedor={slp} set=True usuario=Vendedor_{slp} slpcode={slp} rol=ventas"
    )
    return slp, True


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
def obtener_slpcode() -> int:
    """Devuelve SlpCode como int desde session_state. Log solo si falta o es inválido."""
    val = st.session_state.get("SlpCode", None)  # ✅ mayúscula
    if val in (None, ""):
        print("[SESSION.WARN] obtener_slpcode — not set -> 0")
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        print(f"[SESSION.WARN] obtener_slpcode — invalid={val!r} -> 0")
        return 0


# B_CTX015: Configuración manual de usuario, SlpCode y rol (para pruebas o backdoor)
# # ∂B_CTX015/∂B0
def set_usuario_manual(nombre: str, slpcode: int, rol: str):
    st.session_state.usuario = nombre
    set_slpcode(int(slpcode))  # canónico + back-compat
    st.session_state.rol = rol
