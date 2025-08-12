# B_ROUT001: Importaciones base e infraestructura para router dinámico de módulos
# # ∂B_ROUT001/∂B0
import importlib
import streamlit as st
from config.contexto import obtener_rol_actual

MODULOS_POR_ROL = {
    "ventas": ["ventas", "inicio"],
    "produccion": ["produccion", "inicio"],
    "gestion": ["gestion", "inicio", "nucleo_control"], 
    "admin": ["ventas", "produccion", "compras", "gestion", "admi_panel", "inicio", "nucleo_control"] 
}

MODULOS_DISPONIBLES = {
    "ventas": "modulos.ventas",
    "produccion": "modulos.produccion",
    "gestion": "modulos.gestion",
    "admi_panel": "modulos.admi_panel",
    "inicio": "modulos.main_home",
    "nucleo_control": "modulos.nucleo_control"  # ✅ NUEVO: Módulo de control para jefatura
}

# B_ROUT002: Función para cargar y validar módulo según nombre y permisos de rol
# # ∂B_ROUT002/∂B0
def cargar_modulo_si_valido(nombre_modulo: str):
    rol_actual = obtener_rol_actual()

    # 3.1 – Validación de existencia
    if nombre_modulo not in MODULOS_DISPONIBLES:
        st.warning(f"⚠️ Módulo no reconocido: '{nombre_modulo}'")
        return

    # 3.2 – Validación de permisos
    modulos_autorizados = MODULOS_POR_ROL.get(rol_actual, [])
    if nombre_modulo not in modulos_autorizados:
        if rol_actual != "admin":
            st.warning(f"⚠️ Acceso no autorizado a '{nombre_modulo}' para rol '{rol_actual}'")
            return

    # 3.3 – Importación y ejecución
    try:
        mod = importlib.import_module(MODULOS_DISPONIBLES[nombre_modulo])
        if hasattr(mod, "run") and callable(mod.run):
            mod.run()
        else:
            st.error(f"❌ El módulo '{nombre_modulo}' no tiene una función `run()` definida.")
    except Exception as e:
        st.error(f"❌ Error al cargar el módulo '{nombre_modulo}': {e}")
