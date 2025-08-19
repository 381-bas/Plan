# B_ROUT001: Importaciones base e infraestructura para router dinámico de módulos
# # ∂B_ROUT001/∂B0
import importlib
import streamlit as st
from config.contexto import obtener_rol_actual

MODULOS_POR_ROL = {
    "ventas": ["ventas", "inicio"],
    "produccion": ["produccion", "inicio"],
    "gestion": ["gestion", "inicio", "nucleo_control"],
    "admin": [
        "ventas",
        "produccion",
        "compras",
        "gestion",
        "admi_panel",
        "inicio",
        "nucleo_control",
    ],
}

MODULOS_DISPONIBLES = {
    "ventas": "modulos.ventas",
    "produccion": "modulos.produccion",
    "gestion": "modulos.gestion",
    "admi_panel": "modulos.admi_panel",
    "inicio": "modulos.main_home",
    "nucleo_control": "modulos.nucleo_control",  # ✅ NUEVO: Módulo de control para jefatura
}


# --- PATCH: detector de Rerun para no mostrarlo como error
def _is_rerun_exc(e: Exception) -> bool:
    return e.__class__.__name__ in ("RerunException", "RerunData")


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
            st.warning(
                f"⚠️ Acceso no autorizado a '{nombre_modulo}' para rol '{rol_actual}'"
            )
            return

    # --- PATCH: gating de 'ventas' sin vendedor → redirige limpio a inicio
    if nombre_modulo == "ventas":
        vendedor = st.query_params.get("vendedor")
        if vendedor is None or f"{vendedor}".strip() == "":
            st.info("Selecciona un vendedor para continuar.")
            st.query_params.update({"modulo": "inicio"})
            st.rerun()

    # 3.3 – Importación y ejecución
    try:
        mod = importlib.import_module(MODULOS_DISPONIBLES[nombre_modulo])
        if hasattr(mod, "run") and callable(mod.run):
            mod.run()
        else:
            st.error(
                f"❌ El módulo '{nombre_modulo}' no tiene una función `run()` definida."
            )
    except Exception as e:
<<<<<<< HEAD
        # ── Manejo robusto de rerun de Streamlit (navegación / query_params) ──
        try:
            from streamlit.runtime.scriptrunner import RerunException, RerunData

            if isinstance(e, (RerunException, RerunData)):
                raise  # dejar que Streamlit haga el rerun sin ensuciar la UI
        except Exception:
            # Fallback: por si cambia la ruta de clases entre versiones
            if e.__class__.__name__ in ("RerunException", "RerunData"):
                raise

        # Si NO es un rerun, mostramos el error real
        safe_name = (
            locals().get("nombre_modulo") or locals().get("modulo") or "desconocido"
        )
        st.error(f"❌ Error al cargar el módulo '{safe_name}': {e}")
        st.stop()
=======
        # --- PATCH: no reportar Rerun como error
        if _is_rerun_exc(e):
            raise
        st.error(f"❌ Error al cargar el módulo '{nombre_modulo}': {e}")
>>>>>>> 15e7611 (docs(ventas.py): comenta manejo de RerunData y notas B_ROUT001 (sin cambio de lógica))
