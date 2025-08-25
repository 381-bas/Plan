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


# B_ROUT002: Función para cargar y validar módulo según nombre y permisos de rol
# # ∂B_ROUT002/∂B0
def cargar_modulo_si_valido(nombre_modulo: str):
    import time
    import traceback as _tb

    t0 = time.perf_counter()
    rol_actual = obtener_rol_actual()
    modulo = str(nombre_modulo) if nombre_modulo is not None else ""

    print(f"[ROUTER.INFO] start modulo='{modulo}' rol='{rol_actual}'")

    # 1) Validación de existencia
    if modulo not in MODULOS_DISPONIBLES:
        print(
            f"[ROUTER.WARN] modulo_no_reconocido nombre='{modulo}' disponibles={len(MODULOS_DISPONIBLES)}"
        )
        st.warning(f"⚠️ Módulo no reconocido: '{modulo}'")
        print(
            f"[ROUTER.INFO] end modulo='{modulo}' status='not_found' elapsed={time.perf_counter()-t0:.3f}s"
        )
        return

    # 2) Validación de permisos
    modulos_autorizados = MODULOS_POR_ROL.get(rol_actual, [])
    if modulo not in modulos_autorizados and rol_actual != "admin":
        print(
            f"[ROUTER.WARN] acceso_no_autorizado modulo='{modulo}' rol='{rol_actual}' autorizados={len(modulos_autorizados)}"
        )
        st.warning(f"⚠️ Acceso no autorizado a '{modulo}' para rol '{rol_actual}'")
        print(
            f"[ROUTER.INFO] end modulo='{modulo}' status='forbidden' elapsed={time.perf_counter()-t0:.3f}s"
        )
        return

    # 3) Importación
    module_path = MODULOS_DISPONIBLES[modulo]
    try:
        t_import = time.perf_counter()
        print(f"[ROUTER.INFO] import.enter path='{module_path}'")
        mod = importlib.import_module(module_path)
        print(
            f"[ROUTER.INFO] import.exit path='{module_path}' elapsed={time.perf_counter()-t_import:.3f}s"
        )
    except Exception as e:
        # Rerun handling (poco probable en import, pero homogéneo)
        try:
            from streamlit.runtime.scriptrunner import RerunException, RerunData

            if isinstance(e, (RerunException, RerunData)):
                print(
                    f"[ROUTER.RERUN] modulo='{modulo}' motivo='streamlit_rerun(import)'"
                )
                raise
        except Exception:
            if e.__class__.__name__ in ("RerunException", "RerunData"):
                print(
                    f"[ROUTER.RERUN] modulo='{modulo}' motivo='streamlit_rerun(import_fallback)'"
                )
                raise

        print(
            f"[ROUTER.ERROR] import_failed modulo='{modulo}' path='{module_path}' exc={type(e).__name__} msg={e}"
        )
        st.error(f"❌ Error al importar el módulo '{modulo}' ({module_path}): {e}")
        print(
            f"[ROUTER.TRACE] {''.join(_tb.format_exception_only(type(e), e)).strip()}"
        )
        print(
            f"[ROUTER.INFO] end modulo='{modulo}' status='error_import' elapsed={time.perf_counter()-t0:.3f}s"
        )
        st.stop()

    # 4) Ejecución
    if not hasattr(mod, "run") or not callable(mod.run):
        print(f"[ROUTER.ERROR] run_missing modulo='{modulo}' path='{module_path}'")
        st.error(f"❌ El módulo '{modulo}' no tiene una función `run()` definida.")
        print(
            f"[ROUTER.INFO] end modulo='{modulo}' status='no_run' elapsed={time.perf_counter()-t0:.3f}s"
        )
        return

    try:
        t_run = time.perf_counter()
        print(f"[ROUTER.INFO] run.enter modulo='{modulo}'")
        mod.run()
        print(
            f"[ROUTER.INFO] run.exit modulo='{modulo}' elapsed={time.perf_counter()-t_run:.3f}s"
        )
        print(
            f"[ROUTER.INFO] end modulo='{modulo}' status='ok' elapsed={time.perf_counter()-t0:.3f}s"
        )
    except Exception as e:
        # Manejo robusto de rerun de Streamlit
        try:
            from streamlit.runtime.scriptrunner import RerunException, RerunData

            if isinstance(e, (RerunException, RerunData)):
                print(f"[ROUTER.RERUN] modulo='{modulo}' motivo='streamlit_rerun'")
                raise
        except Exception:
            if e.__class__.__name__ in ("RerunException", "RerunData"):
                print(
                    f"[ROUTER.RERUN] modulo='{modulo}' motivo='streamlit_rerun(fallback)'"
                )
                raise

        print(
            f"[ROUTER.ERROR] run_failed modulo='{modulo}' path='{module_path}' exc={type(e).__name__} msg={e}"
        )
        st.error(f"❌ Error al ejecutar el módulo '{modulo}': {e}")
        print(
            f"[ROUTER.TRACE] {''.join(_tb.format_exception_only(type(e), e)).strip()}"
        )
        print(
            f"[ROUTER.INFO] end modulo='{modulo}' status='error_run' elapsed={time.perf_counter()-t0:.3f}s"
        )
        st.stop()
