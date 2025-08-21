# B_FEN001: Importaciones y carga base de forecast_engine
# # ∂B_FEN001/∂B0
import logging
import os

# Configuración básica del logger local
logger = logging.getLogger(__name__)
if not logger.handlers:
    _hdl = logging.StreamHandler()
    _fmt = logging.Formatter("[%(levelname)s] %(name)s: %(message)s")
    _hdl.setFormatter(_fmt)
    logger.addHandler(_hdl)
logger.setLevel(logging.INFO)
if os.getenv("DEBUG_IMPORTS"):
    print("📍 forecast_engine.py LOADED desde:", __file__)
