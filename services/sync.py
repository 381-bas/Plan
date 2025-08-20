# B_TMP001: Importaciones y configuración de base temporal para backups de edición
# # ∂B_TMP001/∂B0
import os
import pandas as pd
import hashlib
from pathlib import Path
from pandas.util import hash_pandas_object  # hash estructural
from session_utils import normalize_df_for_hash, safe_pickle_load, atomic_pickle_dump

BASE_TEMP = os.path.join(os.path.dirname(__file__), "..", "temp_ediciones")
os.makedirs(BASE_TEMP, exist_ok=True)


# B_TMP002: Construcción de ruta de backup temporal para cliente
# # ∂B_TMP002/∂B0
def _ruta_temp(cliente: str) -> str:
    return os.path.join(BASE_TEMP, f"{cliente}_forecast.pkl")


# B_HDF001: Hash robusto de DataFrame usando SHA-256 (control de integridad)
# # ∂B_HDF001/∂B0
def hash_df(df):
    return hashlib.sha256(
        pd.util.hash_pandas_object(df.sort_index(axis=1), index=True).values
    ).hexdigest()


# B_TMP003: Guardado seguro de backup temporal (.pkl) del DataFrame de cliente
# # ∂B_TMP003/∂B0
def guardar_temp_local(cliente: str, df: pd.DataFrame):
    """
    Backup temporal (pickle) por cliente:
    - Hash estructural estable para evitar escrituras redundantes.
    - Lectura segura (lista blanca) confinada al directorio destino.
    - Escritura atómica (tmp + replace).
    """
    ruta_str = _ruta_temp(cliente)  # p.ej. ".../tmp/<cliente>.pkl"
    ruta = Path(ruta_str).resolve()
    ruta.parent.mkdir(parents=True, exist_ok=True)

    try:
        df_norm = normalize_df_for_hash(df)
        nuevo_hash = int(hash_pandas_object(df_norm, index=True).sum())

        hash_prev = None
        if ruta.exists():
            try:
                df_prev = safe_pickle_load(ruta, ruta.parent)
                df_prev_norm = normalize_df_for_hash(df_prev)
                hash_prev = int(hash_pandas_object(df_prev_norm, index=True).sum())
            except Exception as _e:
                print(f"⚠️  Backup previo ilegible, se reescribirá: {ruta} ({_e})")

        if hash_prev is not None and nuevo_hash == hash_prev:
            print(f"🟡 Sin cambios para {cliente}, se evita escritura redundante.")
            return

        atomic_pickle_dump(df, ruta)
        print(f"✅ Backup temporal guardado para {cliente} -> {ruta}")

    except Exception as e:
        print(f"❌ Error al guardar backup temporal para {cliente}: {e}")
