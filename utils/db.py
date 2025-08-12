"""utils/db.py – núcleo flexible E1+legacy
================================================
Este módulo centraliza el acceso a SQLite siguiendo RATUC‑F.
Provide flexible helper functions that support both legacy and new call signatures so that no downstream code breaks while we migrate.
"""

from __future__ import annotations

import sqlite3
import pandas as pd
import time
from functools import partial, wraps
from typing import Any, Callable, Iterable, List, Tuple

# ---------------------------------------------------------------------------
# 📌 Configuración global
# ---------------------------------------------------------------------------
DB_PATH: str = (
    "C:/Users/qmkbantiman/OneDrive - QMK SPA/Informacion/quickpilot/Plan_Final_Final_Final.db"
)

# ---------------------------------------------------------------------------
# 🔑 Núcleo limpio (lectura / escritura)
# ---------------------------------------------------------------------------

def run_query(sql: str, *args: Any, **kwargs: Any) -> pd.DataFrame:  # noqa: C901
    """Ejecuta un SELECT y devuelve un **pandas.DataFrame**.

    Acepta **dos** patrones de llamada para compatibilidad retro‑activa:

    1. **Legado**  (3 posicionales)
       >>> run_query(sql, DB_PATH, params)

    2. **Nuevo**   (params como *kw‑only*)
       >>> run_query(sql, params=params, db_path=DB_PATH)

    El parámetro *db_path* es opcional y por defecto usa :data:`DB_PATH`.
    """
    # ------------------ Parseo flexible de argumentos ------------------
    db_path: str = kwargs.pop("db_path", DB_PATH)
    params: Tuple[Any, ...] | List[Tuple[Any, ...]] | None

    if args:
        if len(args) == 1:
            # Puede ser db_path o params; detectamos por tipo.
            if isinstance(args[0], str):
                db_path = args[0]
                params = kwargs.pop("params", ())
            else:
                params = args[0]  # type: ignore[assignment]
        elif len(args) == 2:
            db_path, params = args  # type: ignore[assignment]
        else:
            raise TypeError(
                f"run_query() esperaba ≤3 posicionales, recibió {len(args) + 1}."
            )
    else:
        params = kwargs.pop("params", ())

    if kwargs:
        raise TypeError(f"run_query() argumentos inesperados: {list(kwargs)}")

    params = params or ()

    # ------------------ Ejecución ------------------
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def _execute_write(
    sql: str,
    params: Tuple[Any, ...] | List[Tuple[Any, ...]] | None = None,
    *,
    many: bool = False,
    timeout: int = 15,
    db_path: str = DB_PATH,
) -> None:
    """INSERT/UPDATE/DELETE con *commit*.

    *   **many=True** → usa *executemany*.
    *   Param *params* opcional se normaliza a tupla vacía.
    """
    params = params or ()
    with sqlite3.connect(db_path, timeout=timeout) as conn:
        (conn.executemany if many else conn.execute)(sql, params)  # type: ignore[arg-type]
        conn.commit()

# ---------------------------------------------------------------------------
# 🔄 Decorador de reintento (bloqueo)
# ---------------------------------------------------------------------------

def retry_sql_locked(max_attempts: int = 5, delay: float = 0.4) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Reintenta cuando la base está bloqueada (*database is locked*)."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        def _wrapped(*a: Any, **kw: Any):
            for attempt in range(max_attempts):
                try:
                    return fn(*a, **kw)
                except sqlite3.OperationalError as exc:  # pragma: no cover
                    if "database is locked" in str(exc).lower() and attempt < max_attempts - 1:
                        time.sleep(delay)
                    else:
                        raise

        return _wrapped

    return decorator

# utils/db.py  (o el módulo donde se declaren los wrappers)
# ──────────────────────────────────────────────────────────
def _run_forecast_select(
        sql: str,
        params: tuple | None = None,
        db_path: str = DB_PATH
) -> pd.DataFrame:
    """Select genérico (mantiene API antigua, añade db_path opcional)."""
    return run_query(sql, db_path, params=params or ())

# Idéntico patrón para _run_admin_select, _run_product_select, etc.



# ---------------------------------------------------------------------------
# 🧩 Wrappers por flujo (alias → núcleo)
#   Conservan nombres históricos para cero breaking‑changes
# ---------------------------------------------------------------------------
_run_admin_select = lambda sql, params=None: run_query(sql, params=params or ())

_run_product_select = lambda sql, params=None: run_query(sql, params=params or ())
_run_client_select = lambda sql, params=None: run_query(sql, params=params or ())
_run_vendor_select = lambda sql, params=None: run_query(sql, params=params or ())
_run_reasig_select = lambda sql, params=None: run_query(sql, params=params or ())
_run_cf_select = lambda sql, params=None: run_query(sql, params=params or ())
_run_tab_select = lambda sql, params=None: run_query(sql, params=params or ())
_run_gestion_select = lambda sql, params=None: run_query(sql, params=params or ())
_run_home_select = lambda sql, params=None: run_query(sql, params=params or ())

# -- escrituras --
_run_admin_insert = _execute_write
_run_forecast_write = _execute_write
_run_product_insert = _execute_write
_run_client_insert = _execute_write
_run_vendor_insert = _execute_write

# ---------------------------------------------------------------------------
# 🏷️  Wrappers con lógica dedicada
# ---------------------------------------------------------------------------

def _run_log_to_sql(df: pd.DataFrame, table: str, *, db_path: str = DB_PATH) -> None:
    """Carga un DataFrame en *table* (append) si no está vacío."""
    if df.empty:
        return
    with sqlite3.connect(db_path, timeout=15) as conn:
        df.to_sql(table, conn, if_exists="append", index=False)


def _run_forecast_insert_get_id(sql: str, params: Tuple[Any, ...], *, timeout: int = 15) -> int:
    """INSERT y devuelve `lastrowid`."""
    with sqlite3.connect(DB_PATH, timeout=timeout, isolation_level="DEFERRED") as conn:
        cursor = conn.execute(sql, params)
        conn.commit()
        return int(cursor.lastrowid)


@retry_sql_locked()
def _run_log_write(sql: str, params: Tuple[Any, ...], *, db_path: str = DB_PATH) -> None:  # noqa: D401
    """INSERT/UPDATE con reintento cuando la base esté bloqueada."""
    with sqlite3.connect(db_path, timeout=15) as conn:
        conn.execute(sql, params)
        conn.commit()


def _duplicar_forecast_reasignacion(
    slp_origen: int,
    slp_destino: int,
    cardcodes: List[str],
) -> None:
    """Transacción compleja: duplica registros Forecast conservando histórico."""
    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        try:
            conn.execute("BEGIN")
            for cc in cardcodes:
                old_ids = [
                    row[0]
                    for row in conn.execute(
                        """
                        SELECT DISTINCT ForecastID
                        FROM Forecast_Detalle
                        WHERE SlpCode = ? AND CardCode = ?
                        """,
                        (slp_origen, cc),
                    ).fetchall()
                ]
                for old_id in old_ids:
                    new_id = conn.execute(
                        """
                        INSERT INTO Forecast (SlpCode, Fecha_Carga)
                        VALUES (?, datetime('now','localtime'))
                        """,
                        (slp_destino,),
                    ).lastrowid
                    conn.execute(
                        """
                        INSERT INTO Forecast_Detalle (
                            ForecastID, SlpCode, Linea, ItemCode, CardCode,
                            OcrCode3, FechEntr, Cant, PrecioUN, DocCur, TipoForecast
                        )
                        SELECT
                            ?, ?, Linea, ItemCode, CardCode,
                            OcrCode3, FechEntr, Cant, PrecioUN, DocCur, TipoForecast
                        FROM Forecast_Detalle
                        WHERE ForecastID = ? AND SlpCode = ? AND CardCode = ?
                        """,
                        (new_id, slp_destino, old_id, slp_origen, cc),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

# ---------------------------------------------------------------------------
# ✨ API pública
# ---------------------------------------------------------------------------
__all__ = [
    # núcleo
    "run_query",
    "_execute_write",
    # selects
    "_run_admin_select",
    "_run_forecast_select",
    "_run_product_select",
    "_run_client_select",
    "_run_vendor_select",
    "_run_reasig_select",
    "_run_cf_select",
    "_run_tab_select",
    "_run_gestion_select",
    "_run_home_select",
    # writes
    "_run_admin_insert",
    "_run_forecast_write",
    "_run_product_insert",
    "_run_client_insert",
    "_run_vendor_insert",
    # especiales
    "_run_log_to_sql",
    "_run_forecast_insert_get_id",
    "_run_log_write",
    "_duplicar_forecast_reasignacion",
]
