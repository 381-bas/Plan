"""utils/db.py – núcleo flexible E1+legacy
================================================
Este módulo centraliza el acceso a SQLite siguiendo RATUC‑F.
Provide flexible helper functions that support both legacy and new call signatures so that no downstream code breaks while we migrate.
"""

from __future__ import annotations
from contextlib import contextmanager
from threading import Lock
import sqlite3
import pandas as pd
import time
from functools import wraps
from typing import Any, Callable, List, Tuple, Iterator
import os
from typing import Optional

from contextlib import closing

OperationalError = sqlite3.OperationalError
DatabaseError = sqlite3.DatabaseError


# B_CTX001: Configuración global de base de datos y timeouts
# # ∂B_CTX001/∂B0
DB_PATH: str = (
    "C:/Users/qmkbantiman/OneDrive - QMK SPA/Informacion/quickpilot/Plan_Final_Final_Final.db"
)

DB_PATH = os.getenv("DB_PATH", DB_PATH)  # usa el ya definido en tu módulo
BUSY_MS = 30_000


# B_DBQ001: SELECT flexible compatible (legacy y moderno)
# # ∂B_DBQ001/∂B0
def run_query(sql: str, *args: Any, **kwargs: Any) -> pd.DataFrame:  # noqa: C901
    """
    Ejecuta un SELECT y devuelve un DataFrame.

    Firmas soportadas (compatibilidad retro):
      1) legacy:   run_query(sql, DB_PATH, params)
      2) moderna:  run_query(sql, params=..., db_path=DB_PATH)
      3) atajo:    run_query(sql, DB_PATH)  -> params=()

    Notas:
      - Acepta db_path como str u os.PathLike.
      - Rechaza 'params' tipo lista de tuplas/listas (patrón de executemany).
    """
    db_path: str = kwargs.pop("db_path", DB_PATH)
    params: Tuple[Any, ...] | List[Any] | dict | None

    if args:
        if len(args) == 1:
            # Puede ser db_path o params
            if isinstance(args[0], (str, os.PathLike)):
                db_path = str(args[0])
                params = kwargs.pop("params", ())
            else:
                params = args[0]  # type: ignore[assignment]
        elif len(args) == 2:
            db_path, params = args  # type: ignore[assignment]
            if isinstance(db_path, os.PathLike):
                db_path = str(db_path)
        else:
            raise TypeError(
                f"run_query() esperaba ≤3 posicionales, recibió {len(args)+1}."
            )
    else:
        params = kwargs.pop("params", ())

    if kwargs:
        raise TypeError(f"run_query() argumentos inesperados: {list(kwargs)}")

    # Normalización mínima
    params = params or ()

    # Defensa blanda: SELECT no usa 'lista de tuplas' (eso es patrón de executemany)
    if isinstance(params, list) and params and isinstance(params[0], (tuple, list)):
        raise TypeError(
            "run_query(): 'params' no puede ser lista de tuplas/listas; "
            "usa _execute_write(many=True) para escrituras."
        )

    with _get_conn(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params)


# B_INS001: Escrituras genéricas (execute/executemany) con transacción
# # ∂B_INS001/∂B0
def _execute_write(
    sql: str,
    params: Tuple[Any, ...] | List[Tuple[Any, ...]] | None = None,
    *,
    many: bool = False,
    timeout: int = 15,
    db_path: str = DB_PATH,
) -> None:
    params = params or ()
    with tx(db_path, immediate=True, timeout=timeout) as conn:
        (conn.executemany if many else conn.execute)(sql, params)  # type: ignore[arg-type]


# B_AUX001: Decorador de reintento ante 'database is locked'
# # ∂B_AUX001/∂B0
def retry_sql_locked(
    max_attempts: int = 5, delay: float = 0.4
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Reintenta cuando la base está bloqueada (*database is locked*)."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        def _wrapped(*a: Any, **kw: Any):
            for attempt in range(max_attempts):
                try:
                    return fn(*a, **kw)
                except sqlite3.OperationalError as exc:  # pragma: no cover
                    if (
                        "database is locked" in str(exc).lower()
                        and attempt < max_attempts - 1
                    ):
                        time.sleep(delay)
                    else:
                        raise

        return _wrapped

    return decorator


# B_DBQ002: Wrappers de SELECT compatibles con run_query
# # ∂B_DBQ002/∂B0
def _run_forecast_select(
    sql: str, params: tuple | None = None, db_path: str = DB_PATH
) -> pd.DataFrame:
    """Select genérico (mantiene API antigua, añade db_path opcional)."""
    return run_query(sql, db_path, params=params or ())


def _run_admin_select(sql, params=None):
    return run_query(sql, params=params or ())


def _run_product_select(sql, params=None):
    return run_query(sql, params=params or ())


def _run_client_select(sql, params=None):
    return run_query(sql, params=params or ())


def _run_vendor_select(sql, params=None):
    return run_query(sql, params=params or ())


def _run_reasig_select(sql, params=None):
    return run_query(sql, params=params or ())


def _run_cf_select(sql, params=None):
    return run_query(sql, params=params or ())


def _run_tab_select(sql, params=None):
    return run_query(sql, params=params or ())


def _run_gestion_select(sql, params=None):
    return run_query(sql, params=params or ())


def _run_home_select(sql, params=None):
    return run_query(sql, params=params or ())


# B_INS005: Aliases de escrituras para compatibilidad
# # ∂B_INS005/∂B0
_run_admin_insert = _execute_write
_run_forecast_write = _execute_write
_run_product_insert = _execute_write
_run_client_insert = _execute_write
_run_vendor_insert = _execute_write


# B_LOG001: Inserción de DataFrame en tabla de logs (to_sql)
# # ∂B_LOG001/∂B0
def _run_log_to_sql(df: pd.DataFrame, table: str, *, db_path: str = DB_PATH) -> None:
    if df.empty:
        return
    with tx(db_path, immediate=False, timeout=15) as conn:
        df.to_sql(table, conn, if_exists="append", index=False)


# B_INS002: INSERT con retorno de lastrowid para Forecast
# # ∂B_INS002/∂B0
def _run_forecast_insert_get_id(
    sql: str, params: Tuple[Any, ...], *, timeout: int = 15
) -> int:
    """INSERT y devuelve `lastrowid`."""
    with tx(DB_PATH, immediate=True, timeout=timeout) as conn:
        cursor = conn.execute(sql, params)
        return int(cursor.lastrowid)


# B_INS003: Duplicación transaccional de forecast por reasignación
# # ∂B_INS003/∂B0
def _duplicar_forecast_reasignacion(
    slp_origen: int,
    slp_destino: int,
    cardcodes: List[str],
) -> None:
    """Transacción compleja: duplica registros Forecast conservando histórico."""
    with tx(DB_PATH, immediate=True, timeout=30) as conn:
        for cc in cardcodes:
            # ForecastIDs origen que pertenecen al vendedor y cliente indicados
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
                # Nueva cabecera para el vendedor destino
                new_id = conn.execute(
                    """
                    INSERT INTO Forecast (SlpCode, Fecha_Carga)
                    VALUES (?, datetime('now','localtime'))
                    """,
                    (slp_destino,),
                ).lastrowid

                # Duplicar el detalle apuntando al nuevo ForecastID y SlpCode destino
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


# Registro de inicialización por base de datos (evita ejecutar PRAGMAs repetidamente)
# B_CTX007: Registro de inicialización WAL por base de datos
# # ∂B_CTX007/∂B0
__WAL_INIT = {}
__WAL_LOCK = Lock()


# B_CTX002: Inicialización única de WAL y PRAGMAs por base de datos
# # ∂B_CTX002/∂B0
def _init_wal_once(db_path: str) -> None:
    """
    Inicializa la base en modo WAL y PRAGMAs globales una sola vez por `db_path`.
    Idempotente y thread-safe.
    """
    if not db_path:
        raise ValueError("DB path requerido")

    with __WAL_LOCK:
        if __WAL_INIT.get(db_path):
            return

        # Asegura el directorio destino si corresponde
        dir_ = os.path.dirname(db_path)
        if dir_ and not os.path.exists(dir_):
            os.makedirs(dir_, exist_ok=True)

        # Conexión corta para setear PRAGMAs globales
        conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
        try:
            cur = conn.cursor()
            # Journal WAL y parámetros razonables para escritorio / Streamlit
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.execute("PRAGMA busy_timeout=30000;")  # 30s
            __WAL_INIT[db_path] = True
        finally:
            conn.close()


# B_TXN001: Context manager de transacciones (BEGIN/COMMIT/ROLLBACK)
# # ∂B_TXN001/∂B0
@contextmanager
def tx(
    db_path: str = DB_PATH, *, immediate: bool = True, timeout: int = 30
) -> Iterator[sqlite3.Connection]:
    """
    Abre una conexión configurada y maneja BEGIN/COMMIT/ROLLBACK de forma central.
    immediate=True => BEGIN IMMEDIATE (reduce write contention).
    """
    _init_wal_once(db_path)  # asegura WAL/timeout globales 1 sola vez
    conn = sqlite3.connect(db_path, timeout=timeout, isolation_level=None)
    try:
        conn.execute(f"PRAGMA busy_timeout={BUSY_MS};")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        if immediate:
            conn.execute("BEGIN IMMEDIATE;")
        else:
            conn.execute("BEGIN;")
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


# B_DBQ003: Select genérico con db_path explícito
# # ∂B_DBQ003/∂B0
def _read_sql(db_path: str, sql: str, params: tuple = ()) -> pd.DataFrame:
    return run_query(sql, db_path, params=params)


# B_INS004: Mantenimiento — deduplicación por clave real en Forecast_Detalle
# # ∂B_INS004/∂B0
def deduplicar_detalle_por_clave(conn: sqlite3.Connection) -> int:
    """
    Limpia duplicados en Forecast_Detalle usando la clave REAL de BD:
    (ForecastID, CardCode, ItemCode, TipoForecast, OcrCode3, FechEntr)
    Conserva la mejor fila por clave (preferimos la que tenga más datos no nulos).
    Retorna el número de filas eliminadas.
    """
    cur = conn.cursor()

    # 1) Detectar claves duplicadas
    sql_dups = """
    SELECT ForecastID, CardCode, ItemCode, TipoForecast, OcrCode3, FechEntr, COUNT(*) as c
    FROM Forecast_Detalle
    GROUP BY ForecastID, CardCode, ItemCode, TipoForecast, OcrCode3, FechEntr
    HAVING c > 1;
    """
    dups = cur.execute(sql_dups).fetchall()
    if not dups:
        print("[DUP.INFO] sin duplicados por clave")
        return 0

    print(f"[DUP.INFO] claves duplicadas={len(dups)}")

    # 2) Por cada clave, elegir un ROWID 'ganador' (más completo) y borrar el resto
    total_del = 0
    keep_q = """
    SELECT rowid
    FROM Forecast_Detalle
    WHERE ForecastID=? AND CardCode=? AND ItemCode=? AND TipoForecast=? AND OcrCode3=? AND FechEntr=?
    ORDER BY (Cant IS NULL) ASC, (PrecioUN IS NULL) ASC, rowid DESC
    LIMIT 1;
    """
    del_q = """
    DELETE FROM Forecast_Detalle
    WHERE ForecastID=? AND CardCode=? AND ItemCode=? AND TipoForecast=? AND OcrCode3=? AND FechEntr=?
      AND rowid <> ?;
    """

    # Si no hay transacción externa, abrimos una local; si ya hay, reutilizamos
    local_tx = not getattr(conn, "in_transaction", False)
    if local_tx:
        cur.execute("BEGIN IMMEDIATE;")
        print("[SQL.BEGIN] BEGIN IMMEDIATE (dedup)")

    try:
        for fid, card, item, tipo, linea, fch, _c in dups:
            keep_rowid = cur.execute(
                keep_q, (fid, card, item, tipo, linea, fch)
            ).fetchone()
            if not keep_rowid:
                continue
            keep_rowid = keep_rowid[0]
            cur.execute(del_q, (fid, card, item, tipo, linea, fch, keep_rowid))
            total_del += cur.rowcount

        if local_tx:
            conn.commit()
            print(f"[DUP.OK] eliminadas={total_del}")
        else:
            print(f"[DUP.OK] (tx externa) eliminadas={total_del}")
    except Exception as e:
        if local_tx:
            conn.rollback()
            print("[SQL.ROLLBACK]")
        print(f"[DUP.ERROR] {e}")
        raise

    return total_del


# B_CTX003: Conexión con PRAGMAs consistentes (sin iniciar BEGIN)
# # ∂B_CTX003/∂B0
def _get_conn(db_path: str = DB_PATH, *, timeout: int = 30) -> sqlite3.Connection:
    """Conexión con PRAGMAs consistentes (sin iniciar BEGIN)."""
    _init_wal_once(db_path)
    conn = sqlite3.connect(db_path, timeout=timeout, isolation_level=None)
    conn.execute(f"PRAGMA busy_timeout={BUSY_MS};")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


# B_LOG002: Escritura de logs con reintento y manejo de many
# # ∂B_LOG002/∂B0
@retry_sql_locked()
def _run_log_write(
    sql: str,
    params: Tuple[Any, ...] | List[Tuple[Any, ...]] | dict | None = None,
    *,
    db_path: str = DB_PATH,
    many: bool | None = None,
    timeout: int = 15,
) -> None:
    """
    Compat: escritura de logs con reintento ante 'database is locked'.
    - Acepta tupla/dict => execute
    - Acepta lista de tuplas => executemany
    - Si 'many' viene explícito, fuerza el modo.
    """
    params = params or ()
    if many is None:
        many = (
            isinstance(params, list) and params and isinstance(params[0], (list, tuple))
        )
    with tx(db_path, immediate=True, timeout=timeout) as conn:
        if many:
            conn.executemany(sql, params)  # type: ignore[arg-type]
        else:
            conn.execute(sql, params)  # type: ignore[arg-type]


# B_CTX004: Conexión cruda (compat) con inicialización de WAL/PRAGMAs
# # ∂B_CTX004/∂B0
def connect(
    db_path: str = DB_PATH,
    *,
    timeout: float = 30.0,
    isolation_level: str | None = None,
) -> sqlite3.Connection:
    """
    Compat wrapper: inicializa WAL/PRAGMAs y retorna una conexión cruda.
    Útil cuando un consumidor usa `closing()` o maneja la conexión manualmente.
    """
    _init_wal_once(str(db_path))
    return sqlite3.connect(
        str(db_path), timeout=timeout, isolation_level=isolation_level
    )


# B_CTX005: Utilidad de rango de año (YYYY-01-01, YYYY+1-01-01)
# # ∂B_CTX005/∂B0
def year_range(anio: Optional[int]) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (ini, fin) como 'YYYY-01-01' y 'YYYY+1-01-01'.
    Si anio es None o no convertible a int → (None, None).
    """
    if anio is None:
        return None, None
    try:
        y = int(anio)
    except (TypeError, ValueError):
        return None, None
    return f"{y:04d}-01-01", f"{y+1:04d}-01-01"


# B_DBQ004: Inyección de filtro de año en SQL base
# # ∂B_DBQ004/∂B0
def add_year_filter(sql_base: str, anio: Optional[int], *, col: str = "FechEntr"):
    """
    Devuelve (sql, params_extra) agregando AND … >= ? AND … < ? si hay año válido.
    """
    ini, fin = year_range(anio)
    if ini is None:
        return sql_base, ()
    return f"{sql_base} AND {col} >= ? AND {col} < ?", (ini, fin)


# B_DBQ005: Consulta del MAX(ForecastID) visible por rango de año
# # ∂B_DBQ005/∂B0
def _forecast_id_visible(
    slpcode: int,
    cardcode: str,
    anio: int,
    *,
    db_path: str = DB_PATH,
) -> int | None:
    """
    Devuelve el MAX(ForecastID) visible del año acotado por [anio-01-01, (anio+1)-01-01).
    """
    base = """
    SELECT
        MAX(fd.ForecastID) AS MaxID
    FROM Forecast_Detalle fd
    WHERE
        fd.SlpCode  = ? AND
        fd.CardCode = ?
    """
    # inserta filtro de año sobre fd.FechEntr
    q, pextra = add_year_filter(base, anio, col="fd.FechEntr")
    params = (slpcode, cardcode, *pextra)

    try:
        df = run_query(q, db_path, params)
        if df.empty:
            return None
        # tolerante al nombre de columna devuelta por sqlite/pandas
        col = next((c for c in df.columns if c.lower() == "maxid"), None)
        if not col:
            print("[FID][warn] columna MaxID no presente en resultado")
            return None
        val = df.at[0, col]
        return None if pd.isna(val) else int(val)
    except Exception as e:
        print(f"[FID][warn] {e}")
        return None


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def query_scalar(db_path: str, sql: str, params=(), default=None):
    with closing(_connect(db_path)) as con, closing(con.cursor()) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else default


def execute(db_path: str, sql: str, params=()) -> int:
    with closing(_connect(db_path)) as con, closing(con.cursor()) as cur:
        cur.execute(sql, params)
        con.commit()
        return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# ✨ API pública
# ---------------------------------------------------------------------------
# B_CTX006: Exportación de API pública del módulo
# # ∂B_CTX006/∂B0
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
    "_duplicar_forecast_reasignacion",
]

__all__.extend(
    [
        "DB_PATH",
        "tx",
        "OperationalError",
        "DatabaseError",
        "deduplicar_detalle_por_clave",
        "_run_log_write",
        "connect",
        "year_range",
        "add_year_filter",
        "_forecast_id_visible",
    ]
)
