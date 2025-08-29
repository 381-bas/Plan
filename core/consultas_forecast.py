# B_FCS001: Importaciones y configuración de base de datos para consultas forecast
# # ∂B_FCS001/∂B0
from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Optional, Union
import sqlite3
import numpy as np
from utils.db import run_query
from utils.db import DB_PATH, _run_cf_select


# B_FCS003: Obtener clientes asociados a un vendedor
# # ∂B_FCS003/∂B0
def obtener_clientes(slpcode, db_path=DB_PATH):
    query = """
    SELECT DISTINCT fd.CardCode, c.CardName AS Nombre
    FROM Forecast f
    JOIN Forecast_Detalle fd ON f.ForecastID = fd.ForecastID
    LEFT JOIN OCRD c ON fd.CardCode = c.CardCode
    WHERE f.SlpCode = ?
    ORDER BY fd.CardCode
    """
    return run_query(query, db_path, (slpcode,))


# B_FCS004: Obtener forecast histórico detallado por vendedor y cliente
# # ∂B_FCS004/∂B0
# core/consultas_forecast.py
def obtener_forecast_historico(
    slpcode: int,
    cardcode: str,
    anio: Optional[int] = None,
    db_path: Union[str, Path] = "data.db",
) -> pd.DataFrame:
    """
    Lee histórico de forecast para un vendedor (slpcode) y cliente (cardcode),
    trayendo Cant y PrecioUN desde BD, derivando Mes desde FechEntr y
    filtrando opcionalmente por año (FechEntr).
    """

    print("[CFH-001] ▶ start obtener_forecast_historico")
    print(
        f"[CFH-001] args: slpcode={slpcode}({type(slpcode).__name__}), "
        f"cardcode={type(cardcode).__name__}, anio={anio}, db_path={db_path}"
    )

    # ── Guards de tipo
    if not isinstance(slpcode, (int, np.integer)):
        raise TypeError("[CFH-001] slpcode debe ser int")
    if not isinstance(cardcode, str):
        raise TypeError("[CFH-001] cardcode debe ser str (no DataFrame, no dict, etc.)")
    if anio is not None and not isinstance(anio, (int, np.integer)):
        raise TypeError("[CFH-001] anio debe ser int o None")

    # ── Filtro opcional por año en FechEntr
    anio_filter = "AND strftime('%Y', fd.FechEntr) = ?" if anio is not None else ""

    # ── SQL (sin f.Anio, sin fd.Mes)
    sql = f"""
        WITH base AS (
            SELECT
                fd.SlpCode                              AS SlpCode,
                fd.ItemCode                              AS ItemCode,
                i.ItemName                               AS ItemName,
                fd.CardCode                              AS CardCode,
                fd.TipoForecast                          AS TipoForecast,
                fd.OcrCode3                              AS OcrCode3,
                fd.DocCur                                AS DocCur,
                strftime('%m', fd.FechEntr)              AS Mes,
                fd.Cant                                  AS Cant,
                fd.PrecioUN                              AS PrecioUN
            FROM Forecast_Detalle fd
            LEFT JOIN OITM i ON i.ItemCode = fd.ItemCode
            WHERE fd.SlpCode = ?
              AND fd.CardCode = ?
              {anio_filter}
        )
        SELECT *
        FROM base
        ORDER BY ItemCode, TipoForecast, OcrCode3, DocCur, Mes
    """.strip()

    params = [int(slpcode), cardcode]
    if anio is not None:
        params.append(str(int(anio)))  # str para el strftime('%Y', ...)
    print(f"[CFH-001] sql params: {tuple(params)}")

    expected_cols = [
        "SlpCode",
        "ItemCode",
        "ItemName",
        "CardCode",
        "TipoForecast",
        "OcrCode3",
        "DocCur",
        "Mes",
        "Cant",
        "PrecioUN",
    ]
    expected_dtypes = {
        "SlpCode": "Int64",
        "ItemCode": "string",
        "ItemName": "string",
        "CardCode": "string",
        "TipoForecast": "string",
        "OcrCode3": "string",
        "DocCur": "string",
        "Mes": "string",
        "Cant": "float64",
        "PrecioUN": "float64",
    }

    # ── Ejecución
    db_path = str(db_path)
    with sqlite3.connect(db_path) as con:
        df = pd.read_sql_query(sql, con, params=params)

    print(f"[CFH-001] raw result shape: {df.shape}")

    # ── Asegurar columnas esperadas
    for c in expected_cols:
        if c not in df.columns:
            df[c] = pd.Series(
                dtype="float64" if c in ("Cant", "PrecioUN") else "object"
            )

    # ── Normalizaciones de tipo
    df["Mes"] = df["Mes"].astype(str).str.zfill(2)

    for c in ("Cant", "PrecioUN"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ("ItemCode", "ItemName", "CardCode", "TipoForecast", "OcrCode3", "DocCur"):
        df[c] = df[c].astype("string")

    df["SlpCode"] = pd.to_numeric(df["SlpCode"], errors="coerce").astype("Int64")

    # ── Reordenar columnas / ordenar filas
    df = (
        df[expected_cols]
        .sort_values(
            ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Mes"], kind="stable"
        )
        .reset_index(drop=True)
    )

    # ── Cast final (si hay filas)
    if not df.empty:
        for col, dt in expected_dtypes.items():
            try:
                if dt == "Int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dt == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
                elif dt == "string":
                    df[col] = df[col].astype("string")
            except Exception as e:
                print(f"[CFH-001][warn] dtype cast falló en {col} -> {dt}: {e}")

    # ── Prints de diagnóstico
    present = list(df.columns)
    missing = [c for c in expected_cols if c not in present]
    print(f"[CFH-001] columns present: {present}")
    print(f"[CFH-001] columns missing: {missing or '—'}")
    print(
        f"[CFH-001] nunique: ItemCode={df['ItemCode'].nunique(dropna=True)}, "
        f"TipoForecast={df['TipoForecast'].nunique(dropna=True)}, Mes={df['Mes'].nunique(dropna=True)}"
    )
    print(
        f"[CFH-001] PrecioUN nulls: {int(df['PrecioUN'].isna().sum())} / {len(df)}  | "
        f"Cant nulls: {int(df['Cant'].isna().sum())} / {len(df)}"
    )

    print("[CFH-001] ✅ done obtener_forecast_historico")
    return df


# B_FCS006: Consulta de stock disponible para lista de ítems
# # ∂B_FCS006/∂B0
def obtener_stock(itemcodes, db_path=DB_PATH):
    if not itemcodes:
        return pd.DataFrame()
    placeholders = ",".join("?" * len(itemcodes))
    query = f"""
    SELECT ItemCode, ItemName, WhsCode, Stock_Total, Stock_Lote, Lote, FechaVenc
    FROM Stock
    WHERE ItemCode IN ({placeholders})
    ORDER BY ItemCode, WhsCode
    """
    return run_query(query, db_path, tuple(itemcodes))


# B_FCS007: Consulta de órdenes de venta por cliente e ítems, con filtro opcional de estado
# # ∂B_FCS007/∂B0
def obtener_ordenes_venta(cardcode, itemcodes, line_status=None, db_path=DB_PATH):
    if not itemcodes:
        return pd.DataFrame()
    placeholders = ",".join("?" * len(itemcodes))
    query = f"""
    SELECT r.DocDate, r.ItemCode, i.ItemName, r.Dscription, r.Quantity,
           r.LineTotal, r.LineStatus, o.Comments
    FROM RDR1 r
    JOIN ORDR o ON r.DocEntry = o.DocEntry
    LEFT JOIN OITM i ON r.ItemCode = i.ItemCode
    WHERE o.CardCode = ? AND r.ItemCode IN ({placeholders})
    """
    params = [cardcode] + itemcodes
    if line_status in ("O", "C"):
        query += " AND r.LineStatus = ?"
        params.append(line_status)
    query += " ORDER BY r.DocDate DESC"
    return run_query(query, db_path, tuple(params))


# B_FCS008: Consulta de precios unitarios vigentes
# # ∂B_FCS008/∂B0
def obtener_precios_unitarios(db_path=DB_PATH):
    query = """
    SELECT ItemCode, PrecioUnitario
    FROM precios_base
    WHERE PrecioUnitario IS NOT NULL
    """
    df = run_query(query, db_path)
    df["PrecioUnitario"] = pd.to_numeric(
        df["PrecioUnitario"].astype(str).str.replace(",", "."), errors="coerce"
    ).fillna(0)
    return df


# B_FCS009: Consulta de forecast por mes (resumen ejecutivo)
# # ∂B_FCS009/∂B0
def obtener_forecast_mes(db_path: str, anio: int, mes: int):
    query = """
        SELECT f.SlpCode, d.OcrCode3, d.CardCode, d.ItemCode,
               SUM(d.Cant) AS Total
        FROM Forecast f
        JOIN Forecast_Detalle d ON f.ForecastID = d.ForecastID
        WHERE strftime('%Y-%m', d.FechEntr) = ? || '-' || ?
        GROUP BY f.SlpCode, d.OcrCode3, d.CardCode, d.ItemCode
    """
    return _run_cf_select(query, (str(anio), f"{mes:02d}"))


# B_FCS010: Consulta de ventas reales totales por mes
# # ∂B_FCS010/∂B0
def obtener_ventas_mes(db_path: str, anio: int, mes: int):
    query = """
        SELECT r.SlpCode, r.OcrCode3 AS Linea, o.CardCode, r.ItemCode,
               SUM(r.LineTotal) AS Total
        FROM RDR1 r
        JOIN ORDR o ON r.DocEntry = o.DocEntry
        WHERE strftime('%Y-%m', r.DocDate) = ? || '-' || ?
        GROUP BY r.SlpCode, r.OcrCode3, o.CardCode, r.ItemCode
    """
    return _run_cf_select(query, (str(anio), f"{mes:02d}"))


# B_FCS011: Consulta histórica de ventas por cliente e ítem
# # ∂B_FCS011/∂B0
def obtener_historico_ventas(card_code: str, db_path: str = DB_PATH) -> pd.DataFrame:
    query = """
        SELECT r.ItemCode, i.ItemName,
               CAST(STRFTIME('%m', o.DocDate) AS INTEGER) AS Mes,
               CAST(STRFTIME('%Y', o.DocDate) AS INTEGER) AS Anio,
               SUM(r.Quantity) AS Cantidad
        FROM RDR1 r
        JOIN ORDR o ON r.DocEntry = o.DocEntry
        LEFT JOIN OITM i ON r.ItemCode = i.ItemCode
        WHERE o.CardCode = ?
        GROUP BY r.ItemCode, i.ItemName, Anio, Mes
        ORDER BY r.ItemCode, Anio, Mes
    """
    df = run_query(query, db_path, (card_code,))
    if df.empty:
        return df

    df["MesNombre"] = df["Mes"].apply(lambda x: f"{x:02d}")
    df["Columna"] = df["Anio"].astype(str) + "-" + df["MesNombre"]

    pivot_df = df.pivot_table(
        index=["ItemCode", "ItemName"],
        columns="Columna",
        values="Cantidad",
        fill_value=0,
    )
    return pivot_df.sort_index(axis=1).reset_index()


# B_FCS013: Consulta de nombres y códigos de vendedores
# # ∂B_FCS013/∂B0
def obtener_nombre_vendedor(db_path: str):
    return _run_cf_select("SELECT SlpCode, SlpName FROM OSLP")


def vendedores_con_forecast(db_path: str, anio: int) -> pd.DataFrame:
    """
    Lista de vendedores con forecast para el año `anio`.
    1) Primer intento SOLO con Forecast + OSLP usando Fecha_Carga.
    2) Fallback con EXISTS sobre Forecast_Detalle (FechEntr) si queda vacío o hay error.
    """
    y = str(anio)
    print(f"[VEND-LIST.INFO] Iniciando búsqueda de vendedores para el año {anio}")

    # --- 1) Solo Forecast + OSLP (evita dependencias de detalle)
    q1 = """
        SELECT DISTINCT f.SlpCode,
               COALESCE(s.SlpName, CAST(f.SlpCode AS TEXT)) AS SlpName
        FROM Forecast f
        LEFT JOIN OSLP s ON s.SlpCode = f.SlpCode
        WHERE strftime('%Y', f.Fecha_Carga) = ?
    """
    try:
        with sqlite3.connect(db_path) as con:
            df = pd.read_sql(q1, con, params=[y])
        print(f"[VEND-LIST.DEBUG] q1_rows={len(df)} cols={df.columns.tolist()}")
    except Exception as e:
        print(f"[VEND-LIST.WARN] q1 falló: {e}")
        df = pd.DataFrame()

    # --- 2) Fallback con detalle si no hay filas
    if df.empty:
        q2 = """
            SELECT DISTINCT f.SlpCode,
                   COALESCE(s.SlpName, CAST(f.SlpCode AS TEXT)) AS SlpName
            FROM Forecast f
            LEFT JOIN OSLP s ON s.SlpCode = f.SlpCode
            WHERE EXISTS (
                SELECT 1
                FROM Forecast_Detalle d
                WHERE d.ForecastID = f.ForecastID
                  AND strftime('%Y', d.FechEntr) = ?
            )
        """
        try:
            with sqlite3.connect(db_path) as con:
                df = pd.read_sql(q2, con, params=[y])
            print(f"[VEND-LIST.FALLBK] q2_rows={len(df)}")
        except Exception as e:
            print(f"[VEND-LIST.ERROR] Fallback q2 falló: {e}")
            return pd.DataFrame(columns=["SlpCode", "SlpName"])

    if "SlpName" not in df.columns:
        df["SlpName"] = df["SlpCode"].astype(str)

    df_final = df.sort_values(["SlpName", "SlpCode"]).reset_index(drop=True)
    print(f"[VEND-LIST.INFO] rows={len(df_final)} cols={df_final.columns.tolist()}")
    return df_final
