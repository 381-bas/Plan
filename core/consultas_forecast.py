# B_FCS001: Importaciones y configuración de base de datos para consultas forecast
# # ∂B_FCS001/∂B0
import pandas as pd
from utils.db import run_query
from typing import Any
from utils.db import (
    DB_PATH,
    _run_cf_select
)
from typing import Optional


# B_FCS002: Obtener lista de vendedores activos
# # ∂B_FCS002/∂B0
def obtener_vendedores(db_path=DB_PATH):
    query = """
    SELECT DISTINCT f.SlpCode, o.SlpName AS Nombre
    FROM Forecast f
    LEFT JOIN OSLP o ON f.SlpCode = o.SlpCode
    ORDER BY f.SlpCode
    """
    return run_query(query, db_path)

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
def obtener_forecast_historico(slp_code, card_code, db_path=DB_PATH):
    query = """
    SELECT f.Fecha_Carga, fd.FechEntr, fd.ItemCode, i.ItemName,
           fd.TipoForecast, fd.Cant
    FROM Forecast f
    JOIN Forecast_Detalle fd ON f.ForecastID = fd.ForecastID
    LEFT JOIN OITM i ON fd.ItemCode = i.ItemCode
    WHERE f.SlpCode = ? AND fd.CardCode = ?
      AND UPPER(fd.TipoForecast) IN ('FIRME', 'PROYECTADO')
    ORDER BY fd.FechEntr
    """
    return run_query(query, db_path, (slp_code, card_code))

# B_FCS005: Obtener forecast editable para edición directa por cliente
# ∂B_FCS005/∂B0
def obtener_forecast_editable(
    slp_code: int,
    card_code: str,
    anio: int | None = None,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Devuelve el forecast editable (cantidad, precio UN, moneda) en formato ancho 01-12.

    Parámetros
    ----------
    slp_code   : vendedor (SlpCode)
    card_code  : cliente (CardCode)
    anio       : año a filtrar; si es None trae todos
    db_path    : ruta a la BD SQLite
    """
    # ────────────────────────────── filtros reutilizables ──────────────────────────────
    filtro_anio = "AND strftime('%Y', FechEntr) = ?" if anio else ""
    # mismos filtros se usan en CTE base y en SELECT final
    # ────────────────────────────────── query SQL ──────────────────────────────────────
    query = f"""
    WITH base AS (                           -- 1️⃣ registros del detalle
        SELECT
            ItemCode,
            TipoForecast,
            OcrCode3,
            CAST(strftime('%m', FechEntr) AS TEXT) AS Mes,
            Cant,
            ForecastID
        FROM Forecast_Detalle
        WHERE SlpCode  = ?
          AND CardCode = ?
          {filtro_anio}
    ),
    ultimo AS (                              -- 2️⃣ sólo la última versión por clave
        SELECT *
        FROM (
            SELECT
                base.*,
                ROW_NUMBER() OVER (
                    PARTITION BY ItemCode, TipoForecast, OcrCode3, Mes
                    ORDER BY ForecastID DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),
    catalogo AS (                            -- 3️⃣ item × tipo
        SELECT
            i.ItemCode,
            i.ItemName,
            tf.TipoForecast
        FROM OITM i
        CROSS JOIN (
            SELECT 'Firme'      AS TipoForecast
            UNION ALL
            SELECT 'Proyectado' AS TipoForecast
        ) tf
    )
    SELECT
        c.ItemCode,
        c.ItemName,
        c.TipoForecast,
        fd.OcrCode3,
        CAST(strftime('%m', fd.FechEntr) AS INTEGER) AS Mes,
        SUM(fd.Cant)     AS Cantidad,
        AVG(fd.PrecioUN) AS PrecioUN,
        MAX(fd.DocCur)   AS DocCur
    FROM catalogo c
    LEFT JOIN Forecast_Detalle fd            -- sólo registros del forecast vigente
           ON  c.ItemCode     = fd.ItemCode
           AND c.TipoForecast = fd.TipoForecast
    JOIN  ultimo u                           -- garantie versión más reciente
           ON  fd.ForecastID   = u.ForecastID
           AND fd.ItemCode     = u.ItemCode
           AND fd.TipoForecast = u.TipoForecast
           AND fd.OcrCode3     = u.OcrCode3
           AND CAST(strftime('%m', fd.FechEntr) AS TEXT) = u.Mes
    WHERE  fd.CardCode = ?
      {filtro_anio}
    GROUP BY
        c.ItemCode, c.ItemName, c.TipoForecast, fd.OcrCode3, Mes
    ORDER BY
        c.ItemCode, c.TipoForecast, Mes;
    """

    # ─────────────────── parámetros en el mismo orden que los ? ────────────────────
    params: list[Any] = [slp_code, card_code]
    if anio:
        params.append(str(anio))   # CTE base
    params.append(card_code)       # WHERE final
    if anio:
        params.append(str(anio))   # WHERE final

    df = run_query(query, db_path, tuple(params))

    # ────────────────────────────── pivot + enriquecimiento ──────────────────────────
    if df.empty:
        return pd.DataFrame(columns=[
            "ItemCode", "ItemName", "TipoForecast", "Métrica",
            "OcrCode3", "PrecioUN", "DocCur",
            *[str(m).zfill(2) for m in range(1, 13)],
        ])

    pivot = (
        df.pivot_table(
            index=["ItemCode", "ItemName", "TipoForecast", "OcrCode3"],
            columns="Mes",
            values="Cantidad",
            fill_value=0,
        )
        .reset_index()
    )

    # PrecioUN y DocCur (una sola fila por clave ⇒ avg / first son seguros)
    precio_un = df.groupby(
        ["ItemCode", "TipoForecast", "OcrCode3"], as_index=False
    )["PrecioUN"].mean()
    doc_cur = df.groupby(
        ["ItemCode", "TipoForecast", "OcrCode3"], as_index=False
    )["DocCur"].first()

    pivot = (
        pivot.merge(precio_un, on=["ItemCode", "TipoForecast", "OcrCode3"])
             .merge(doc_cur, on=["ItemCode", "TipoForecast", "OcrCode3"])
    )

    # Normaliza nombres 01-12
    pivot.columns = [
        str(c).zfill(2) if isinstance(c, int) else c for c in pivot.columns
    ]
    for m in range(1, 13):
        col = str(m).zfill(2)
        if col not in pivot.columns:
            pivot[col] = 0

    pivot["Métrica"] = "Cantidad"

    orden = (
        ["ItemCode", "ItemName", "TipoForecast", "Métrica", "OcrCode3"]
        + [str(m).zfill(2) for m in range(1, 13)]
        + ["PrecioUN", "DocCur"]
    )
    return pivot[orden]



# B_FCS006: Consulta de stock disponible para lista de ítems
# # ∂B_FCS006/∂B0
def obtener_stock(itemcodes, db_path=DB_PATH):
    if not itemcodes:
        return pd.DataFrame()
    placeholders = ','.join('?' * len(itemcodes))
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
    placeholders = ','.join('?' * len(itemcodes))
    query = f"""
    SELECT r.DocDate, r.ItemCode, i.ItemName, r.Dscription, r.Quantity,
           r.LineTotal, r.LineStatus, o.Comments
    FROM RDR1 r
    JOIN ORDR o ON r.DocEntry = o.DocEntry
    LEFT JOIN OITM i ON r.ItemCode = i.ItemCode
    WHERE o.CardCode = ? AND r.ItemCode IN ({placeholders})
    """
    params = [cardcode] + itemcodes
    if line_status in ('O', 'C'):
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
    df["PrecioUnitario"] = pd.to_numeric(df["PrecioUnitario"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
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

    df['MesNombre'] = df['Mes'].apply(lambda x: f"{x:02d}")
    df['Columna'] = df['Anio'].astype(str) + "-" + df['MesNombre']

    pivot_df = df.pivot_table(index=["ItemCode", "ItemName"], columns="Columna", values="Cantidad", fill_value=0)
    return pivot_df.sort_index(axis=1).reset_index()


# B_FCS013: Consulta de nombres y códigos de vendedores
# # ∂B_FCS013/∂B0
def obtener_nombre_vendedor(db_path: str):
    return _run_cf_select("SELECT SlpCode, SlpName FROM OSLP")





