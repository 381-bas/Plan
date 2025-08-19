# B_FCS001: Importaciones y configuración de base de datos para consultas forecast
# # ∂B_FCS001/∂B0
import pandas as pd
from utils.db import run_query
from typing import Any
from utils.db import DB_PATH, _run_cf_select


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
    Devuelve el forecast editable en formato ancho 01-12, con Métrica ∈ {Cantidad, Precio}.
    """
    print(
        f"\n=== Inicio consulta forecast: SLP={slp_code}, Cliente={card_code}, Año={anio} ==="
    )

    filtro_anio = "AND strftime('%Y', fd.FechEntr) = ?" if anio else ""

    query = f"""
    WITH base AS (
        SELECT
            fd.ItemCode,
            fd.TipoForecast,
            fd.OcrCode3,
            CAST(strftime('%m', fd.FechEntr) AS TEXT) AS Mes,
            fd.Cant,
            fd.PrecioUN,
            fd.DocCur,
            fd.ForecastID
        FROM Forecast_Detalle fd
        WHERE fd.SlpCode  = ?
          AND fd.CardCode = ?
          {filtro_anio}
    ),
    rankeado AS (
        SELECT base.*,
               ROW_NUMBER() OVER (
                    PARTITION BY ItemCode, TipoForecast, OcrCode3, Mes
                    ORDER BY ForecastID DESC
               ) AS rn
        FROM base
    ),
    ultimo AS (
        SELECT * FROM rankeado WHERE rn = 1
    ),
    catalogo AS (
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
        u.OcrCode3,
        u.Mes AS Mes,                          -- puede venir NULL → se sanea en Python
        SUM(COALESCE(u.Cant,0))        AS Cantidad,
        AVG(COALESCE(u.PrecioUN,0))    AS PrecioUN,
        MAX(COALESCE(u.DocCur,'CLP'))  AS DocCur
    FROM catalogo c
    LEFT JOIN ultimo u
           ON  c.ItemCode     = u.ItemCode
           AND c.TipoForecast = u.TipoForecast
    GROUP BY
        c.ItemCode, c.ItemName, c.TipoForecast, u.OcrCode3, Mes
    ORDER BY
        c.ItemCode, c.TipoForecast, Mes;
    """

    params: list[Any] = [slp_code, card_code]
    if anio:
        params.append(str(anio))

    print(f"Ejecutando query con parámetros: {params}")
    df = run_query(query, db_path, tuple(params))
    print(f"Registros obtenidos: {len(df)}")

    # Skeleton vacío con dos métricas (Cantidad/Precio)
    cols_meses = [f"{m:02d}" for m in range(1, 13)]
    if df.empty:
        print("WARNING: DataFrame inicial vacío")
        return pd.DataFrame(
            columns=[
                "ItemCode",
                "ItemName",
                "TipoForecast",
                "Métrica",
                "OcrCode3",
                "DocCur",
                *cols_meses,
            ]
        )

    print("\nValidando mes y datos...")
    df["Mes"] = pd.to_numeric(df["Mes"], errors="coerce")
    print(f"Meses encontrados: {sorted(df['Mes'].unique().tolist())}")

    df = df[(df["Mes"] >= 1) & (df["Mes"] <= 12)]
    if df.empty:
        print("WARNING: DataFrame vacío después de filtrar meses")
        return pd.DataFrame(
            columns=[
                "ItemCode",
                "ItemName",
                "TipoForecast",
                "Métrica",
                "OcrCode3",
                "DocCur",
                *cols_meses,
            ]
        )

    df["Mes"] = df["Mes"].astype(int).astype(str).str.zfill(2)
    print("Conversión de mes completada")

    print("\nAplicando valores por defecto...")
    if "OcrCode3" in df.columns:
        df["OcrCode3"] = df["OcrCode3"].fillna("")
    if "DocCur" in df.columns:
        df["DocCur"] = df["DocCur"].fillna("CLP")
    if "PrecioUN" in df.columns:
        df["PrecioUN"] = pd.to_numeric(df["PrecioUN"], errors="coerce").fillna(0.0)
    if "Cantidad" in df.columns:
        df["Cantidad"] = pd.to_numeric(df["Cantidad"], errors="coerce").fillna(0.0)

    print("\nPreparando pivot tables...")
    df_cant = df.copy()
    df_cant["Métrica"] = "Cantidad"
    pivot_cant = df_cant.pivot_table(
        index=["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur", "Métrica"],
        columns="Mes",
        values="Cantidad",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    print(f"Pivot Cantidad shape: {pivot_cant.shape}")

    df_prec = df.copy()
    df_prec["Métrica"] = "Precio"
    pivot_prec = df_prec.pivot_table(
        index=["ItemCode", "ItemName", "TipoForecast", "OcrCode3", "DocCur", "Métrica"],
        columns="Mes",
        values="PrecioUN",
        aggfunc="last",
        fill_value=0,
    ).reset_index()
    print(f"Pivot Precio shape: {pivot_prec.shape}")

    df_metrico = pd.concat([pivot_cant, pivot_prec], ignore_index=True)
    print(f"\nShape final después de concat: {df_metrico.shape}")

    for mes in cols_meses:
        if mes not in df_metrico.columns:
            print(f"Agregando mes faltante: {mes}")
            df_metrico[mes] = 0

    # === NUEVO: asegurar líneas Proyectado (Cantidad=0) y Precio solo en meses con cantidad (Firme o Proyectado) ===
    print(
        "\nAsegurando filas 'Proyectado' para cada SKU (Cantidad=0, Precio=Firme SOLO en meses c/ cantidad)..."
    )
    MESES = cols_meses
    base_keys = ["ItemCode", "ItemName", "OcrCode3", "DocCur"]
    dfs = [df_metrico]

    grupos = df_metrico.groupby(base_keys, dropna=False)
    print(f"Grupos base encontrados: {len(grupos)}")

    faltantes_total = 0
    for keys, g in grupos:
        tipos = set(g["TipoForecast"].astype(str).unique().tolist())
        if "Firme" in tipos and "Proyectado" not in tipos:
            # Precio base desde Firme/Precio (si existe)
            precio_firme = g[
                (g["TipoForecast"] == "Firme") & (g["Métrica"] == "Precio")
            ]

            # Máscara de meses con cantidad > 0 en CUALQUIER tipo (Firme o Proyectado)
            qty_rows = g[g["Métrica"] == "Cantidad"]
            qty_mask = {}
            for m in MESES:
                has_qty = False
                for _, rq in qty_rows.iterrows():
                    try:
                        has_qty = has_qty or (float(rq.get(m, 0) or 0) > 0)
                    except Exception:
                        pass
                qty_mask[m] = has_qty
            print(
                "Máscara meses con cantidad>0:",
                {m: int(v) for m, v in qty_mask.items()},
            )

            # Construcción de filas Proyectado
            base = dict(zip(base_keys, keys))
            fila_cant = {"TipoForecast": "Proyectado", "Métrica": "Cantidad", **base}
            fila_prec = {"TipoForecast": "Proyectado", "Métrica": "Precio", **base}

            # Valores por mes
            if precio_firme.empty:
                # sin precio base → todo 0
                for m in MESES:
                    fila_cant[m] = 0.0
                    fila_prec[m] = 0.0
            else:
                rprice = precio_firme.iloc[0]
                for m in MESES:
                    fila_cant[m] = 0.0
                    # Copia precio solo si hay cantidad en ese mes (en Firme o Proyectado)
                    fila_prec[m] = float(rprice[m]) if qty_mask[m] else 0.0

            dfs.append(pd.DataFrame([fila_cant, fila_prec]))
            faltantes_total += 2

    if faltantes_total:
        print(f"Filas Proyectado generadas: {faltantes_total}")
    else:
        print("No fue necesario generar filas Proyectado adicionales.")

    df_metrico = pd.concat(dfs, ignore_index=True)

    orden = [
        "ItemCode",
        "ItemName",
        "TipoForecast",
        "Métrica",
        "OcrCode3",
        "DocCur",
        *cols_meses,
    ]
    df_metrico = (
        df_metrico[orden]
        .sort_values(["ItemCode", "TipoForecast", "Métrica"])
        .reset_index(drop=True)
    )

    print("\n=== Proceso completado exitosamente ===")
    return df_metrico


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
