# core/inicio_ventas.py
# -*- coding: utf-8 -*-
"""
Dashboard de Ventas (Inicio) – UX revisada
- DAL + UI en este archivo (sin sqlite3 directo; consultas vía utils.db.run_query).
- Filtro por período (YYYY-MM) con default = mes actual.
- ClientSet = clientes del Forecast_Detalle del período y SlpCode.
- Distribución:
  • Columna izquierda: G1 (totales & curva vendedor vs compañía)
  • Columna derecha:  G2 (detalle fijo por línea con participación)
  • Debajo:           G3 (cumplimiento con KPIs, barras y tablas fijas)
"""

from __future__ import annotations

from typing import Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import streamlit as st

from utils.db import run_query, DB_PATH


# =============================================================================
# Utilidades
# =============================================================================


def _fmt_clp(n: float | int | None) -> str:
    if n is None:
        n = 0
    try:
        return f"$ {int(round(float(n))):,}".replace(",", ".")
    except Exception:
        return "$ 0"


def _periodo_actual() -> str:
    from datetime import date

    d = date.today()
    return f"{d.year:04d}-{d.month:02d}"


def _mk_in_clause(ids: Sequence[str]) -> Tuple[str, Tuple]:
    safe_ids = tuple(str(x) for x in ids if x is not None and str(x) != "")
    if not safe_ids:
        return "('__VOID__')", tuple()
    placeholders = ",".join(["?"] * len(safe_ids))
    return f"({placeholders})", safe_ids


def _db_select_df(sql: str, params: tuple | list | dict | None = None) -> pd.DataFrame:
    params = params or ()
    try:
        df = run_query(sql, params=params)  # firma moderna
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    except TypeError:
        df = run_query(sql, DB_PATH, params=params)  # compat
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)


# =============================================================================
# DAL (todas las funciones de datos)
# =============================================================================


@st.cache_data(show_spinner=False)
def listar_periodos_disponibles(slpcode: int) -> list[str]:
    q1 = """
        SELECT DISTINCT STRFTIME('%Y-%m', fd.FechEntr) AS Periodo
        FROM Forecast_Detalle fd
        WHERE fd.SlpCode = ?
          AND fd.FechEntr IS NOT NULL
    """
    # OINV global (no filtra por SlpCode; robusto para vendedores nuevos)
    q2 = """
        SELECT DISTINCT STRFTIME('%Y-%m', v.DocDate) AS Periodo
        FROM OINV v
        WHERE v.DocDate IS NOT NULL
    """
    p1 = _db_select_df(q1, (slpcode,))
    p2 = _db_select_df(q2)  # sin params
    per = pd.concat([p1, p2], ignore_index=True)
    if "Periodo" not in per.columns or per.empty:
        return []
    return sorted(per["Periodo"].dropna().astype(str).unique(), reverse=True)


@st.cache_data(show_spinner=False)
def get_client_set_forecast(slpcode: int, periodo: str) -> list[str]:
    sql = """
        SELECT DISTINCT fd.CardCode
        FROM Forecast_Detalle fd
        WHERE fd.SlpCode = ?
          AND STRFTIME('%Y-%m', fd.FechEntr) = ?
    """
    df = _db_select_df(sql, (slpcode, periodo))
    if df.empty:
        return []
    return df["CardCode"].dropna().astype(str).tolist()


# ---------- Facturación VENDEDOR (filtrada por cartera; SIN SlpCode) ----------


@st.cache_data(show_spinner="Cargando ventas acumuladas…")
def get_facturas_total_mes_vendor(
    slpcode: int, periodo: str, clients: Sequence[str]
) -> float:
    if not clients:
        return 0.0
    in_sql, in_params = _mk_in_clause(tuple(clients))
    sql = f"""
        SELECT SUM(i.LineTotal) AS TotalCLP
        FROM INV1 i
        JOIN OINV v ON v.DocEntry = i.DocEntry
        WHERE STRFTIME('%Y-%m', v.DocDate) = ?
          AND v.CardCode IN {in_sql}
    """
    row = _db_select_df(sql, (periodo, *in_params))
    val = (
        row.iloc[0]["TotalCLP"] if (not row.empty and "TotalCLP" in row.columns) else 0
    )
    try:
        return float(val or 0)
    except Exception:
        return 0.0


@st.cache_data(show_spinner=False)
def get_facturas_diario_mes_vendor(
    slpcode: int, periodo: str, clients: Sequence[str]
) -> pd.DataFrame:
    """
    Serie diaria del mes (CLP) solo para cartera del vendedor.
    Devuelve Monto (día) y Acum (acumulado) con etiqueta Serie='Vendedor'.
    """
    if not clients:
        return pd.DataFrame(columns=["Dia", "Monto", "Acum", "Serie"])
    in_sql, in_params = _mk_in_clause(tuple(clients))
    sql = f"""
        SELECT STRFTIME('%d', v.DocDate) AS Dia, SUM(i.LineTotal) AS Monto
        FROM INV1 i
        JOIN OINV v ON v.DocEntry = i.DocEntry
        WHERE STRFTIME('%Y-%m', v.DocDate) = ?
          AND v.CardCode IN {in_sql}
        GROUP BY Dia
        ORDER BY Dia
    """
    df = _db_select_df(sql, (periodo, *in_params))
    if df.empty:
        return pd.DataFrame(columns=["Dia", "Monto", "Acum", "Serie"])
    df["Dia"] = pd.to_numeric(df["Dia"], errors="coerce")
    df["Monto"] = pd.to_numeric(df["Monto"], errors="coerce").fillna(0.0)
    df = df.sort_values("Dia").reset_index(drop=True)
    df["Acum"] = df["Monto"].cumsum()
    df["Serie"] = "Vendedor"
    return df[["Dia", "Monto", "Acum", "Serie"]]


# ---------- Facturación COMPAÑÍA (global) ----------


@st.cache_data(show_spinner=False)
def get_facturas_total_mes_company(periodo: str) -> float:
    sql = """
        SELECT SUM(i.LineTotal) AS TotalCLP
        FROM INV1 i
        JOIN OINV v ON v.DocEntry = i.DocEntry
        WHERE STRFTIME('%Y-%m', v.DocDate) = ?
    """
    row = _db_select_df(sql, (periodo,))
    val = (
        row.iloc[0]["TotalCLP"] if (not row.empty and "TotalCLP" in row.columns) else 0
    )
    try:
        return float(val or 0)
    except Exception:
        return 0.0


@st.cache_data(show_spinner=False)
def get_facturas_diario_mes_company(periodo: str) -> pd.DataFrame:
    """
    Serie diaria del mes (CLP) para toda la compañía.
    Devuelve Monto (día) y Acum (acumulado) con etiqueta Serie='Compañía'.
    """
    sql = """
        SELECT STRFTIME('%d', v.DocDate) AS Dia, SUM(i.LineTotal) AS Monto
        FROM INV1 i
        JOIN OINV v ON v.DocEntry = i.DocEntry
        WHERE STRFTIME('%Y-%m', v.DocDate) = ?
        GROUP BY Dia
        ORDER BY Dia
    """
    df = _db_select_df(sql, (periodo,))
    if df.empty:
        return pd.DataFrame(columns=["Dia", "Monto", "Acum", "Serie"])
    df["Dia"] = pd.to_numeric(df["Dia"], errors="coerce")
    df["Monto"] = pd.to_numeric(df["Monto"], errors="coerce").fillna(0.0)
    df = df.sort_values("Dia").reset_index(drop=True)
    df["Acum"] = df["Monto"].cumsum()
    df["Serie"] = "Compañía"
    return df[["Dia", "Monto", "Acum", "Serie"]]


# ---------- G2: Resumen por cliente (toda la compañía) + marcas de cartera ----------


def _fmt_price_cell(x: float | int | None) -> str:
    """Price como string: sin decimales si es entero, si no, hasta 4 decimales (sin ceros sobrantes)."""
    if x is None:
        return ""
    try:
        xv = float(x)
    except Exception:
        return ""
    if not np.isfinite(xv):
        return ""
    if abs(xv - int(round(xv))) < 1e-9:
        return f"{int(round(xv))}"
    s = f"{xv:.4f}".rstrip("0").rstrip(".")
    return s


@st.cache_data(show_spinner="Cargando detalle de facturas…")
def get_facturas_resumen_clientes(
    periodo: str, clients_vendor: Sequence[str]
) -> pd.DataFrame:
    """
    Una fila por cliente con facturas en el mes (compañía completa).
    Total = SUM(LineTotal). Moneda = 'Mixto' si hay más de una.
    EsCartera = True si el cliente está en la cartera del vendedor.
    ParticipacionPct (0..100) = Total / total_cartera_vendedor * 100 (solo clientes de cartera; resto = 0)
    PctQMK (0..100) = Total / total_compañía * 100 (barra '% sobre QMK').
    """
    sql = """
        SELECT
            v.CardCode,
            COALESCE(c.CardName, v.CardCode) AS Cliente,
            CASE
              WHEN COUNT(DISTINCT i.Currency) = 1 THEN MAX(i.Currency)
              ELSE 'Mixto'
            END AS Moneda,
            SUM(i.LineTotal) AS Total
        FROM INV1 i
        JOIN OINV v ON v.DocEntry = i.DocEntry
        LEFT JOIN OCRD c ON c.CardCode = v.CardCode
        WHERE STRFTIME('%Y-%m', v.DocDate) = ?
        GROUP BY v.CardCode, c.CardName
    """
    df = _db_select_df(sql, (periodo,))
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Cliente",
                "Cartera",
                "Moneda",
                "Total",
                "ParticipacionPct",
                "PctQMK",
            ]
        )

    df["Total"] = pd.to_numeric(df["Total"], errors="coerce").fillna(0.0)

    # Marcar cartera del vendedor
    cartera = set(str(x) for x in (clients_vendor or []))
    df["EsCartera"] = df["CardCode"].astype(str).isin(cartera)
    df["Cartera"] = np.where(df["EsCartera"], "Sí", "No")

    # Totales para %s
    total_company = float(df["Total"].sum()) if not df.empty else 0.0
    total_cartera = (
        float(df.loc[df["EsCartera"], "Total"].sum()) if not df.empty else 0.0
    )

    df["ParticipacionPct"] = np.where(
        df["EsCartera"] & (total_cartera > 0),
        (df["Total"] / total_cartera) * 100.0,
        0.0,
    )
    df["PctQMK"] = (df["Total"] / total_company) * 100.0 if total_company > 0 else 0.0

    # Redondeo a 1 decimal
    df["ParticipacionPct"] = np.round(df["ParticipacionPct"], 1)
    df["PctQMK"] = np.round(df["PctQMK"], 1)

    # Orden: cartera primero, luego mayor venta, luego nombre
    df = df.sort_values(
        ["EsCartera", "Total", "Cliente"], ascending=[False, False, True]
    ).reset_index(drop=True)

    # Selección/orden de columnas
    df = df[["Cliente", "Cartera", "Moneda", "Total", "ParticipacionPct", "PctQMK"]]
    return df


# ---------- Forecast / OV / Factura por cliente (OV/Fac SIN SlpCode; por cartera) ----------


@st.cache_data(show_spinner=False)
def get_fp_por_cliente(slpcode: int, periodo: str) -> pd.DataFrame:
    sql = """
        SELECT fd.CardCode,
               COALESCE(c.CardName, fd.CardCode) AS Cliente,
               SUM(CASE WHEN fd.TipoForecast='Firme'      THEN fd.Cant ELSE 0 END) AS FirmeUnid,
               SUM(CASE WHEN fd.TipoForecast='Proyectado' THEN fd.Cant ELSE 0 END) AS ProyUnid
        FROM Forecast_Detalle fd
        LEFT JOIN OCRD c ON c.CardCode = fd.CardCode
        WHERE fd.SlpCode = ?
          AND STRFTIME('%Y-%m', fd.FechEntr) = ?
        GROUP BY fd.CardCode, c.CardName
    """
    df = _db_select_df(sql, (slpcode, periodo))
    for col in ("FirmeUnid", "ProyUnid"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


@st.cache_data(show_spinner=False)
def get_ov_por_cliente(
    slpcode: int, periodo: str, clients: Sequence[str]
) -> pd.DataFrame:
    if not clients:
        return pd.DataFrame(columns=["CardCode", "OV_Unid"])
    in_sql, in_params = _mk_in_clause(tuple(clients))
    sql = f"""
        SELECT o.CardCode, SUM(r.Quantity) AS OV_Unid
        FROM RDR1 r
        JOIN ORDR o ON o.DocEntry = r.DocEntry
        WHERE STRFTIME('%Y-%m', o.DocDueDate) = ?
          AND o.CardCode IN {in_sql}
        GROUP BY o.CardCode
    """
    df = _db_select_df(sql, (periodo, *in_params))
    if df.empty:
        return pd.DataFrame(columns=["CardCode", "OV_Unid"])
    df["OV_Unid"] = pd.to_numeric(df["OV_Unid"], errors="coerce").fillna(0.0)
    return df


@st.cache_data(show_spinner=False)
def get_fac_por_cliente(
    slpcode: int, periodo: str, clients: Sequence[str]
) -> pd.DataFrame:
    if not clients:
        return pd.DataFrame(columns=["CardCode", "Fac_Unid"])
    in_sql, in_params = _mk_in_clause(tuple(clients))
    sql = f"""
        SELECT v.CardCode, SUM(i.Quantity) AS Fac_Unid
        FROM INV1 i
        JOIN OINV v ON v.DocEntry = i.DocEntry
        WHERE STRFTIME('%Y-%m', v.DocDate) = ?
          AND v.CardCode IN {in_sql}
        GROUP BY v.CardCode
    """
    df = _db_select_df(sql, (periodo, *in_params))
    if df.empty:
        return pd.DataFrame(columns=["CardCode", "Fac_Unid"])
    df["Fac_Unid"] = pd.to_numeric(df["Fac_Unid"], errors="coerce").fillna(0.0)
    return df


@st.cache_data(show_spinner="Cotejando OV vs Factura…")
def get_estado_ov_cliente_item(
    slpcode: int, periodo: str, clients: Sequence[str]
) -> pd.DataFrame:
    """
    Comparativo OV ↔ Factura a nivel cliente–item (CardCode, ItemCode) en el período.
    Retorna: CardCode, ItemCode, QtyOV, QtyFAC, Moneda (de facturas), Precio (prom. ponderado), Estado
    """
    if not clients:
        return pd.DataFrame(
            columns=[
                "CardCode",
                "ItemCode",
                "QtyOV",
                "QtyFAC",
                "Moneda",
                "Precio",
                "Estado",
            ]
        )

    in_sql, in_params = _mk_in_clause(tuple(clients))

    sql = f"""
        WITH
        OV AS (
            SELECT o.CardCode, r.ItemCode, SUM(r.Quantity) AS QtyOV
            FROM RDR1 r
            JOIN ORDR o ON o.DocEntry = r.DocEntry
            WHERE STRFTIME('%Y-%m', o.DocDueDate) = ?
              AND o.CardCode IN {in_sql}
            GROUP BY o.CardCode, r.ItemCode
        ),
        FAC AS (
            SELECT
                v.CardCode,
                i.ItemCode,
                SUM(i.Quantity) AS QtyFAC,
                CASE
                  WHEN SUM(COALESCE(i.Quantity,0)) > 0
                  THEN SUM(COALESCE(i.Price,0) * COALESCE(i.Quantity,0)) / SUM(COALESCE(i.Quantity,0))
                  ELSE NULL
                END AS Precio,
                CASE
                  WHEN COUNT(DISTINCT i.Currency) = 1 THEN MAX(i.Currency)
                  ELSE 'Mixto'
                END AS Moneda
            FROM INV1 i
            JOIN OINV v ON v.DocEntry = i.DocEntry
            WHERE STRFTIME('%Y-%m', v.DocDate) = ?
              AND v.CardCode IN {in_sql}
            GROUP BY v.CardCode, i.ItemCode
        ),
        L AS (
            SELECT ov.CardCode, ov.ItemCode, ov.QtyOV,
                   COALESCE(f.QtyFAC,0) AS QtyFAC,
                   f.Moneda,
                   f.Precio
            FROM OV ov
            LEFT JOIN FAC f
              ON f.CardCode = ov.CardCode AND f.ItemCode = ov.ItemCode
            UNION ALL
            SELECT f.CardCode, f.ItemCode, 0 AS QtyOV,
                   f.QtyFAC, f.Moneda, f.Precio
            FROM FAC f
            LEFT JOIN OV ov
              ON ov.CardCode = f.CardCode AND ov.ItemCode = f.ItemCode
            WHERE ov.CardCode IS NULL
        )
        SELECT * FROM L
    """
    params = (periodo, *in_params, periodo, *in_params)
    df = _db_select_df(sql, params)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "CardCode",
                "ItemCode",
                "QtyOV",
                "QtyFAC",
                "Moneda",
                "Precio",
                "Estado",
            ]
        )

    df["QtyOV"] = pd.to_numeric(df["QtyOV"], errors="coerce").fillna(0.0)
    df["QtyFAC"] = pd.to_numeric(df["QtyFAC"], errors="coerce").fillna(0.0)
    df["Precio"] = pd.to_numeric(df["Precio"], errors="coerce")
    df["Estado"] = np.select(
        [
            df["QtyFAC"] >= df["QtyOV"],
            (df["QtyFAC"] > 0) & (df["QtyFAC"] < df["QtyOV"]),
            df["QtyFAC"] <= 0,
        ],
        ["Cumplida", "Parcial", "No facturada"],
        default="No facturada",
    )
    return df


@st.cache_data(show_spinner=False)
def get_alertas_precioun_firme(slpcode: int, periodo: str) -> pd.DataFrame:
    sql = """
        SELECT COALESCE(c.CardName, fd.CardCode) AS Cliente,
               STRFTIME('%Y-%m', fd.FechEntr)    AS Mes,
               fd.ItemCode,
               fd.Cant,
               fd.PrecioUN
        FROM Forecast_Detalle fd
        LEFT JOIN OCRD c ON c.CardCode = fd.CardCode
        WHERE fd.SlpCode = ?
          AND STRFTIME('%Y-%m', fd.FechEntr) = ?
          AND fd.TipoForecast = 'Firme'
          AND (fd.PrecioUN IS NULL OR fd.PrecioUN <= 0)
          AND COALESCE(fd.Cant,0) > 0
    """
    df = _db_select_df(sql, (slpcode, periodo))
    if df.empty:
        return df
    df["Cant"] = pd.to_numeric(df["Cant"], errors="coerce").fillna(0.0)
    return df


# =============================================================================
# UI
# =============================================================================


def _selector_periodo_ui(slpcode: int) -> str:
    per_list = listar_periodos_disponibles(slpcode)
    default = _periodo_actual()
    idx = per_list.index(default) if default in per_list else 0
    return st.selectbox(
        "Periodo (YYYY-MM)", options=per_list or [default], index=idx if per_list else 0
    )


def _render_g1_g2_row(slpcode: int, periodo: str, clients: Sequence[str]):
    col1, col2 = st.columns([1, 1.2])

    # ---------------- G1: Totales + curva vendedor vs compañía (acumulada) ----------------
    with col1:
        st.subheader("G1 · Ventas acumuladas (Facturas)")

        total_vendor = get_facturas_total_mes_vendor(slpcode, periodo, clients)
        total_company = get_facturas_total_mes_company(periodo)

        c1, c2 = st.columns(2)
        c1.metric("Vendedor (clientes Forecast)", _fmt_clp(total_vendor))
        c2.metric("Compañía (global)", _fmt_clp(total_company))

        serie_v = get_facturas_diario_mes_vendor(slpcode, periodo, clients)
        serie_c = get_facturas_diario_mes_company(periodo)
        serie = pd.concat([serie_v, serie_c], ignore_index=True)

        if not serie.empty:
            try:
                import altair as alt

                chart = (
                    alt.Chart(serie)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("Dia:O", title="Día"),
                        y=alt.Y("Acum:Q", title="CLP acumulado"),
                        color=alt.Color("Serie:N", legend=alt.Legend(title="Serie")),
                        tooltip=[
                            alt.Tooltip("Dia:O", title="Día"),
                            alt.Tooltip("Serie:N"),
                            alt.Tooltip("Acum:Q", title="CLP acumulado", format=",.0f"),
                        ],
                    )
                )
                st.altair_chart(chart, use_container_width=True)
            except Exception:
                # Fallback simple por serie
                for s in ("Vendedor", "Compañía"):
                    sub = serie[serie["Serie"] == s].set_index("Dia")["Acum"]
                    st.line_chart(sub, height=160)

        st.caption(f"Clientes en Forecast del período: {len(clients)}")
        st.caption(f"Facturación compañía en período: {_fmt_clp(total_company)}")

    # ---------------- G2: Detalle fijo (tabla) ----------------
    with col2:
        st.subheader("G2 · Detalle facturado clientes")

        df = get_facturas_resumen_clientes(periodo, clients)
        if df.empty:
            st.info("No hay clientes con facturación en el período.")
        else:
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Cliente": st.column_config.TextColumn("Cliente", width="large"),
                    "Cartera": st.column_config.TextColumn(
                        "Cartera", help="Pertenece a la cartera del vendedor"
                    ),
                    "Moneda": st.column_config.TextColumn("Moneda", width="small"),
                    "Total": st.column_config.NumberColumn(
                        "Total Venta", format="$ %d"
                    ),
                    "ParticipacionPct": st.column_config.ProgressColumn(
                        "Participación (%)",
                        format="%.1f%%",
                        min_value=0.0,
                        max_value=100.0,
                    ),
                    "PctQMK": st.column_config.ProgressColumn(
                        "% sobre QMK", format="%.1f%%", min_value=0.0, max_value=100.0
                    ),
                },
            )


def _render_g3(slpcode: int, periodo: str, clients: Sequence[str]):
    st.subheader("G3 · Cumplimiento mensual (Firme/Proyectado ↔ OV ↔ Factura)")

    # Datos base
    df_fp = get_fp_por_cliente(slpcode, periodo)
    df_ov = get_ov_por_cliente(slpcode, periodo, clients)
    df_fac = get_fac_por_cliente(slpcode, periodo, clients)

    df = df_fp.merge(df_ov, on="CardCode", how="outer").merge(
        df_fac, on="CardCode", how="outer"
    )
    if "Cliente" not in df.columns:
        df["Cliente"] = df["CardCode"]
    for col in ("FirmeUnid", "ProyUnid", "OV_Unid", "Fac_Unid"):
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Totales
    total_firme = float(df["FirmeUnid"].sum()) if not df.empty else 0.0
    total_proy = float(df["ProyUnid"].sum()) if not df.empty else 0.0
    total_ov = float(df["OV_Unid"].sum()) if not df.empty else 0.0
    total_fac = float(df["Fac_Unid"].sum()) if not df.empty else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Firme (unid)", f"{int(round(total_firme)):,}".replace(",", "."))
    c2.metric("Proyectado (unid)", f"{int(round(total_proy)):,}".replace(",", "."))
    c3.metric("Compromiso OV (unid)", f"{int(round(total_ov)):,}".replace(",", "."))
    c4.metric("Facturado (unid)", f"{int(round(total_fac)):,}".replace(",", "."))

    # Barras globales
    st.markdown("---")
    colA, colB = st.columns(2)
    ratio_fac_vs_firme = (total_fac / total_firme) if total_firme > 0 else 0.0
    ratio_fac_vs_ov = (total_fac / total_ov) if total_ov > 0 else 0.0
    colA.progress(
        min(1.0, ratio_fac_vs_firme),
        text=f"Facturado / Firme: {ratio_fac_vs_firme*100:.1f}%",
    )
    colB.progress(
        min(1.0, ratio_fac_vs_ov), text=f"Facturado / OV: {ratio_fac_vs_ov*100:.1f}%"
    )
    st.caption(
        "Facturado / Firme = (unidades facturadas del mes) ÷ (unidades comprometidas como *Firme* en Forecast para el mes). "
        "Puede superar 100% si se vendió por sobre lo comprometido."
    )
    st.caption(
        "Facturado / OV = (unidades facturadas del mes) ÷ (unidades comprometidas en **órdenes de venta** con DocDueDate del mes). "
        "Por contrato puede exceder 100%."
    )

    # Resumen por cliente
    st.markdown("#### Resumen por cliente")
    if df.empty:
        st.info("Sin datos para el período.")
    else:

        def safe_div(a, b):
            a = pd.to_numeric(a, errors="coerce").fillna(0.0)
            b = pd.to_numeric(b, errors="coerce").fillna(0.0)
            with np.errstate(divide="ignore", invalid="ignore"):
                r = np.where(b > 0, a / b, 0.0)
            return pd.Series(r, index=a.index)

        df_client = df.copy()
        df_client["OV_vs_Firme"] = np.round(
            safe_div(df_client["OV_Unid"], df_client["FirmeUnid"]) * 100.0, 1
        )
        df_client["OV_vs_Proy"] = np.round(
            safe_div(df_client["OV_Unid"], df_client["ProyUnid"]) * 100.0, 1
        )
        df_client["Fac_vs_Firme"] = np.round(
            safe_div(df_client["Fac_Unid"], df_client["FirmeUnid"]) * 100.0, 1
        )
        df_client["Fac_vs_Proy"] = np.round(
            safe_div(df_client["Fac_Unid"], df_client["ProyUnid"]) * 100.0, 1
        )
        df_client["Fac_vs_OV"] = np.round(
            safe_div(df_client["Fac_Unid"], df_client["OV_Unid"]) * 100.0, 1
        )

        df_client["SinCompromiso"] = (df_client["FirmeUnid"] <= 0) & (
            df_client["ProyUnid"] <= 0
        )
        df_client = df_client.sort_values(
            ["SinCompromiso", "Cliente"], ascending=[True, True]
        ).reset_index(drop=True)

        st.dataframe(
            df_client[
                [
                    "Cliente",
                    "FirmeUnid",
                    "ProyUnid",
                    "OV_Unid",
                    "Fac_Unid",
                    "OV_vs_Firme",
                    "OV_vs_Proy",
                    "Fac_vs_Firme",
                    "Fac_vs_Proy",
                    "Fac_vs_OV",
                ]
            ],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Cliente": st.column_config.TextColumn("Cliente", width="large"),
                "FirmeUnid": st.column_config.NumberColumn("Firme (unid)", format="%d"),
                "ProyUnid": st.column_config.NumberColumn(
                    "Proyectado (unid)", format="%d"
                ),
                "OV_Unid": st.column_config.NumberColumn("OV (unid)", format="%d"),
                "Fac_Unid": st.column_config.NumberColumn(
                    "Facturado (unid)", format="%d"
                ),
                "OV_vs_Firme": st.column_config.ProgressColumn(
                    "OV / Firme", format="%.1f%%", min_value=0.0, max_value=100.0
                ),
                "OV_vs_Proy": st.column_config.ProgressColumn(
                    "OV / Proyectado", format="%.1f%%", min_value=0.0, max_value=100.0
                ),
                "Fac_vs_Firme": st.column_config.ProgressColumn(
                    "Facturado / Firme", format="%.1f%%", min_value=0.0, max_value=100.0
                ),
                "Fac_vs_Proy": st.column_config.ProgressColumn(
                    "Facturado / Proy", format="%.1f%%", min_value=0.0, max_value=100.0
                ),
                "Fac_vs_OV": st.column_config.ProgressColumn(
                    "Facturado / OV", format="%.1f%%", min_value=0.0, max_value=100.0
                ),
            },
        )

    # Detalle cliente–item
    st.markdown("#### Detalle cliente–item (OV vs Factura y Estado)")
    ov_items = get_estado_ov_cliente_item(slpcode, periodo, clients)
    if ov_items.empty:
        st.info("No hay compromisos para el período.")
    else:
        try:
            names_c = _db_select_df("SELECT CardCode, CardName FROM OCRD")
        except Exception:
            names_c = pd.DataFrame(columns=["CardCode", "CardName"])
        try:
            names_i = _db_select_df("SELECT ItemCode, ItemName FROM OITM")
        except Exception:
            names_i = pd.DataFrame(columns=["ItemCode", "ItemName"])

        det = ov_items.copy()
        if not names_c.empty:
            det = det.merge(names_c, on="CardCode", how="left")
        if not names_i.empty:
            det = det.merge(names_i, on="ItemCode", how="left")

        det["Cliente"] = (
            det["CardName"].fillna(det["CardCode"])
            if "CardName" in det.columns
            else det["CardCode"]
        )
        det["Producto"] = (
            det["ItemName"].fillna(det["ItemCode"])
            if "ItemName" in det.columns
            else det["ItemCode"]
        )

        # Precio formateado
        det["PrecioDisp"] = det["Precio"].apply(_fmt_price_cell)
        det["PctCumpl"] = np.where(
            det["QtyOV"] > 0, (det["QtyFAC"] / det["QtyOV"]) * 100.0, 0.0
        )
        det["PctCumpl"] = np.round(det["PctCumpl"], 1)

        estado_order = pd.Categorical(
            det["Estado"],
            categories=["No facturada", "Parcial", "Cumplida"],
            ordered=True,
        )
        det = det.assign(_estado_ord=estado_order).sort_values(
            ["_estado_ord", "PctCumpl"], ascending=[True, False]
        )

        st.dataframe(
            det[
                [
                    "Cliente",
                    "ItemCode",
                    "Producto",
                    "Moneda",
                    "PrecioDisp",
                    "QtyOV",
                    "QtyFAC",
                    "PctCumpl",
                    "Estado",
                ]
            ]
            .rename(columns={"ItemCode": "Item", "PrecioDisp": "Precio"})
            .reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Cliente": st.column_config.TextColumn("Cliente", width="large"),
                "Item": st.column_config.TextColumn("Item", width="small"),
                "Producto": st.column_config.TextColumn("Producto", width="large"),
                "Moneda": st.column_config.TextColumn("Moneda", width="small"),
                "Precio": st.column_config.TextColumn("Precio", width="small"),
                "QtyOV": st.column_config.NumberColumn("OV (unid)", format="%d"),
                "QtyFAC": st.column_config.NumberColumn(
                    "Facturado (unid)", format="%d"
                ),
                "PctCumpl": st.column_config.ProgressColumn(
                    "Cumplimiento (Fact/OV)",
                    format="%.1f%%",
                    min_value=0.0,
                    max_value=100.0,
                ),
                "Estado": st.column_config.TextColumn("Estado"),
            },
        )


def _render_gn_alertas(slpcode: int, periodo: str):
    st.subheader("G-N · Alertas de Precio (Firme sin PrecioUN)")
    df = get_alertas_precioun_firme(slpcode, periodo)
    if df is None or df.empty:
        st.caption("Sin alertas de precio para el período.")
        return
    st.warning(
        f"Se detectaron {len(df)} fila(s) Firme con Cant>0 y PrecioUN inválido en el período."
    )
    for r in df.itertuples(index=False):
        st.write(
            f"Cliente **{getattr(r, 'Cliente', '')}** — **{getattr(r, 'Mes', '')}**: "
            f"falta PrecioUN para **{int(round(getattr(r, 'Cant', 0) or 0))}** unid "
            f"(Item {getattr(r, 'ItemCode', '')})."
        )
    with st.expander("Ver detalle de alertas", expanded=False):
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Cliente": st.column_config.TextColumn("Cliente", width="large"),
                "Mes": st.column_config.TextColumn("Mes"),
                "ItemCode": st.column_config.TextColumn("Item"),
                "Cant": st.column_config.NumberColumn("Cantidad", format="%d"),
                "PrecioUN": st.column_config.NumberColumn("PrecioUN", format="%.4f"),
            },
        )


# =============================================================================
# API pública
# =============================================================================


def render_dashboard(
    slpcode: Optional[int] = None,
    anio: Optional[int] = None,  # ignorado (período es YYYY-MM)
    mes: Optional[int] = None,  # ignorado
) -> None:
    # 1) SlpCode desde sesión si no viene
    if slpcode is None:
        slpcode = st.session_state.get("SlpCode")
    if not isinstance(slpcode, int) or slpcode <= 0:
        st.error("No hay vendedor activo en la sesión.")
        return

    # 2) Selector de período (sin títulos/captions del header)
    try:
        periodo = _selector_periodo_ui(slpcode)
    except Exception as e:
        st.error(f"No fue posible listar períodos: {e}")
        return

    # 3) ClientSet (obligatorio)
    try:
        clients = get_client_set_forecast(slpcode, periodo)
    except Exception as e:
        st.error(f"No fue posible obtener clientes del Forecast: {e}")
        return

    if not clients:
        st.info("No hay clientes en el Forecast para el período seleccionado.")
        return

    # 4) Fila G1 + G2
    try:
        _render_g1_g2_row(slpcode, periodo, clients)
    except Exception as e:
        st.error(f"G1/G2 falló: {e}")

    # 5) G3 (cumplimiento + tablas fijas)
    try:
        _render_g3(slpcode, periodo, clients)
    except Exception as e:
        st.error(f"G3 falló: {e}")

    # 6) G-N (alertas complementarias)
    try:
        _render_gn_alertas(slpcode, periodo)
    except Exception as e:
        st.error(f"Alertas falló: {e}")
