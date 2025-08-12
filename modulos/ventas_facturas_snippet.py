# modulos/ventas_facturas_snippet.py

from __future__ import annotations
from typing import Any, List, Tuple
from datetime import datetime
import pandas as pd
import streamlit as st

from utils.db import run_query, DB_PATH


# ───────────────────────────────────────────────────────────────────────────
# 👓  Punto de entrada UI
# ───────────────────────────────────────────────────────────────────────────


def mostrar_facturas() -> None:
    """Render principal de la pestaña *Facturas* (OINV × INV1)."""

    df_base = obtener_facturas_base()

    if df_base.empty:
        st.warning(
            "La consulta no devolvió facturas disponibles (verifique Forecast_Detalle y fechas)."
        )
        return

    df_filtrado = aplicar_filtros(df_base)

    if df_filtrado.empty:
        st.info("No se encontraron facturas para los filtros activos.")
        return

    calcular_kpis(df_filtrado)
    # diagnostico_semantico(df_filtrado)
    renderizar_vista(df_filtrado)


# ───────────────────────────────────────────────────────────────────────────
# 📑  Vista SQL Base – OINV × INV1 (+OCRD)
# ───────────────────────────────────────────────────────────────────────────


# B_VF001
def _parse_docdate(col: pd.Series) -> pd.Series:
    """Normaliza DocDate priorizando 'YYYY-MM-DD' y con fallback day-first."""
    s = pd.to_datetime(col, format="%Y-%m-%d", errors="coerce")
    mask = s.isna()
    if mask.any():
        # Intenta interpretar entradas atípicas (p.ej., 'YYYY-DD-MM' o variantes)
        s.loc[mask] = pd.to_datetime(col[mask], dayfirst=True, errors="coerce")
    return s.dt.date  # devuelve date (sin hora)


def obtener_facturas_base(
    *,
    db_path: str = DB_PATH,
    filtros_sql: List[str] | None = None,
    params: Tuple[Any, ...] | None = None,
) -> pd.DataFrame:
    """Carga facturas de clientes con *Forecast* y genera `MesFactura`."""

    filtros_sql = filtros_sql or []
    params = params or []

    filtros_sql.insert(
        0, "o.CardCode IN (SELECT DISTINCT CardCode FROM Forecast_Detalle)"
    )
    where_clause = "WHERE " + " AND ".join(filtros_sql)

    query = f"""
        SELECT
            o.DocEntry  AS DocEntry,
            o.DocNum    AS DocNum,
            o.DocDate   AS DocDate,
            o.CardCode  AS CardCode,
            c.CardName  AS CardName,
            o.SlpCode   AS SlpCode,
            o.DocCur    AS DocCur,
            o.DocTotal  AS DocTotal,
            i.LineTotal AS LineTotal,
            i.ItemCode  AS ItemCode,
            i.Dscription AS Dscription,
            i.Quantity  AS Quantity,
            i.Price     AS Price,
            i.DiscPrcnt AS DiscPrcnt,
            'Factura'   AS TipoDoc
        FROM   OINV  o
        JOIN   INV1  i ON i.DocEntry = o.DocEntry
        LEFT  JOIN OCRD c ON c.CardCode = o.CardCode
        {where_clause};
    """

    df = run_query(query, params=params, db_path=db_path)

    if df.empty:
        return df

    df["DocEntry"] = df["DocEntry"].astype(str)
    df["DocNum"] = df["DocNum"].astype(str)
    df["DocDate"] = _parse_docdate(df["DocDate"])
    df["MesFactura"] = pd.to_datetime(
        df["DocDate"], format="%Y-%m-%d", errors="coerce"
    ).dt.strftime("%Y-%m")

    return df


# ───────────────────────────────────────────────────────────────────────────
# 🎛️  Filtros UI (Mes, Cliente)
# ───────────────────────────────────────────────────────────────────────────

# B_VF002


def aplicar_filtros(df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("Filtros")

    meses_unicos = sorted(df["MesFactura"].dropna().unique())
    if not meses_unicos:
        st.error("No existen fechas válidas para mostrar.")
        return pd.DataFrame()

    mes_actual = datetime.now().strftime("%Y-%m")
    default_meses = [mes_actual] if mes_actual in meses_unicos else [meses_unicos[-1]]

    meses_sel = st.multiselect(
        "Mes factura (YYYY-MM)",
        options=meses_unicos,
        default=default_meses,
    )

    clientes_unicos = sorted(df["CardName"].dropna().unique())
    cliente_sel = st.multiselect("Cliente", options=clientes_unicos, default=[])

    if meses_sel:
        df = df[df["MesFactura"].isin(meses_sel)]

    if cliente_sel:
        df_cli = df[df["CardName"].isin(cliente_sel)]
        if df_cli.empty:
            st.warning(
                "Para el/los cliente(s) seleccionado(s) no existen ventas en los meses filtrados."
            )
        df = df_cli

    return df


# ───────────────────────────────────────────────────────────────────────────
# 📊  KPIs, Diagnóstico y Vista final
# ───────────────────────────────────────────────────────────────────────────

# B_VF003


def calcular_kpis(df: pd.DataFrame) -> None:
    st.markdown("### KPIs")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Facturas", f"{df['DocEntry'].nunique()}")
    col2.metric("Monto Neto (LineTotal)", f"{df['LineTotal'].sum():,.2f}")
    col3.metric("Docs Únicos", f"{df['DocEntry'].nunique()}")


# B_VF004


def diagnostico_semantico(df: pd.DataFrame) -> None:
    pass  # Comentado temporalmente por decisión operativa


# B_VF005


def renderizar_vista(df: pd.DataFrame) -> None:
    st.subheader("Vista Detallada de Facturas")
    st.dataframe(df, use_container_width=True)
