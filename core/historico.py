# core/historico.py
from __future__ import annotations
from typing import Optional, Tuple, List
import pandas as pd
import numpy as np
import streamlit as st
import altair as alt

from utils.db import run_query, DB_PATH


# ──────────────────────────────────────────────────────────────────────────────
# Catálogos base
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _obtener_clientes() -> pd.DataFrame:
    sql = """
        SELECT CardCode, CardName
        FROM OCRD
        WHERE (CardType='C' OR CardType IS NULL)
          AND (validFor='Y' OR validFor IS NULL)
        ORDER BY CardCode
    """
    df = run_query(sql, DB_PATH)
    return df if not df.empty else pd.DataFrame(columns=["CardCode", "CardName"])


@st.cache_data(show_spinner=False)
def _obtener_items() -> pd.DataFrame:
    sql = """
        SELECT ItemCode, ItemName
        FROM OITM
        WHERE (validFor='Y' OR validFor IS NULL)
        ORDER BY ItemCode
    """
    df = run_query(sql, DB_PATH)
    return df if not df.empty else pd.DataFrame(columns=["ItemCode", "ItemName"])


# Dependencias: opciones condicionadas por la selección del otro filtro
@st.cache_data(show_spinner=False)
def _items_para_cliente(cardcode: str) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT i.ItemCode, it.ItemName
        FROM INV1 i
        JOIN OINV o ON o.DocEntry = i.DocEntry
        LEFT JOIN OITM it ON it.ItemCode = i.ItemCode
        WHERE o.CardCode = ?
        ORDER BY i.ItemCode
    """
    df = run_query(sql, DB_PATH, (cardcode,))
    return df if not df.empty else pd.DataFrame(columns=["ItemCode", "ItemName"])


@st.cache_data(show_spinner=False)
def _clientes_para_item(itemcode: str) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT o.CardCode, o.CardName
        FROM INV1 i
        JOIN OINV o ON o.DocEntry = i.DocEntry
        WHERE i.ItemCode = ?
        ORDER BY o.CardCode
    """
    df = run_query(sql, DB_PATH, (itemcode,))
    return df if not df.empty else pd.DataFrame(columns=["CardCode", "CardName"])


# ──────────────────────────────────────────────────────────────────────────────
# Consulta base OINV × INV1 (única fecha: DocDueDate)
# ──────────────────────────────────────────────────────────────────────────────
def _build_sql(cardcode: Optional[str], itemcode: Optional[str]) -> Tuple[str, Tuple]:
    sql = """
    SELECT
        o.DocNum,
        o.DocDueDate,
        o.CardName,
        i.ItemCode,
        i.Quantity,
        i.Price,
        i.LineTotal,
        i.OcrCode3
    FROM INV1 AS i
    JOIN OINV AS o ON o.DocEntry = i.DocEntry
    WHERE 1=1
    """
    params: List = []
    if cardcode:
        sql += " AND o.CardCode = ?"
        params.append(cardcode)
    if itemcode:
        sql += " AND i.ItemCode = ?"
        params.append(itemcode)
    sql += " ORDER BY o.DocDueDate DESC, o.DocNum"
    return sql, tuple(params)


def _obtener_base(cardcode: Optional[str], itemcode: Optional[str]) -> pd.DataFrame:
    sql, params = _build_sql(cardcode, itemcode)
    df = run_query(sql, DB_PATH, params)
    if df.empty:
        return df

    # Enriquecer ItemName
    items = _obtener_items()
    if not items.empty:
        df = df.merge(items, on="ItemCode", how="left")

    # Moneda fija CLP (LineTotal ya viene en CLP)
    df["Currency"] = "CLP"

    # Columnas finales
    cols = [
        "DocDueDate",
        "DocNum",
        "CardName",
        "ItemCode",
        "ItemName",
        "OcrCode3",
        "Quantity",
        "Price",
        "Currency",
        "LineTotal",
    ]
    df = df[cols]
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Utilidades de tiempo: completar meses y preparar etiquetas
# ──────────────────────────────────────────────────────────────────────────────
MESES_ES = {
    1: "Ene",
    2: "Feb",
    3: "Mar",
    4: "Abr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Ago",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dic",
}


def _rango_mensual(df_dates: pd.Series) -> pd.DatetimeIndex:
    fechas = pd.to_datetime(df_dates, errors="coerce").dropna()
    if fechas.empty:
        hoy = pd.Timestamp.today().normalize()
        return pd.date_range(
            hoy.replace(month=1, day=1), hoy.replace(month=12, day=1), freq="MS"
        )

    anios = sorted(fechas.dt.year.unique())
    if len(anios) == 1:
        y = anios[0]
        start = pd.Timestamp(year=y, month=1, day=1)
        end = pd.Timestamp(year=y, month=12, day=1)
    else:
        y0, y1 = anios[-2], anios[-1]  # últimos 2 años con ventas
        start = pd.Timestamp(year=y0, month=1, day=1)
        end = pd.Timestamp(year=y1, month=12, day=1)
    return pd.date_range(start, end, freq="MS")


def _serie_mensual(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """
    Retorna: Periodo (datetime64, inicio de mes), value_col, MesLbl (str para eje nominal), Ord (int)
    """
    df = df.copy()
    df["Periodo"] = pd.to_datetime(df["DocDueDate"]).dt.to_period("M").dt.to_timestamp()
    idx = _rango_mensual(df["DocDueDate"])
    ser = (
        df.groupby("Periodo", as_index=True)[value_col].sum().reindex(idx, fill_value=0)
    )
    out = ser.reset_index().rename(columns={"index": "Periodo", value_col: value_col})

    # Etiqueta visible en eje: Ene, Feb, ..., y en enero añadimos salto de línea con el año
    meses = out["Periodo"].dt.month
    anios = out["Periodo"].dt.year.astype(str)
    out["MesLbl"] = [
        f"{MESES_ES[m]}" + (f"\n{y}" if m == 1 else "") for m, y in zip(meses, anios)
    ]

    # Orden nominal para conservar la secuencia temporal
    out["Ord"] = np.arange(len(out))
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Vista UI – Filtros dependientes (en una fila) + tabla + gráficos
# ──────────────────────────────────────────────────────────────────────────────
def vista_historico(
    slpcode: int, _unused=None
) -> None:  # slpcode intencionalmente ignorado
    st.markdown("### 📜 Histórico de ventas — detalle (OINV × INV1)")

    # Filtros (horizontal) con dependencia mutua
    sel_item_prev = st.session_state.get("item_selectbox_historico")
    clientes_src = (
        _clientes_para_item(sel_item_prev) if sel_item_prev else _obtener_clientes()
    )

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        cliente_opts: List[Optional[str]] = [None] + clientes_src["CardCode"].tolist()
        cardcode = st.selectbox(
            "Cliente:",
            options=cliente_opts,
            index=0,
            format_func=lambda x: (
                "(Todos)"
                if x is None
                else f"{x} - {clientes_src.loc[clientes_src['CardCode'] == x, 'CardName'].values[0]}"
            ),
            key="cliente_selectbox_historico",
        )

    if cardcode:
        items_src = _items_para_cliente(cardcode)
    else:
        items_src = _obtener_items()

    with col_f2:
        item_opts: List[Optional[str]] = [None] + items_src["ItemCode"].tolist()
        itemcode = st.selectbox(
            "Ítem:",
            options=item_opts,
            index=0,
            format_func=lambda x: (
                "(Todos)"
                if x is None
                else f"{x} - {items_src.loc[items_src['ItemCode'] == x, 'ItemName'].values[0]}"
            ),
            key="item_selectbox_historico",
        )

    # Tabla
    df = _obtener_base(cardcode, itemcode)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # ──────────────────────────────────────────────────────────────────────────
    # Gráficos en fila: Cantidad | Monto (CLP) | Participación
    # Eje X NOMINAL con etiquetas preformateadas (MesLbl) para visibilidad total
    # ──────────────────────────────────────────────────────────────────────────
    if (cardcode is not None or itemcode is not None) and not df.empty:
        col_qty, col_amt, col_break = st.columns(3)

        # 1) Cantidad por mes
        with col_qty:
            qty_m = _serie_mensual(df[["DocDueDate", "Quantity"]], "Quantity")
            order_qty = qty_m["MesLbl"].tolist()
            axis_nom = alt.Axis(labelAngle=-90, labelOverlap=False, title="Mes")
            base_qty = (
                alt.Chart(qty_m)
                .mark_bar()
                .encode(
                    x=alt.X("MesLbl:N", sort=order_qty, axis=axis_nom),
                    y=alt.Y("Quantity:Q", title="Cantidad"),
                    tooltip=[
                        alt.Tooltip("Periodo:T", title="Periodo"),
                        alt.Tooltip("Quantity:Q", title="Cantidad", format=","),
                    ],
                )
                .properties(height=280, title="Cantidad por mes")
            )
            labels_qty = (
                alt.Chart(qty_m)
                .mark_text(color="white", dy=-6, baseline="bottom")
                .encode(
                    x=alt.X("MesLbl:N", sort=order_qty),
                    y="Quantity:Q",
                    text=alt.Text("Quantity:Q", format=","),
                )
            )
            st.altair_chart(alt.layer(base_qty, labels_qty), use_container_width=True)

        # 2) Monto por mes (CLP)
        with col_amt:
            amt_m = _serie_mensual(df[["DocDueDate", "LineTotal"]], "LineTotal")
            order_amt = amt_m["MesLbl"].tolist()
            axis_nom = alt.Axis(labelAngle=-90, labelOverlap=False, title="Mes")
            base_amt = (
                alt.Chart(amt_m)
                .mark_bar()
                .encode(
                    x=alt.X("MesLbl:N", sort=order_amt, axis=axis_nom),
                    y=alt.Y("LineTotal:Q", title="Monto (CLP)"),
                    tooltip=[
                        alt.Tooltip("Periodo:T", title="Periodo"),
                        alt.Tooltip("LineTotal:Q", title="Monto (CLP)", format=","),
                    ],
                )
                .properties(height=280, title="Monto por mes (CLP)")
            )
            labels_amt = (
                alt.Chart(amt_m)
                .mark_text(color="white", dy=-6, baseline="bottom")
                .encode(
                    x=alt.X("MesLbl:N", sort=order_amt),
                    y="LineTotal:Q",
                    text=alt.Text("LineTotal:Q", format=","),
                )
            )
            st.altair_chart(alt.layer(base_amt, labels_amt), use_container_width=True)

        # 3) Participación / Breakdown — barra horizontal %
        with col_break:
            if cardcode is not None and itemcode is None:
                # participación por ítem
                base = df.groupby(["ItemCode", "ItemName"], as_index=False)[
                    "LineTotal"
                ].sum()
                total = base["LineTotal"].sum()
                base["Share"] = np.where(total > 0, base["LineTotal"] / total, 0.0)
                base = base.sort_values("Share", ascending=False)

                bars = (
                    alt.Chart(base)
                    .mark_bar()
                    .encode(
                        y=alt.Y("ItemName:N", sort="-x", title="Ítem"),
                        x=alt.X(
                            "Share:Q", title="Participación", axis=alt.Axis(format="%")
                        ),
                        tooltip=[
                            alt.Tooltip("ItemCode:N", title="ItemCode"),
                            alt.Tooltip("ItemName:N", title="Ítem"),
                            alt.Tooltip("Share:Q", title="Participación", format=".1%"),
                            alt.Tooltip("LineTotal:Q", title="Monto (CLP)", format=","),
                        ],
                    )
                    .properties(height=280, title="Participación por ítem (%)")
                )
                labels = (
                    alt.Chart(base)
                    .mark_text(align="left", dx=4, color="white", baseline="middle")
                    .encode(
                        y="ItemName:N",
                        x="Share:Q",
                        text=alt.Text("Share:Q", format=".0%"),
                    )
                )
                st.altair_chart(alt.layer(bars, labels), use_container_width=True)

            elif itemcode is not None and cardcode is None:
                # participación por cliente (sin título adicional)
                base = df.groupby("CardName", as_index=False)["LineTotal"].sum()
                total = base["LineTotal"].sum()
                base["Share"] = np.where(total > 0, base["LineTotal"] / total, 0.0)
                base = base.sort_values("Share", ascending=False)

                bars = (
                    alt.Chart(base)
                    .mark_bar()
                    .encode(
                        y=alt.Y("CardName:N", sort="-x", title=""),
                        x=alt.X(
                            "Share:Q", title="Participación", axis=alt.Axis(format="%")
                        ),
                        tooltip=[
                            alt.Tooltip("CardName:N", title="Cliente"),
                            alt.Tooltip("Share:Q", title="Participación", format=".1%"),
                            alt.Tooltip("LineTotal:Q", title="Monto (CLP)", format=","),
                        ],
                    )
                    .properties(height=280)
                )
                labels = (
                    alt.Chart(base)
                    .mark_text(align="left", dx=4, color="white", baseline="middle")
                    .encode(
                        y="CardName:N",
                        x="Share:Q",
                        text=alt.Text("Share:Q", format=".0%"),
                    )
                )
                st.altair_chart(alt.layer(bars, labels), use_container_width=True)

            else:
                # contribución por OcrCode3
                por_centro = (
                    df.groupby("OcrCode3", as_index=False)["LineTotal"]
                    .sum()
                    .sort_values("LineTotal", ascending=False)
                )
                bars = (
                    alt.Chart(por_centro)
                    .mark_bar()
                    .encode(
                        y=alt.Y("OcrCode3:N", sort="-x", title="OcrCode3"),
                        x=alt.X("LineTotal:Q", title="Monto (CLP)"),
                        tooltip=[
                            "OcrCode3",
                            alt.Tooltip("LineTotal:Q", title="Monto (CLP)", format=","),
                        ],
                    )
                    .properties(height=280, title="Contribución por OcrCode3")
                )
                labels = (
                    alt.Chart(por_centro)
                    .mark_text(align="left", dx=4, color="white", baseline="middle")
                    .encode(
                        y="OcrCode3:N",
                        x="LineTotal:Q",
                        text=alt.Text("LineTotal:Q", format=","),
                    )
                )
                st.altair_chart(alt.layer(bars, labels), use_container_width=True)
