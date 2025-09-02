# core/stock.py
from __future__ import annotations
from typing import Optional, Tuple, List, Dict
import pandas as pd
import numpy as np
import streamlit as st
import altair as alt
from core.historico import _serie_mensual_hist
from utils.db import run_query, DB_PATH


# B_DBQ020: Catálogo de ítems desde Stock (cacheado)
# # ∂B_DBQ020/∂B0
@st.cache_data(show_spinner=False)
def _obtener_items_stock() -> pd.DataFrame:
    sql = """
        SELECT DISTINCT ItemCode, ItemName
        FROM Stock
        ORDER BY ItemCode
    """
    df = run_query(sql, DB_PATH)
    return df if not df.empty else pd.DataFrame(columns=["ItemCode", "ItemName"])


# B_DBQ021: Catálogo de bodegas desde Stock (cacheado)
# # ∂B_DBQ021/∂B0
@st.cache_data(show_spinner=False)
def _obtener_bodegas_stock() -> pd.DataFrame:
    sql = """
        SELECT DISTINCT WhsCode, WhsName
        FROM Stock
        ORDER BY WhsCode
    """
    df = run_query(sql, DB_PATH)
    return df if not df.empty else pd.DataFrame(columns=["WhsCode", "WhsName"])


# B_DBQ022: Catálogo de grupos (ItmsGrpNam) desde Stock (cacheado)
# # ∂B_DBQ022/∂B0
@st.cache_data(show_spinner=False)
def _obtener_grupos() -> pd.DataFrame:
    sql = """
        SELECT DISTINCT ItmsGrpNam
        FROM Stock
        WHERE ItmsGrpNam IS NOT NULL AND ItmsGrpNam <> ''
        ORDER BY ItmsGrpNam
    """
    df = run_query(sql, DB_PATH)
    return df if not df.empty else pd.DataFrame(columns=["ItmsGrpNam"])


# B_DBQ023: Bodegas disponibles para un ítem (cacheado)
# # ∂B_DBQ023/∂B0
@st.cache_data(show_spinner=False)
def _bodegas_para_item(itemcode: str) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT WhsCode, WhsName
        FROM Stock
        WHERE ItemCode = ?
        ORDER BY WhsCode
    """
    df = run_query(sql, DB_PATH, (itemcode,))
    return df if not df.empty else pd.DataFrame(columns=["WhsCode", "WhsName"])


# B_DBQ024: Ítems disponibles para una bodega (cacheado)
# # ∂B_DBQ024/∂B0
@st.cache_data(show_spinner=False)
def _items_para_bodega(whscode: str) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT ItemCode, ItemName
        FROM Stock
        WHERE WhsCode = ?
        ORDER BY ItemCode
    """
    df = run_query(sql, DB_PATH, (whscode,))
    return df if not df.empty else pd.DataFrame(columns=["ItemCode", "ItemName"])


# B_DBQ025: Lotes disponibles filtrados por ítem y bodega (cacheado)
# # ∂B_DBQ025/∂B0
@st.cache_data(show_spinner=False)
def _lotes_disponibles(itemcode: Optional[str], whscode: Optional[str]) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT Lote
        FROM Stock
        WHERE 1=1
    """
    params: List = []
    if itemcode:
        sql += " AND ItemCode = ?"
        params.append(itemcode)
    if whscode:
        sql += " AND WhsCode = ?"
        params.append(whscode)
    sql += " ORDER BY Lote"
    df = run_query(sql, DB_PATH, tuple(params))
    return df if not df.empty else pd.DataFrame(columns=["Lote"])


# B_DBQ026: Constructor de SQL base para Stock con filtros opcionales
# # ∂B_DBQ026/∂B0
def _build_sql(
    itemcode: Optional[str],
    whscode: Optional[str],
    grupo: Optional[str],
    lote: Optional[str],
) -> Tuple[str, Tuple]:
    sql = """
    SELECT
        ItemCode, ItemName,
        WhsCode, WhsName,
        ItmsGrpNam,
        Costo, Stock_Total,
        Lote, Stock_Lote,
        Asignado, En_Transito,
        FechaIngreso, FechaVenc, FechaFabric,
        Uom, SegmArea, SegmLNeg, SegmCluster
    FROM Stock
    WHERE 1=1
    """
    params: List = []
    if itemcode:
        sql += " AND ItemCode = ?"
        params.append(itemcode)
    if whscode:
        sql += " AND WhsCode = ?"
        params.append(whscode)
    if grupo:
        sql += " AND ItmsGrpNam = ?"
        params.append(grupo)
    if lote:
        sql += " AND Lote = ?"
        params.append(lote)
    sql += " ORDER BY ItemCode, WhsCode, COALESCE(Lote,''), FechaVenc"
    return sql, tuple(params)


# B_DBQ027: Consulta base de Stock con derivados y orden amigable
# # ∂B_DBQ027/∂B0
def _obtener_base(
    itemcode: Optional[str],
    whscode: Optional[str],
    grupo: Optional[str],
    lote: Optional[str],
) -> pd.DataFrame:
    sql, params = _build_sql(itemcode, whscode, grupo, lote)
    df = run_query(sql, DB_PATH, params)
    if df.empty:
        return df
    # Derivados útiles
    with np.errstate(invalid="ignore"):
        df["ValorTotal"] = (
            pd.to_numeric(df["Costo"], errors="coerce").fillna(0)
            * pd.to_numeric(df["Stock_Total"], errors="coerce").fillna(0)
        ).round(2)
    # Orden de columnas amigable
    cols = [
        "ItemCode",
        "ItemName",
        "ItmsGrpNam",
        "Uom",
        "WhsCode",
        "WhsName",
        "Lote",
        "Stock_Lote",
        "Stock_Total",
        "Asignado",
        "En_Transito",
        "Costo",
        "ValorTotal",
        "FechaIngreso",
        "FechaFabric",
        "FechaVenc",
        "SegmArea",
        "SegmLNeg",
        "SegmCluster",
    ]
    df = df[cols]
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Serie mensual (FechaVenc o FechaIngreso) con autocompletar meses
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


# B_HDF020: Etiquetas centrales por año para línea temporal
# # ∂B_HDF020/∂B0
def _labels_de_anio_tiempo(df_mensual: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for y, sub in df_mensual.groupby("Year"):
        sub = sub.sort_values("Periodo").reset_index(drop=True)
        if sub.empty:
            continue
        mid = len(sub) // 2
        rows.append({"Periodo": sub.loc[mid, "Periodo"], "YearStr": str(y)})
    return pd.DataFrame(rows)


# B_UIX020: Gráfico de barras mensual con etiquetas y orden nominal
# # ∂B_UIX020/∂B0
def _chart_barras_mensual(
    df_m: pd.DataFrame, y_field: str, y_title: str, chart_title: str
):
    """
    Construye bloque: barras 232 + fila meses 24 (vertical, 270°) + fila año 24 = 280 px
    Evita labelExpr para máxima compatibilidad.
    """
    if df_m is None or df_m.empty:
        return (
            alt.Chart(pd.DataFrame({"x": [], "y": []}))
            .mark_bar()
            .properties(height=280, title=chart_title)
        )

    base = (
        alt.Chart(df_m)
        .mark_bar()
        .encode(
            x=alt.X("Periodo:T", axis=None),
            y=alt.Y(f"{y_field}:Q", title=y_title),
            tooltip=[
                alt.Tooltip("Periodo:T", title="Periodo"),
                alt.Tooltip(f"{y_field}:Q", title=y_title, format=","),
            ],
        )
        .properties(height=232, title=chart_title)
    )
    labels = (
        alt.Chart(df_m)
        .mark_text(color="white", dy=-6, baseline="bottom")
        .encode(
            x=alt.X("Periodo:T"),
            y=f"{y_field}:Q",
            text=alt.Text(f"{y_field}:Q", format=","),
        )
    )
    meses_row = (
        alt.Chart(df_m)
        .mark_text(angle=270, baseline="top")
        .encode(x=alt.X("Periodo:T", axis=None), text="MonthAbbr:N")
        .properties(height=24)
    )
    years = _labels_de_anio_tiempo(df_m)
    anio_row = (
        alt.Chart(years)
        .mark_text(baseline="top", fontWeight="bold")
        .encode(x=alt.X("Periodo:T", axis=None), text="YearStr:N")
        .properties(height=24)
    )
    return alt.vconcat(
        alt.layer(base, labels), meses_row, anio_row, spacing=2
    ).resolve_scale(x="shared")


# B_UIX021: Vista de stock con filtros, tabla y gráficos Altair
# # ∂B_UIX021/∂B0
def vista_stock(
    slpcode: int, _unused=None  # slpcode intencionalmente ignorado
) -> None:
    print(f"[STOCK.INFO] start slpcode={slpcode}")

    st.markdown("### 🧱 Stock — detalle por ítem / bodega / lote")

    # Filtros dependientes (Item ⟷ Bodega) + Grupo + Lote
    sel_item_prev = st.session_state.get("item_selectbox_stock")
    sel_whs_prev = st.session_state.get("whs_selectbox_stock")

    items_src = (
        _items_para_bodega(sel_whs_prev) if sel_whs_prev else _obtener_items_stock()
    )
    bodegas_src = (
        _bodegas_para_item(sel_item_prev) if sel_item_prev else _obtener_bodegas_stock()
    )
    grupos_src = _obtener_grupos()

    print(
        f"[STOCK.INFO] src items_shape={getattr(items_src,'shape',None)} "
        f"bodegas_shape={getattr(bodegas_src,'shape',None)} grupos_shape={getattr(grupos_src,'shape',None)}"
    )

    # Mapping para format_func seguros
    items_map: Dict[str, str] = dict(
        zip(items_src["ItemCode"], items_src.get("ItemName", ""))
    )
    bodegas_map: Dict[str, str] = dict(
        zip(bodegas_src["WhsCode"], bodegas_src.get("WhsName", ""))
    )

    col_f1, col_f2, col_f3, col_f4 = st.columns(4)

    with col_f1:
        item_opts: List[Optional[str]] = [None] + items_src["ItemCode"].tolist()
        itemcode = st.selectbox(
            "Ítem:",
            options=item_opts,
            index=0,
            format_func=lambda x: (
                "(Todos)" if x is None else f"{x} - {items_map.get(x,'')}"
            ),
            key="item_selectbox_stock",
        )

    with col_f2:
        whs_opts: List[Optional[str]] = [None] + bodegas_src["WhsCode"].tolist()
        whscode = st.selectbox(
            "Bodega:",
            options=whs_opts,
            index=0,
            format_func=lambda x: (
                "(Todas)" if x is None else f"{x} - {bodegas_map.get(x,'')}"
            ),
            key="whs_selectbox_stock",
        )

    with col_f3:
        grupo_opts: List[Optional[str]] = [None] + grupos_src["ItmsGrpNam"].tolist()
        grupo = st.selectbox(
            "Grupo:",
            options=grupo_opts,
            index=0,
            format_func=lambda x: "(Todos)" if x is None else f"{x}",
            key="grupo_selectbox_stock",
        )

    # Lotes disponibles tras aplicar item/bodega
    lotes_src = _lotes_disponibles(itemcode, whscode)
    print(
        f"[STOCK.INFO] filtros_pre lotes_rows={len(lotes_src) if lotes_src is not None else 0}"
    )
    with col_f4:
        lote_opts: List[Optional[str]] = [None] + lotes_src["Lote"].dropna().astype(
            str
        ).tolist()
        lote = st.selectbox(
            "Lote:",
            options=lote_opts,
            index=0,
            format_func=lambda x: (
                "(Todos)" if (x is None or str(x).strip() == "") else str(x)
            ),
            key="lote_selectbox_stock",
        )

    print(
        f"[STOCK.INFO] filtros item={itemcode} whs={whscode} grupo={grupo} lote={lote}"
    )

    # Tabla
    df = _obtener_base(itemcode, whscode, grupo, lote)
    print(f"[STOCK.INFO] result shape={getattr(df,'shape',None)}")
    st.dataframe(df, use_container_width=True, hide_index=True)

    if df.empty:
        print("[STOCK.INFO] result empty -> return")
        st.info("No hay registros para los filtros seleccionados.")
        return

    # ──────────────────────────────────────────────────────────────────────────
    # Gráficos en fila (altura uniforme 280): Vencimiento mensual | Stock por bodega | Participación por grupo
    # ──────────────────────────────────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)

    # 1) Vencimiento mensual
    with c1:
        venc_series = _serie_mensual_hist(
            df[["FechaVenc", "Stock_Lote"]], "FechaVenc", "Stock_Lote"
        )
        print(
            f"[STOCK.INFO] chart.venc_mensual rows={0 if venc_series is None else len(venc_series)}"
        )
        if venc_series.empty:
            st.caption("Sin fechas de vencimiento para los filtros actuales.")
        else:
            st.altair_chart(
                _chart_barras_mensual(
                    venc_series,
                    "Stock_Lote",
                    "Stock por vencer (unid.)",
                    "Vencimientos por mes",
                ),
                use_container_width=True,
            )

    # 2) Stock por bodega
    with c2:
        por_bodega = (
            df.groupby(["WhsName"], as_index=False)["Stock_Total"]
            .sum()
            .sort_values("Stock_Total", ascending=False)
        )
        print(f"[STOCK.INFO] chart.por_bodega rows={len(por_bodega)}")
        bars = (
            alt.Chart(por_bodega)
            .mark_bar()
            .encode(
                y=alt.Y("WhsName:N", sort="-x", title="Bodega"),
                x=alt.X("Stock_Total:Q", title="Stock (unid.)"),
                tooltip=[
                    alt.Tooltip("WhsName:N", title="Bodega"),
                    alt.Tooltip("Stock_Total:Q", title="Stock", format=","),
                ],
            )
            .properties(height=280, title="Stock por bodega")
        )
        labels = (
            alt.Chart(por_bodega)
            .mark_text(align="left", dx=4, color="white", baseline="middle")
            .encode(
                y="WhsName:N",
                x="Stock_Total:Q",
                text=alt.Text("Stock_Total:Q", format=","),
            )
        )
        st.altair_chart(alt.layer(bars, labels), use_container_width=True)

    # 3) Participación por grupo
    with c3:
        por_grupo = df.groupby(["ItmsGrpNam"], as_index=False)["Stock_Total"].sum()
        total = por_grupo["Stock_Total"].sum()
        print(f"[STOCK.INFO] chart.por_grupo rows={len(por_grupo)} total_stock={total}")
        if total > 0:
            por_grupo["Share"] = por_grupo["Stock_Total"] / total
        else:
            por_grupo["Share"] = 0.0
        por_grupo = por_grupo.sort_values("Share", ascending=False)
        bars = (
            alt.Chart(por_grupo)
            .mark_bar()
            .encode(
                y=alt.Y("ItmsGrpNam:N", sort="-x", title="Grupo"),
                x=alt.X("Share:Q", title="Participación", axis=alt.Axis(format="%")),
                tooltip=[
                    alt.Tooltip("ItmsGrpNam:N", title="Grupo"),
                    alt.Tooltip("Share:Q", title="Participación", format=".1%"),
                    alt.Tooltip("Stock_Total:Q", title="Stock (unid.)", format=","),
                ],
            )
            .properties(height=280, title="Participación por grupo (%)")
        )
        labels = (
            alt.Chart(por_grupo)
            .mark_text(align="left", dx=4, color="white", baseline="middle")
            .encode(
                y="ItmsGrpNam:N", x="Share:Q", text=alt.Text("Share:Q", format=".0%")
            )
        )
        st.altair_chart(alt.layer(bars, labels), use_container_width=True)
