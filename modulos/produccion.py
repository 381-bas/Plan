# modulos/produccion.py
# B_PROD000: Módulo Producción SYMBIOS v1 (Asistencia + Proceso + KPIs + Avance OF)

from __future__ import annotations

import datetime as _dt
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import altair as alt

# Capa DB centralizada
from utils.db import run_query as _rq, _execute_write as _wr, DB_PATH  # noqa: F401


# ============================================================
# HELPERS / DB
# ============================================================


def q(sql: str, params: Optional[Tuple] = None) -> pd.DataFrame:
    return _rq(sql, params or ())


def w(sql: str, params: Optional[Tuple] = None) -> None:
    _wr(sql, params or ())


def to_iso(d: _dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def _to_Int64(series) -> pd.Series:
    """Normaliza a dtype pandas Int64 (permite NA)."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")


# ============================================================
# CATÁLOGOS / BASES
# ============================================================


@st.cache_data(show_spinner=False)
def get_estados_asistencia() -> pd.DataFrame:
    return q(
        "SELECT EstadoID, Nombre AS EstadoNombre FROM PR_Asis_Estado ORDER BY EstadoID"
    )


@st.cache_data(show_spinner=False)
def get_faltas_asistencia() -> pd.DataFrame:
    return q(
        "SELECT FaltaID, Nombre AS FaltaNombre FROM PR_Asis_Falta ORDER BY FaltaID"
    )


@st.cache_data(show_spinner=False)
def get_trabajadores_activos() -> pd.DataFrame:
    df = q(
        """
        SELECT TrabajadorID, Nombre, IFNULL(Empresa,'') AS Empresa
        FROM PR_Trabajador
        WHERE IFNULL(Activo,1)=1
        ORDER BY Nombre
    """
    )
    # Normalizo tipo del ID
    if "TrabajadorID" in df:
        df["TrabajadorID"] = _to_Int64(df["TrabajadorID"])
    return df


@st.cache_data(show_spinner=False)
def get_procesos() -> List[str]:
    df = q("SELECT Proceso FROM PR_Proceso ORDER BY Proceso")
    return df["Proceso"].tolist()


@st.cache_data(show_spinner=False)
def get_ratio_maquina(proceso: str) -> Dict[str, Optional[float]]:
    df = q(
        """
        SELECT Proceso, Vel_Hr, Horas_Jornada, Factor_Operacional,
               Tiempo_Disponible_Hr, Tiempo_Operativo_Hr, Porc_Utilizacion
        FROM PR_Ratio_Maquina
        WHERE Proceso = ?
    """,
        (proceso,),
    )
    if df.empty:
        return dict(
            Proceso=proceso,
            Vel_Hr=None,
            Horas_Jornada=None,
            Factor_Operacional=None,
            Tiempo_Disponible_Hr=None,
            Tiempo_Operativo_Hr=None,
            Porc_Utilizacion=None,
        )
    return df.iloc[0].to_dict()


@st.cache_data(show_spinner=False)
def get_ratio_maquina_efectiva(
    proceso: str, fecha: _dt.date
) -> Dict[str, Optional[float]]:
    """Override diario desde PR_Ratio_Maquina_Dia; si no existe, fallback a la maestra."""
    df = q(
        """
        SELECT Proceso, Vel_Hr, Horas_Jornada, Factor_Operacional,
               Tiempo_Disponible_Hr, Tiempo_Operativo_Hr, Porc_Utilizacion
        FROM PR_Ratio_Maquina_Dia
        WHERE Proceso = ? AND Fecha = ?
    """,
        (proceso, to_iso(fecha)),
    )
    if not df.empty:
        return df.iloc[0].to_dict()
    return get_ratio_maquina(proceso)


# ---- Catálogo de OF y utilidades OF ----


@st.cache_data(show_spinner=False)
def get_of_catalog() -> pd.DataFrame:
    return q(
        """
        SELECT OF_ID, Tipo, Estado, Cod_SAP, Producto, Cant_Plan,
               Fecha_Pedido, Fecha_Inicio, Fecha_Vencimiento
        FROM PR_Orden_Fabricacion
        ORDER BY OF_ID
    """
    )


@st.cache_data(show_spinner=False)
def get_of_info(of_id: str) -> Dict[str, Optional[str]]:
    df = q(
        """
        SELECT OF_ID, Producto, IFNULL(Cant_Plan,0) AS Cant_Plan
        FROM PR_Orden_Fabricacion
        WHERE OF_ID = ?
    """,
        (of_id,),
    )
    if df.empty:
        return dict(OF_ID=of_id, Producto=None, Cant_Plan=0.0)
    return df.iloc[0].to_dict()


@st.cache_data(show_spinner=False)
def get_ofs_por_proceso(proceso: str) -> List[str]:
    df = q(
        """
        SELECT DISTINCT OF
        FROM PR_Produccion_Registro
        WHERE Proceso = ? AND OF IS NOT NULL
        ORDER BY OF
    """,
        (proceso,),
    )
    return df["OF"].astype(str).tolist()


# ============================================================
# BLOQUE A · ASISTENCIA (funciones + UI)
# ============================================================


def fetch_asistencia_existing(fecha: _dt.date) -> pd.DataFrame:
    """Sólo filas guardadas en la DB para esa fecha."""
    fecha_s = to_iso(fecha)
    df = q(
        """
        SELECT a.AsistenciaID, a.Fecha, a.TrabajadorID,
               t.Nombre AS Trabajador, IFNULL(t.Empresa,'') AS Empresa,
               a.EstadoID, a.FaltaID, a.HorasEfectivas, IFNULL(a.Observacion,'') AS Observacion
        FROM PR_Asistencia a
        JOIN PR_Trabajador t ON t.TrabajadorID = a.TrabajadorID
        WHERE a.Fecha = ?
        ORDER BY t.Nombre
    """,
        (fecha_s,),
    )
    if df.empty:
        return df
    # Normalizo tipos
    df["TrabajadorID"] = _to_Int64(df["TrabajadorID"])
    df["EstadoID"] = (
        pd.to_numeric(df["EstadoID"], errors="coerce").fillna(1).astype(int)
    )
    df["FaltaID"] = pd.to_numeric(df["FaltaID"], errors="coerce").astype("Int64")
    df["HorasEfectivas"] = (
        pd.to_numeric(df["HorasEfectivas"], errors="coerce").fillna(8.0).astype(float)
    )
    return df


def fetch_asistencia_template(fecha: _dt.date) -> pd.DataFrame:
    """Plantilla para el día: todos los trabajadores activos con Presente (8h), sin guardar."""
    fecha_s = to_iso(fecha)
    df_t = get_trabajadores_activos().rename(columns={"Nombre": "Trabajador"})
    df_t["AsistenciaID"] = pd.NA
    df_t["Fecha"] = fecha_s
    df_t["EstadoID"] = 1  # Presente
    df_t["FaltaID"] = pd.NA
    df_t["HorasEfectivas"] = 8.0
    df_t["Observacion"] = ""
    cols = [
        "AsistenciaID",
        "Fecha",
        "TrabajadorID",
        "Trabajador",
        "Empresa",
        "EstadoID",
        "FaltaID",
        "HorasEfectivas",
        "Observacion",
    ]
    return df_t[cols].sort_values("Trabajador").reset_index(drop=True)


def validate_asistencia_rows(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    errores = []
    must_zero = {2, 3, 4}  # Ausente/Vacaciones/Licencia
    for _, r in df.iterrows():
        hh = float(pd.to_numeric(r.get("HorasEfectivas"), errors="coerce") or 0.0)
        est = int(pd.to_numeric(r.get("EstadoID"), errors="coerce") or 1)
        obs = (r.get("Observacion") or "").strip()
        if est in must_zero and hh > 0.0 and obs == "":
            errores.append(
                f"Trabajador {r.Trabajador}: Estado exige HH=0 o justificar en Observación."
            )
        if hh < 0 or hh > 24:
            errores.append(
                f"Trabajador {r.Trabajador}: HorasEfectivas fuera de rango (0..24)."
            )
    return (len(errores) == 0, errores)


def upsert_asistencia_rows(df_edit: pd.DataFrame) -> None:
    for _, r in df_edit.iterrows():
        params = (
            r["Fecha"],
            int(r["TrabajadorID"]),
            int(r["EstadoID"]),
            (None if pd.isna(r.get("FaltaID")) else int(r["FaltaID"])),
            float(r["HorasEfectivas"] or 0.0),
            (r.get("Observacion") or "").strip(),
        )
        w(
            """
            INSERT INTO PR_Asistencia
            (Fecha, TrabajadorID, EstadoID, FaltaID, HorasEfectivas, Observacion)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(TrabajadorID, Fecha) DO UPDATE SET
              EstadoID=excluded.EstadoID,
              FaltaID=excluded.FaltaID,
              HorasEfectivas=excluded.HorasEfectivas,
              Observacion=excluded.Observacion
        """,
            params,
        )


def asistencia_resumen_indicadores(df_view: pd.DataFrame) -> None:
    """Indicadores + 2 gráficos: Barras H (Personas) + Torta HH (donut con etiquetas)."""
    if df_view.empty:
        return

    df_sum = df_view.groupby("Estado", dropna=False, as_index=False).agg(
        Personas=("Trabajador", "count"),
        HH=("HorasEfectivas", "sum"),
    )
    estado_order = [
        "Presente",
        "Permiso",
        "Ausente",
        "Vacaciones",
        "Licencia",
        "Teletrabajo",
    ]
    df_sum["Estado"] = pd.Categorical(
        df_sum["Estado"], categories=estado_order, ordered=True
    )
    df_sum = df_sum.sort_values("Estado")

    cols = st.columns(min(6, max(1, len(df_sum))))
    for i, (_, row) in enumerate(df_sum.iterrows()):
        cols[i].metric(
            f"{row['Estado']}",
            f"{int(row['Personas'])} pers",
            help=f"HH: {row['HH']:.1f}",
        )

    c1, c2 = st.columns(2)

    chart_personas = (
        alt.Chart(df_sum)
        .mark_bar()
        .encode(
            y=alt.Y("Estado:N", sort=estado_order, title=""),
            x=alt.X("Personas:Q", title="Personas"),
            tooltip=[
                "Estado",
                alt.Tooltip("Personas:Q", format="d"),
                alt.Tooltip("HH:Q", title="HH", format=".1f"),
            ],
        )
        .properties(height=220)
    )
    with c1:
        st.altair_chart(
            chart_personas.configure_axis(
                labelFontSize=11, titleFontSize=12
            ).configure_view(strokeWidth=0),
            use_container_width=True,
        )

    total_hh = float(df_sum["HH"].sum())
    if total_hh <= 0:
        with c2:
            st.info("Sin HH registradas para esta fecha.")
    else:
        pie_base = (
            alt.Chart(df_sum)
            .transform_joinaggregate(total="sum(HH)")
            .transform_calculate(
                pct="datum.HH / datum.total",
                label='format(datum.HH, ".1f") + "h (" + format(datum.pct*100, ".0f") + "%)"',
            )
            .properties(height=220)
        )
        chart_pie = pie_base.mark_arc(outerRadius=90, innerRadius=35).encode(
            theta=alt.Theta("HH:Q"),
            color=alt.Color("Estado:N", sort=estado_order, legend=None),
            tooltip=[
                "Estado",
                alt.Tooltip("HH:Q", title="HH", format=".1f"),
                alt.Tooltip("pct:Q", title="%", format=".0%"),
            ],
        )
        labels = pie_base.mark_text(radius=105).encode(
            theta=alt.Theta("HH:Q"), text=alt.Text("label:N")
        )
        layered = chart_pie + labels
        st.altair_chart(layered.configure_view(strokeWidth=0), use_container_width=True)


def ui_asistencia():
    """UI de Asistencia – estable."""
    st.subheader("📋 Asistencia diaria")

    hoy = _dt.date.today()
    fecha = st.date_input(
        "Fecha", value=hoy, format="YYYY-MM-DD", key="prod_fecha_asist"
    )

    df_e = get_estados_asistencia()
    df_f = get_faltas_asistencia()
    est_id2name = dict(zip(df_e["EstadoID"], df_e["EstadoNombre"]))
    est_name2id = {v: k for k, v in est_id2name.items()}
    fal_id2name = dict(zip(df_f["FaltaID"], df_f["FaltaNombre"]))
    fal_name2id = {v: k for k, v in fal_id2name.items()}

    is_today = fecha == hoy

    df_exist = fetch_asistencia_existing(fecha)
    if df_exist.empty and not is_today:
        st.info("No hay asistencia registrada para esta fecha.")
        return
    if df_exist.empty and is_today:
        st.info(
            "📌 Pendiente por registrar la asistencia de hoy. Se muestran valores por defecto (Presente / 8h) sin guardar."
        )
        df_base = fetch_asistencia_template(fecha)
    else:
        df_base = df_exist

    df_view = df_base.copy()
    df_view["Estado"] = df_view["EstadoID"].map(est_id2name).fillna("Presente")
    df_view["Falta"] = df_view["FaltaID"].map(fal_id2name).fillna("—")

    cols_visible = [
        "Trabajador",
        "Empresa",
        "Estado",
        "Falta",
        "HorasEfectivas",
        "Observacion",
    ]
    edited = st.data_editor(
        df_view[cols_visible],
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "Trabajador": st.column_config.Column("Trabajador", disabled=True),
            "Empresa": st.column_config.Column("Empresa", disabled=True),
            "Estado": st.column_config.SelectboxColumn(
                "Estado", options=list(est_name2id.keys())
            ),
            "Falta": st.column_config.SelectboxColumn(
                "Falta", options=["—"] + list(fal_name2id.keys())
            ),
            "HorasEfectivas": st.column_config.NumberColumn(
                "HH", step=0.5, min_value=0.0, max_value=24.0
            ),
            "Observacion": st.column_config.TextColumn("Observación"),
        },
        key="prod_asistencia_editor",
    )

    st.markdown("#### 📊 Resumen de Asistencia")
    asistencia_resumen_indicadores(edited)

    edited = edited.copy()
    edited["EstadoID"] = edited["Estado"].map(est_name2id).astype(int)
    edited["FaltaID"] = edited["Falta"].map(
        lambda x: fal_name2id.get(x) if x != "—" else None
    )
    edited["Fecha"] = to_iso(fecha)
    edited = edited.merge(
        df_base[["Trabajador", "TrabajadorID", "Fecha"]],
        on=["Trabajador", "Fecha"],
        how="left",
    )

    c1, c2, _ = st.columns([1, 1, 6])
    with c1:
        if st.button("💾 Guardar asistencia", type="primary"):
            ok, errs = validate_asistencia_rows(edited)
            if not ok:
                st.error("No se pudo guardar: corrige las filas marcadas.")
                for e in errs:
                    st.warning(f"• {e}")
                st.stop()
            upsert_asistencia_rows(edited)
            st.success("Asistencia guardada.")
            st.rerun()
    with c2:
        st.download_button(
            "⬇️ Exportar CSV",
            edited[cols_visible].to_csv(index=False).encode("utf-8"),
            file_name=f"asistencia_{to_iso(fecha)}.csv",
            mime="text/csv",
        )


# ============================================================
# BLOQUE B · PROCESOS (funciones + UI)
# ============================================================


def has_asistencia_guardada(fecha: _dt.date) -> bool:
    df = q("SELECT COUNT(*) AS n FROM PR_Asistencia WHERE Fecha=?", (to_iso(fecha),))
    return int(df.iloc[0]["n"]) > 0


def fetch_elegibles_por_proceso(proceso: str) -> pd.DataFrame:
    df = q(
        """
        SELECT t.TrabajadorID, t.Nombre AS Trabajador, IFNULL(t.Empresa,'') AS Empresa
        FROM PR_Trabajador t
        JOIN PR_R_Trab_Puesto m ON m.TrabajadorID = t.TrabajadorID
        WHERE IFNULL(t.Activo,1)=1
          AND m.Proceso_Principal = ?
        ORDER BY t.Nombre
    """,
        (proceso,),
    )
    if "TrabajadorID" in df:
        df["TrabajadorID"] = _to_Int64(df["TrabajadorID"])
    return df


def fetch_hh_asignadas(fecha: _dt.date, proceso: str) -> pd.DataFrame:
    """(Legacy) Elegibles + HH asignadas del día (sin OF). Se mantiene por compatibilidad."""
    fecha_s = to_iso(fecha)
    df_t = fetch_elegibles_por_proceso(proceso)
    df_sum = q(
        """
        SELECT CAST(TrabajadorID AS INTEGER) AS TrabajadorID,
               SUM(IFNULL(HH_Asignadas,0.0)) AS HH_Asignadas
        FROM PR_Produccion_Registro
        WHERE Fecha=? AND Proceso=?
        GROUP BY TrabajadorID
    """,
        (fecha_s, proceso),
    )
    if "TrabajadorID" in df_sum:
        df_sum["TrabajadorID"] = _to_Int64(df_sum["TrabajadorID"])
    df = df_t.merge(df_sum, how="left", on="TrabajadorID")
    df["HH_Asignadas"] = (
        pd.to_numeric(df["HH_Asignadas"], errors="coerce").fillna(0.0).astype(float)
    )
    return (
        df[["TrabajadorID", "Trabajador", "HH_Asignadas", "Empresa"]]
        .sort_values("Trabajador")
        .reset_index(drop=True)
    )


def fetch_hh_produccion(fecha: _dt.date, proceso: str, of_id: str) -> pd.DataFrame:
    """Elegibles + valores ya capturados ese día/of (HH y Cantidad) para prellenar."""
    fecha_s = to_iso(fecha)
    base = fetch_elegibles_por_proceso(proceso).copy()
    # Merge robusto: casteo de ID en la consulta y normalizo dtype
    prev = q(
        """
        SELECT CAST(TrabajadorID AS INTEGER) AS TrabajadorID, 
               SUM(IFNULL(HH_Asignadas,0.0))  AS HH_Asignadas,
               SUM(IFNULL(Cant_Realizada,0.0)) AS Cant_Realizada
        FROM PR_Produccion_Registro
        WHERE Fecha=? AND Proceso=? AND OF=?
        GROUP BY TrabajadorID
    """,
        (fecha_s, proceso, of_id),
    )
    if "TrabajadorID" in prev:
        prev["TrabajadorID"] = _to_Int64(prev["TrabajadorID"])
    if "TrabajadorID" in base:
        base["TrabajadorID"] = _to_Int64(base["TrabajadorID"])
    df = base.merge(prev, how="left", on="TrabajadorID")
    df["HH_Asignadas"] = pd.to_numeric(df["HH_Asignadas"], errors="coerce").fillna(0.0)
    df["Cant_Realizada"] = pd.to_numeric(df["Cant_Realizada"], errors="coerce").fillna(
        0.0
    )
    # Solo mostramos columnas visibles (sin TrabajadorID)
    return df[["Trabajador", "Empresa", "HH_Asignadas", "Cant_Realizada"]].copy()


def upsert_produccion(
    df_edit: pd.DataFrame, fecha: _dt.date, proceso: str, of_id: str
) -> None:
    """Guarda filas por trabajador con HH y cantidad del día; recalcula acumulado OF."""
    fecha_s = to_iso(fecha)
    of = str(of_id)
    of_info = get_of_info(of)
    producto = (of_info.get("Producto") or "").strip()
    uom = "Blister" if "BLISTER" in producto.upper() else "Unid"

    # ID mapping (por nombre)
    elegibles = fetch_elegibles_por_proceso(proceso)[
        ["TrabajadorID", "Trabajador"]
    ].copy()
    elegibles["TrabajadorID"] = _to_Int64(elegibles["TrabajadorID"])
    data = df_edit.merge(elegibles, on="Trabajador", how="left")

    # Totales del día para acumulado
    total_dia = float(
        pd.to_numeric(data["Cant_Realizada"], errors="coerce").fillna(0.0).sum()
    )

    # Acumulado previo a la fecha
    prev_acum_df = q(
        """
        SELECT IFNULL(SUM(Cant_Realizada),0.0) AS PrevAcum
        FROM PR_Produccion_Registro
        WHERE Proceso=? AND OF=? AND Fecha<?
    """,
        (proceso, of, fecha_s),
    )
    prev_acum = float(prev_acum_df.iloc[0]["PrevAcum"] or 0.0)
    nuevo_acum = prev_acum + total_dia

    # Limpia el día/of (por si estamos regrabando)
    w(
        "DELETE FROM PR_Produccion_Registro WHERE Fecha=? AND Proceso=? AND OF=?",
        (fecha_s, proceso, of),
    )

    # Inserta filas (una por trabajador con algo que guardar)
    for _, r in data.iterrows():
        hh = float(pd.to_numeric(r.get("HH_Asignadas"), errors="coerce") or 0.0)
        qty = float(pd.to_numeric(r.get("Cant_Realizada"), errors="coerce") or 0.0)
        if hh <= 0 and qty <= 0:
            continue
        trab_id = None if pd.isna(r.get("TrabajadorID")) else int(r["TrabajadorID"])
        params = (
            fecha_s,
            proceso,
            of,
            producto,
            uom,
            None,  # Meta_Diaria = None
            qty,
            nuevo_acum,
            hh,
            r["Trabajador"],
            trab_id,
            None,
            None,
            None,
            None,
            "PROD_UI",
        )
        w(
            """
            INSERT INTO PR_Produccion_Registro
            (Fecha, Proceso, OF, Producto, UoM, Meta_Diaria, Cant_Realizada, Cant_Acumulada,
             HH_Asignadas, Operario, TrabajadorID, Tiempo_Operativo_Maq_Hr, Rendimiento_Hr,
             Velocidad_Maq, Merma, Observaciones)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            params,
        )


# ---- Capacitación ----


def fetch_capacitacion(fecha: _dt.date, proceso: str) -> pd.DataFrame:
    """Plantilla de elegibles + merge con PR_Capacitacion del día para prellenar."""
    fecha_s = to_iso(fecha)
    base = fetch_elegibles_por_proceso(proceso)[["Trabajador", "Empresa"]].copy()
    prev = q(
        """
        SELECT Operario AS Trabajador, IFNULL(HH_Asignadas,0.0) AS HH_Asignadas,
               IFNULL(Capacitacion,'') AS Capacitacion
        FROM PR_Capacitacion
        WHERE Fecha=? AND Proceso=?
    """,
        (fecha_s, proceso),
    )
    df = base.merge(prev, how="left", on="Trabajador")
    df["HH_Asignadas"] = pd.to_numeric(df["HH_Asignadas"], errors="coerce").fillna(0.0)
    df["Capacitacion"] = df["Capacitacion"].fillna("")
    return df[["Trabajador", "Empresa", "HH_Asignadas", "Capacitacion"]].copy()


def upsert_capacitacion(df_edit: pd.DataFrame, fecha: _dt.date, proceso: str) -> None:
    fecha_s = to_iso(fecha)
    # Limpia el día/proceso
    w("DELETE FROM PR_Capacitacion WHERE Fecha=? AND Proceso=?", (fecha_s, proceso))
    for _, r in df_edit.iterrows():
        hh = float(pd.to_numeric(r.get("HH_Asignadas"), errors="coerce") or 0.0)
        cap = (r.get("Capacitacion") or "").strip()
        if hh <= 0 and cap == "":
            continue
        w(
            """
            INSERT INTO PR_Capacitacion (Fecha, Proceso, Operario, HH_Asignadas, Capacitacion)
            VALUES (?, ?, ?, ?, ?)
        """,
            (fecha_s, proceso, r["Trabajador"], hh, cap),
        )


# ---- Avance por OF ----


def fetch_avance_of(
    fecha: _dt.date, proceso: str, of_id: Optional[str] = None
) -> pd.DataFrame:
    fecha_s = to_iso(fecha)
    of_id = str(of_id) if of_id is not None else None

    # Filtro y params correctamente "aplanados"
    of_clause = "AND OF=?" if of_id else ""
    params_dia = (fecha_s, proceso) + ((of_id,) if of_id else ())
    params_acum = (proceso, fecha_s) + ((of_id,) if of_id else ())
    plan_clause = "WHERE OF_ID=?" if of_id else ""
    params_plan = (of_id,) if of_id else ()

    df_dia = q(
        f"""
        SELECT OF, MAX(IFNULL(Producto,'')) AS Producto,
               SUM(IFNULL(Cant_Realizada,0.0)) AS Real_dia,
               SUM(IFNULL(HH_Asignadas,0.0))   AS HH_dia
        FROM PR_Produccion_Registro
        WHERE Fecha=? AND Proceso=? AND OF IS NOT NULL
        {of_clause}
        GROUP BY OF
    """,
        params_dia,
    )

    df_acum = q(
        f"""
        SELECT OF,
               SUM(IFNULL(Cant_Realizada,0.0)) AS Real_acum,
               SUM(IFNULL(HH_Asignadas,0.0))   AS HH_acum
        FROM PR_Produccion_Registro
        WHERE Proceso=? AND OF IS NOT NULL AND Fecha<=?
        {of_clause}
        GROUP BY OF
    """,
        params_acum,
    )

    df_plan = q(
        f"""
        SELECT OF_ID AS OF, IFNULL(Producto,'') AS Producto,
               IFNULL(Cant_Plan, NULL) AS Plan
        FROM PR_Orden_Fabricacion
        {plan_clause}
    """,
        params_plan,
    )

    df = df_plan.merge(df_dia, how="left", on=["OF", "Producto"]).merge(
        df_acum, how="left", on="OF"
    )
    df[["Real_dia", "HH_dia", "Real_acum", "HH_acum", "Plan"]] = df[
        ["Real_dia", "HH_dia", "Real_acum", "HH_acum", "Plan"]
    ].fillna(0.0)
    df["%Avance"] = (df["Real_acum"] / df["Plan"]).where(df["Plan"] > 0, None)
    cols = [
        "OF",
        "Producto",
        "Plan",
        "Real_dia",
        "Real_acum",
        "%Avance",
        "HH_dia",
        "HH_acum",
    ]
    return df[cols].sort_values("OF")


def fetch_avance_diario(proceso: str, of_id: str, fecha_max: _dt.date) -> pd.DataFrame:
    df = q(
        """
        SELECT Fecha,
               SUM(IFNULL(Cant_Realizada,0.0)) AS Real_dia,
               SUM(IFNULL(HH_Asignadas,0.0))   AS HH_dia
        FROM PR_Produccion_Registro
        WHERE Proceso=? AND OF=? AND Fecha<=?
        GROUP BY Fecha
        ORDER BY Fecha
    """,
        (proceso, of_id, to_iso(fecha_max)),
    )
    if df.empty:
        return df
    df["Fecha"] = pd.to_datetime(df["Fecha"])
    df["Acumulado"] = df["Real_dia"].cumsum()
    return df


# ============================================================
# UI · PROCESOS
# ============================================================


def ui_procesos():
    """UI de Procesos – editable."""
    st.subheader("⚙️ Procesos")

    hoy = _dt.date.today()
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])  # Fecha · Proceso · OF · Origen
    with c1:
        fecha = st.date_input(
            "Fecha", value=hoy, format="YYYY-MM-DD", key="prod_fecha_proc"
        )
    with c2:
        procesos = get_procesos()
        if not procesos:
            st.info("No hay procesos configurados en PR_Proceso.")
            return
        default_proc = "BLISTERA" if "BLISTERA" in procesos else procesos[0]
        idx = procesos.index(default_proc) if default_proc in procesos else 0
        proceso = st.selectbox("Proceso", options=procesos, index=idx)
    with c3:
        ofs_proc = get_ofs_por_proceso(proceso)
        if not ofs_proc:
            df_ofcat = get_of_catalog()
            ofs_proc = df_ofcat["OF_ID"].astype(str).tolist()
        if not ofs_proc:
            st.info("No hay OF disponibles aún. Crea una en la pestaña '🧾 OF'.")
            return
        of_sel = st.selectbox("OF", options=ofs_proc, index=0, key="proc_of_sel")
        of_sel = str(of_sel)
    with c4:
        modo = st.selectbox("Origen", options=["Producción", "Capacitación"], index=0)

    # Requiere asistencia guardada
    if not has_asistencia_guardada(fecha):
        st.info(
            "Para esta fecha no hay asistencia guardada. Registra asistencia antes de asignar HH o ver KPIs."
        )
        return

    # ---------------- Parámetros del día (override) ----------------
    st.write("### Parámetros del día")
    ratio_eff = get_ratio_maquina_efectiva(proceso, fecha)
    p1, p2, p3, p4 = st.columns(4)
    vel = p1.number_input(
        "Velocidad (unid/hr)",
        min_value=0.0,
        value=float(ratio_eff.get("Vel_Hr") or 0.0),
        step=100.0,
    )
    hj = p2.number_input(
        "Horas Jornada",
        min_value=0.0,
        value=float(ratio_eff.get("Horas_Jornada") or 8.0),
        step=0.5,
    )
    fo = p3.number_input(
        "Factor Operacional",
        min_value=0.0,
        max_value=1.0,
        value=float(ratio_eff.get("Factor_Operacional") or 0.85),
        step=0.01,
    )
    toh = p4.number_input(
        "Tiempo Operativo (hr)",
        min_value=0.0,
        value=float(ratio_eff.get("Tiempo_Operativo_Hr") or 0.0),
        step=0.5,
    )

    if st.button(
        "💾 Guardar parámetros del día", key="save_ratio_day", type="secondary"
    ):
        w(
            """
            INSERT INTO PR_Ratio_Maquina_Dia
            (Fecha, Proceso, Vel_Hr, Horas_Jornada, Factor_Operacional, Tiempo_Operativo_Hr)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(Proceso, Fecha) DO UPDATE SET
              Vel_Hr=excluded.Vel_Hr,
              Horas_Jornada=excluded.Horas_Jornada,
              Factor_Operacional=excluded.Factor_Operacional,
              Tiempo_Operativo_Hr=excluded.Tiempo_Operativo_Hr
        """,
            (to_iso(fecha), proceso, vel, hj, fo, toh),
        )
        st.success("Parámetros del día guardados.")
        st.rerun()

    # ---------------- Asignación (Producción o Capacitación) ----------------
    st.write("### 👥 Asignación de HH (personas)")
    if modo == "Producción":
        df_prod = fetch_hh_produccion(fecha, proceso, of_sel)
        df_prod_edit = st.data_editor(
            df_prod,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Trabajador": st.column_config.Column("Trabajador", disabled=True),
                "Empresa": st.column_config.Column("Empresa", disabled=True),
                "HH_Asignadas": st.column_config.NumberColumn(
                    "HH asignadas", step=0.5, min_value=0.0
                ),
                "Cant_Realizada": st.column_config.NumberColumn(
                    "Cantidad producida (día)", step=1.0, min_value=0.0
                ),
            },
            key="prod_editor_prod",
        )
        if st.button("💾 Guardar producción del día", type="primary", key="save_prod"):
            upsert_produccion(df_prod_edit, fecha, proceso, of_sel)
            st.success("Producción del día guardada.")
            st.rerun()
    else:
        df_cap = fetch_capacitacion(fecha, proceso)
        df_cap_edit = st.data_editor(
            df_cap,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Trabajador": st.column_config.Column("Trabajador", disabled=True),
                "Empresa": st.column_config.Column("Empresa", disabled=True),
                "HH_Asignadas": st.column_config.NumberColumn(
                    "HH capacitación", step=0.5, min_value=0.0
                ),
                "Capacitacion": st.column_config.TextColumn("Capacitación"),
            },
            key="prod_editor_cap",
        )
        if st.button("💾 Guardar capacitación del día", type="primary", key="save_cap"):
            upsert_capacitacion(df_cap_edit, fecha, proceso)
            st.success("Capacitación guardada.")
            st.rerun()

    # ---------------- KPIs reales (OF) ----------------
    st.write("### 📈 KPIs del día (OF)")
    df_of = fetch_avance_of(fecha, proceso, of_sel)
    if df_of.empty:
        st.info("Sin datos de producción para la OF seleccionada hasta la fecha.")
        real_dia = real_acum = hh_dia = plan = 0.0
        pct = None
    else:
        row = df_of.iloc[0]
        plan = float(row["Plan"] or 0.0)
        real_dia = float(row["Real_dia"] or 0.0)
        real_acum = float(row["Real_acum"] or 0.0)
        hh_dia = float(row["HH_dia"] or 0.0)
        pct = (real_acum / plan) if plan > 0 else None

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("HH del día", f"{hh_dia:.2f}")
    k2.metric("Real del día (unid)", f"{real_dia:,.0f}")
    k3.metric("Acumulado OF / Plan", f"{real_acum:,.0f} / {plan:,.0f}")
    k4.metric("% Avance", f"{(pct*100):.1f}%" if pct is not None else "—")

    # ---------------- Evolución diaria (OF) ----------------
    st.write("### 📉 Evolución diaria")
    df_ts = fetch_avance_diario(proceso, of_sel, fecha)
    if df_ts.empty:
        st.info("Sin registros de producción para la OF seleccionada.")
    else:
        bars = (
            alt.Chart(df_ts)
            .mark_bar()
            .encode(
                x=alt.X("yearmonthdate(Fecha):T", title="Fecha"),
                y=alt.Y("Real_dia:Q", title="Real del día"),
                tooltip=[
                    alt.Tooltip("Fecha:T"),
                    alt.Tooltip("Real_dia:Q", format=",.0f"),
                    alt.Tooltip("Acumulado:Q", format=",.0f"),
                ],
            )
            .properties(height=240)
        )

        line = (
            alt.Chart(df_ts)
            .mark_line(point=True)
            .encode(
                x=alt.X("yearmonthdate(Fecha):T", title="Fecha"),
                y=alt.Y("Acumulado:Q", title="Acumulado"),
                tooltip=[
                    alt.Tooltip("Fecha:T"),
                    alt.Tooltip("Acumulado:Q", format=",.0f"),
                ],
            )
        )

        layered = (
            (bars + line)
            .configure_axis(labelFontSize=11, titleFontSize=12)
            .configure_view(strokeWidth=0)
        )
        st.altair_chart(layered, use_container_width=True)

    # ---------------- Tabla Avance por OF ----------------
    st.write("### 🧾 Avance por OF (plan vs real)")
    st.dataframe(df_of, use_container_width=True, hide_index=True)
    st.download_button(
        "⬇️ Exportar avance OF (CSV)",
        (
            df_of
            if not df_of.empty
            else pd.DataFrame(
                columns=[
                    "OF",
                    "Producto",
                    "Plan",
                    "Real_dia",
                    "Real_acum",
                    "%Avance",
                    "HH_dia",
                    "HH_acum",
                ]
            )
        )
        .to_csv(index=False)
        .encode("utf-8"),
        file_name=f"avance_OF_{proceso}_{of_sel or 'ALL'}_{to_iso(fecha)}.csv",
        mime="text/csv",
    )

    # ---------------- Paradas (al final) ----------------
    st.write("### 🧰 Paradas (Plan/No plan)")
    df_p = q(
        """
        SELECT ParadaID, Proceso, Fecha, IFNULL(Horas_Parada,0.0) AS Horas_Parada,
               IFNULL(Tipo,'') AS Tipo, IFNULL(Motivo,'') AS Motivo,
               IFNULL(Accion,'') AS Accion, IFNULL(Observacion,'') AS Observacion
        FROM PR_Paradas
        WHERE Fecha = ? AND Proceso = ?
        ORDER BY ParadaID
    """,
        (to_iso(fecha), proceso),
    )
    df_p_edit = st.data_editor(
        (
            df_p
            if not df_p.empty
            else pd.DataFrame(
                [
                    dict(
                        ParadaID=None,
                        Proceso=proceso,
                        Fecha=to_iso(fecha),
                        Horas_Parada=0.0,
                        Tipo="",
                        Motivo="",
                        Accion="",
                        Observacion="",
                    )
                ]
            )
        ),
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "ParadaID": st.column_config.Column("ParadaID", disabled=True),
            "Proceso": st.column_config.Column("Proceso", disabled=True),
            "Fecha": st.column_config.Column("Fecha", disabled=True),
            "Horas_Parada": st.column_config.NumberColumn(
                "Horas parada", step=0.1, min_value=0.0
            ),
            "Tipo": st.column_config.TextColumn("Tipo"),
            "Motivo": st.column_config.TextColumn("Motivo"),
            "Accion": st.column_config.TextColumn("Acción"),
            "Observacion": st.column_config.TextColumn("Observación"),
        },
        key="prod_paradas_editor",
    )
    if st.button("💾 Guardar paradas", type="secondary"):
        w(
            "DELETE FROM PR_Paradas WHERE Fecha=? AND Proceso=?",
            (to_iso(fecha), proceso),
        )
        for _, r in df_p_edit.iterrows():
            horas = float(pd.to_numeric(r.get("Horas_Parada"), errors="coerce") or 0.0)
            tipo = (r.get("Tipo") or "").strip()
            motivo = (r.get("Motivo") or "").strip()
            accion = (r.get("Accion") or "").strip()
            obs = (r.get("Observacion") or "").strip()
            if horas <= 0 and not any([tipo, motivo, accion, obs]):
                continue
            w(
                """
                INSERT INTO PR_Paradas (Proceso, Fecha, Horas_Parada, Tipo, Motivo, Accion, Observacion)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (proceso, to_iso(fecha), horas, tipo, motivo, accion, obs),
            )
        st.success("Paradas guardadas.")
        st.rerun()


# ============================================================
# Pestaña OF (crear/editar)
# ============================================================


def ui_of():
    st.subheader("🧾 Órdenes de Fabricación")
    df = get_of_catalog()
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown("#### ➕ Crear / editar OF")
    with st.form("form_of"):
        c1, c2, c3 = st.columns(3)
        with c1:
            of_id = st.text_input("OF_ID", value="")
            tipo = st.text_input("Tipo", value="Estándar")
            estado = st.text_input("Estado", value="Planif.")
        with c2:
            cod_sap = st.text_input("Cod_SAP", value="")
            producto = st.text_input("Producto", value="")
            cant_plan = st.number_input("Cant_Plan", min_value=0.0, step=100.0)
        with c3:
            f_ped = st.date_input("Fecha_Pedido", value=_dt.date(2025, 8, 4))
            f_ini = st.date_input("Fecha_Inicio", value=_dt.date(2025, 8, 4))
            f_ven = st.date_input("Fecha_Vencimiento", value=_dt.date(2025, 9, 15))
        submitted = st.form_submit_button("💾 Guardar OF", type="primary")

    if submitted:
        if not of_id or not producto:
            st.error("OF_ID y Producto son obligatorios.")
        else:
            w(
                """
                INSERT INTO PR_Orden_Fabricacion
                (OF_ID, Tipo, Estado, Cod_SAP, Producto, Cant_Plan, Fecha_Pedido, Fecha_Inicio, Fecha_Vencimiento)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(OF_ID) DO UPDATE SET
                  Tipo=excluded.Tipo,
                  Estado=excluded.Estado,
                  Cod_SAP=excluded.Cod_SAP,
                  Producto=excluded.Producto,
                  Cant_Plan=excluded.Cant_Plan,
                  Fecha_Pedido=excluded.Fecha_Pedido,
                  Fecha_Inicio=excluded.Fecha_Inicio,
                  Fecha_Vencimiento=excluded.Fecha_Vencimiento
            """,
                (
                    of_id,
                    tipo,
                    estado,
                    cod_sap,
                    producto,
                    float(cant_plan),
                    to_iso(f_ped),
                    to_iso(f_ini),
                    to_iso(f_ven),
                ),
            )
            st.success(f"OF {of_id} guardada.")
            st.rerun()


# ============================================================
# RUN · Contenedor de pestañas
# ============================================================


def run():
    # Estilo igual a ventas.py (padding alto y título compacto)
    st.markdown(
        """
        <style>
            .block-container { padding-top: 4rem !important; }
            .titulo-ajustado  { margin: .5rem 0 1rem; font-size: 1.2rem; font-weight: 500; }
        </style>
        <div class="titulo-ajustado">🧰 Producción · Asistencia / Procesos / OF</div>
        """,
        unsafe_allow_html=True,
    )

    tabs = st.tabs(["📋 Asistencia", "⚙️ Procesos", "🧾 OF"])
    with tabs[0]:
        ui_asistencia()
    with tabs[1]:
        ui_procesos()
    with tabs[2]:
        ui_of()
