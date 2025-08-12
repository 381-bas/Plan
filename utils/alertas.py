# utils\alertas.py
import sqlite3
import json
import time
import hashlib
import pandas as pd
import streamlit as st
from typing import Any, Tuple, List
from datetime import datetime
from contextlib import closing

from utils.db import run_query, DB_PATH
from utils.utils_buffers import _refrescar_buffer_ui


def evaluar_alertas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula alertas OV vs Forecast con estas reglas:
      â€¢ Firme â†’ compara cantidad y fecha.
      â€¢ Proyectado (Cant_Forecast > 0) â†’ â€œâš ï¸ Mover a Firmeâ€ si ya hay OV.

    Devuelve el mismo DF + columnas:
        - Alerta_Fecha
        - Alerta_Cantidad
    """
    df = df.copy()
    df = df.loc[:, ~df.columns.duplicated()].copy()  # limpia cabeceras duplicadas
    df = df.drop(
        columns=["Alerta_Fecha", "Alerta_Cantidad"], errors="ignore"
    )  # borra alertas previas

    # ---------- helpers -------------------------------------------------
    def _mes(d):
        if pd.isnull(d):
            return ""
        return pd.to_datetime(d, format="%Y-%m-%d", errors="coerce").strftime("%Y-%m")

    def _alerta_fecha(row, mes_fc):
        mes_ov = _mes(row["DocDueDate"])
        if mes_fc == "":
            return "Sin Forecast"
        if mes_ov == "":
            return "Sin OV"
        if mes_fc == mes_ov:
            return "âœ“"
        return "âš ï¸ Adelantada" if mes_ov < mes_fc else "âš ï¸ Atrasada"

    def _cant_firme(row):
        if pd.isna(row["OpenQty"]) or pd.isna(row["Cant_Forecast"]):
            return "Sin datos"
        return (
            "âœ“"
            if float(row["OpenQty"]) == float(row["Cant_Forecast"])
            else "âš ï¸ Difiere"
        )

    def _cant_proy(row):
        if pd.isna(row["OpenQty"]) or row["OpenQty"] == 0:
            return "Sin OV"
        return "âš ï¸ Mover a Firme"

    # ---------- segmentaciÃ³n -------------------------------------------
    df_firme = df[df["TipoForecast"].str.upper() == "FIRME"].copy()
    df_proj = df[
        (df["TipoForecast"].str.upper() == "PROYECTADO") & (df["Cant_Forecast"] > 0)
    ].copy()

    # Crea columnas por defecto (garantiza existencia aunque el bloque estÃ© vacÃ­o)
    for part in (df_firme, df_proj):
        part["Alerta_Fecha"] = ""
        part["Alerta_Cantidad"] = ""

    # ---------- cÃ¡lculo alertas Firme ----------------------------------
    if not df_firme.empty:
        mes_fc = df_firme["FechEntr_Forecast"].apply(_mes)
        df_firme["Alerta_Fecha"] = df_firme.apply(
            lambda r: _alerta_fecha(r, mes_fc.loc[r.name]), axis=1
        )
        df_firme["Alerta_Cantidad"] = df_firme.apply(_cant_firme, axis=1)

    # ---------- cÃ¡lculo alertas Proyectado -----------------------------
    if not df_proj.empty:
        mes_fc = df_proj["FechEntr_Forecast"].apply(_mes)
        df_proj["Alerta_Fecha"] = df_proj.apply(
            lambda r: _alerta_fecha(r, mes_fc.loc[r.name]), axis=1
        )
        df_proj["Alerta_Cantidad"] = df_proj.apply(_cant_proy, axis=1)

    # ---------- salida --------------------------------------------------
    return pd.concat([df_firme, df_proj], ignore_index=True)


# B_FCS015: Inconsistencias Forecast vs OV
# âˆ‚B_FCS015/âˆ‚B0
# FUNCION A REUTILIZAR
def obtener_inconsistencias_forecast(
    slpcode: int, cardcode: str | None = None, db_path: str = DB_PATH
) -> pd.DataFrame:
    """
    Devuelve las lÃ­neas de Forecast_Detalle que presentan
    inconsistencias con Ã“rdenes de Venta (ORDR/RDR1) a partir
    del mes actual.
    Flags:
      â€¢ flag_sin_OV       â†’ no existe OV ligada
      â€¢ flag_fecha_menor  â†’ OV con fecha de entrega < fecha forecast
      â€¢ flag_qty_distinta â†’ cantidades diferentes
    """
    # â€”â€”â€” WHERE dinÃ¡mico segÃºn filtros â€”â€”â€”
    base_where = """
        WHERE date(fd.FechEntr) >= date('now','start of month')
          AND fd.SlpCode = ?
    """
    params: tuple[Any, ...] = (slpcode,)

    if cardcode:
        base_where += " AND fd.CardCode = ?"
        params += (cardcode,)

    sql = f"""
        WITH Base AS (
            SELECT
                fd.ForecastID,
                fd.ItemCode,
                fd.CardCode,
                fd.SlpCode,
                date(fd.FechEntr)           AS FechEntr,
                fd.Cant                     AS Cant_Forecast,
                COALESCE(SUM(r.Quantity),0) AS Cant_OV,
                MIN(o.DocDueDate)           AS Fecha_OV
            FROM   Forecast_Detalle fd
            LEFT JOIN RDR1 r
                   ON r.ItemCode = fd.ItemCode
            LEFT JOIN ORDR o
                   ON o.DocEntry = r.DocEntry
                  AND o.CardCode = fd.CardCode
                  AND o.SlpCode  = fd.SlpCode
            {base_where}
            GROUP BY fd.ForecastID, fd.ItemCode, fd.CardCode,
                     fd.SlpCode, fd.FechEntr
        )
        SELECT *
        FROM (
            SELECT *,
                   (Cant_OV = 0)              AS flag_sin_OV,
                   (Fecha_OV < FechEntr)      AS flag_fecha_menor,
                   (Cant_OV <> Cant_Forecast) AS flag_qty_distinta
            FROM Base
        )
        WHERE flag_sin_OV = 1
           OR flag_fecha_menor = 1
           OR flag_qty_distinta = 1
        ORDER BY FechEntr, ItemCode;
    """

    return run_query(sql, db_path, params)


def df_alerta_is_valid(df: pd.DataFrame) -> bool:
    """
    Valida el DataFrame que llega desde el editor.
    Reglas mÃ­nimas (DSL A1_ALERTAS_FORECAST):
      â€¢ ForecastID  > 0 y no nulos
      â€¢ ItemCode    no vacÃ­o
      â€¢ Cant_Forecast â‰¥ 0 y finito
      â€¢ FechEntr    fecha parseable
      â€¢ SlpCode     presente y consistente (opcional segÃºn tu flujo)
    """
    return all(
        [
            df["ForecastID"].notna().all() and (df["ForecastID"] > 0).all(),
            df["ItemCode"].astype(str).str.len().gt(0).all(),
            pd.to_numeric(df["Cant_Forecast"], errors="coerce").ge(0).all(),
            pd.to_datetime(df["FechEntr"], format="%Y-%m-%d", errors="coerce"),
            # Descomenta si quisieras validar SlpCode tambiÃ©n
            # df["SlpCode"].notna().all() and (df["SlpCode"] > 0).all(),
        ]
    )


MAX_RETRY = 5  # reintentos ante DBLocked


# B_ALR002: AplicaciÃ³n de cambios sobre Forecast_Detalle   âˆ‚B_ALR002/âˆ‚B0
# FUNCIÃ“N A REUTILZIAR
def _aplicar_cambios_alertas(
    df_original: pd.DataFrame, df_editado: pd.DataFrame
) -> None:
    """
    Detecta diferencias entre DF original y editado, persiste los cambios
    (cantidad y/o fecha) en Forecast_Detalle con:
        â€¢ transacciÃ³n BEGIN IMMEDIATE + retry
        â€¢ UPSERT (INSERT â€¦ ON CONFLICT â€¦ DO UPDATE)
        â€¢ traza C2_TRACE con usuario, slpcode y hash de diff
    """

    # â”€â”€ 0 Â· validaciÃ³n de datos â€” regla R1 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if not df_alerta_is_valid(df_editado):
        st.error("âŒ Datos incompletos o invÃ¡lidos â€“ corrige antes de guardar.")
        return

    # â”€â”€ 1 Â· diff: arma lista de cambios y log detallado â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    cambios: List[Tuple[Any, ...]] = []
    diff_log: List[dict] = []

    print("[DEBUG-ALR] Columnas recibidas:", df_original.columns.tolist())

    for idx in df_original.index:
        old = df_original.loc[idx]
        new = df_editado.loc[idx]

        if (
            old["Cant_Forecast"] != new["Cant_Forecast"]
            or old["FechEntr"] != new["FechEntr"]
        ):
            cambios.append(
                (
                    new["Cant_Forecast"],  # SET Cant
                    new["FechEntr"],  # SET FechEntr
                    int(old["ForecastID"]),  # PK ForecastID
                    old["ItemCode"],  # PK ItemCode
                )
            )
            diff_log.append(
                {
                    "ForecastID": int(old["ForecastID"]),
                    "ItemCode": old["ItemCode"],
                    "SlpCode": int(old["SlpCode"]),
                    "Cant_old": old["Cant_Forecast"],
                    "Cant_new": new["Cant_Forecast"],
                    "Fecha_old": old["FechEntr"],
                    "Fecha_new": new["FechEntr"],
                }
            )

    if not cambios:
        st.info("â„¹ï¸ No se detectaron modificaciones.")
        return

    # â”€â”€ 2 Â· transacciÃ³n + UPSERT con retry â€” reglas R2 & R3 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    user_email = st.session_state.get("user_email", "desconocido")
    intento, ok = 0, False

    while intento < MAX_RETRY and not ok:
        try:
            with closing(sqlite3.connect(DB_PATH, timeout=5.0)) as conn, conn:
                conn.execute("BEGIN IMMEDIATE;")

                conn.executemany(
                    """
                    INSERT INTO Forecast_Detalle
                           (Cant, FechEntr, ForecastID, ItemCode)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(ForecastID, ItemCode)
                    DO UPDATE SET Cant     = excluded.Cant,
                                  FechEntr = excluded.FechEntr;
                """,
                    cambios,
                )

                # â”€â”€ 3 Â· C2_TRACE â€” regla R4 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                payload = {
                    "usuario": user_email,
                    "diff": diff_log,
                }
                payload_hash = hashlib.sha256(
                    json.dumps(payload, default=str).encode()
                ).hexdigest()

                conn.execute(
                    """
                    INSERT INTO C2_TRACE
                          (timestamp, usuario, slpcode, accion,
                           bloque, severity, payload_hash, detalle)
                    VALUES (?,?,?,?,?,?,?,?)
                """,
                    (
                        datetime.now().isoformat(timespec="seconds"),
                        user_email,
                        int(diff_log[0]["SlpCode"]),
                        "alert_fix",
                        "A1_ALERTAS_FORECAST_DSL",
                        "info",
                        payload_hash,
                        json.dumps(payload, default=str),
                    ),
                )
            ok = True

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                intento += 1
                time.sleep(0.6 * intento)  # back-off exponencial
            else:
                raise

    if not ok:
        st.error("ðŸš« No se pudieron aplicar cambios por bloqueo de base.")
        return

    # â”€â”€ 4 Â· feedback UI + refresco de buffers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    st.success(f"âœ… {len(cambios)} cambios aplicados.")

    # refresca sÃ³lo una vez por ForecastID editado
    ids_para_refrescar = {c[2] for c in cambios}  # 3.er campo = ForecastID
    for fc_id in ids_para_refrescar:
        _refrescar_buffer_ui(
            forecast_id=fc_id,
            key_buffer="buffer_alertas",  # â† pon aquÃ­ tu clave real
            db_path=DB_PATH,
        )

    st.experimental_rerun()


def editor_cambios_forecast(df: pd.DataFrame, *, key: str = "ed_alertas") -> None:
    """
    Permite editar Cantidad y Fecha del Forecast directamente desde una tabla.
    """
    st.caption(
        "âœï¸ Puedes corregir cantidades y fechas directamente desde esta tabla."
    )

    # ---------- verificaciÃ³n de columna ----------
    if "FechEntr" not in df.columns:
        st.error(
            "âš ï¸ No se encontrÃ³ la columna 'FechEntr' despuÃ©s de normalizar. Revisa los logs."
        )
        print("âŒ [DEBUG-EDITOR] columnas recibidas:", df.columns.tolist())
        return

    # ---------- conversiÃ³n robusta a datetime ----------
    #  â€¢ Acepta  'YYYY-MM'  Ã³  'YYYY-MM-DD'
    df = df.copy()
    df["FechEntr"] = (
        df["FechEntr"]
        .astype(str)
        .str.slice(0, 10)  # garantiza largo mÃ¡ximo 'YYYY-MM-DD'
        .apply(lambda x: x if len(x) == 10 else f"{x}-01")
    )
    df["FechEntr"] = pd.to_datetime(df["FechEntr"], format="%Y-%m-%d", errors="coerce")

    # LOG antes de mostrar editor
    print(
        "ðŸ” [DEBUG-EDITOR] dtypes convertidos:\n",
        df.dtypes[["Cant_Forecast", "FechEntr"]],
    )
    print(df[["Cant_Forecast", "FechEntr"]].head(5).to_string(index=False))

    column_config = {
        "Cant_Forecast": st.column_config.NumberColumn(
            label="Cantidad comprometida", min_value=0.0, step=1.0
        ),
        "FechEntr": st.column_config.DateColumn(
            label="Fecha de Entrega", format="YYYY-MM-DD"
        ),
    }

    df_edit = st.data_editor(
        df[["Cant_Forecast", "FechEntr"]],
        key=key,
        column_config=column_config,
        use_container_width=True,
        num_rows="fixed",
    )

    if st.button("ðŸ’¾ Guardar correcciones", type="primary"):
        _aplicar_cambios_alertas(df, df_edit)
        st.success("âœ… Cambios aplicados. Refresca para volver a analizar.")


def obtener_meses_disponibles_OV(db_path=DB_PATH):
    # Devuelve lista de meses 'YYYY-MM' desde el mes actual en adelante con OV abiertas y fecha de vencimiento
    sql = """
        SELECT DISTINCT strftime('%Y-%m', o.DocDueDate) as Mes
        FROM ORDR o
        JOIN RDR1 r ON o.DocEntry = r.DocEntry
        WHERE r.LineStatus = 'O'
          AND o.DocDueDate >= date('now','start of month')
        ORDER BY Mes ASC
    """
    df = run_query(sql, db_path)
    return df["Mes"].tolist() if not df.empty else []


def consultar_ordenes_venta_alertas_mes(
    meses_yyyy_mm: list[str],
    tipo_forecast: str = "Firme",
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Devuelve OV para los meses YYYY-MM indicados (usando DocDueDate), solo lÃ­neas abiertas,
    mostrando forecast Firme/Proyectado/Ambos asociado.
    """
    if not meses_yyyy_mm:
        # Si no hay meses seleccionados, retornar DataFrame vacÃ­o
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(meses_yyyy_mm))
    filtros = [f"strftime('%Y-%m', o.DocDueDate) IN ({placeholders})"]
    params = list(meses_yyyy_mm)
    filtros.append("r.LineStatus = 'O'")  # solo abiertas

    # Filtro de tipo forecast dinÃ¡mico
    if tipo_forecast == "Firme":
        tipo_fc_sql = "AND UPPER(fd.TipoForecast) = 'FIRME'"
    elif tipo_forecast == "Proyectado":
        tipo_fc_sql = "AND UPPER(fd.TipoForecast) = 'PROYECTADO'"
    else:  # Ambos
        tipo_fc_sql = "AND UPPER(fd.TipoForecast) IN ('FIRME', 'PROYECTADO')"

    where_clause = " AND ".join(filtros)
    sql = f"""
        SELECT
            o.DocEntry,
            r.ItemCode,
            r.Dscription,
            r.OpenQty,               
            r.Price,
            r.Currency,
            r.OcrCode3,
            r.LineStatus,
            o.DocDueDate,
            o.CardCode,
            fd.TipoForecast,
            fd.FechEntr AS FechEntr_Forecast,
            fd.Cant     AS Cant_Forecast
        FROM RDR1 r
        JOIN ORDR o ON r.DocEntry = o.DocEntry
        LEFT JOIN Forecast_Detalle fd ON
            fd.ItemCode = r.ItemCode
            AND fd.CardCode = o.CardCode
            AND fd.OcrCode3 = r.OcrCode3
            {tipo_fc_sql}
            AND strftime('%Y-%m', fd.FechEntr) = strftime('%Y-%m', o.DocDueDate)
        WHERE {where_clause}
        ORDER BY o.DocDueDate ASC, o.DocEntry ASC
    """

    return run_query(sql, db_path, tuple(params))


def obtener_meses_disponibles_Forecast(
    tipo_forecast: str = "Ambos",
    db_path: str = DB_PATH,
) -> list[str]:
    """
    Devuelve lista Ãºnica de meses (YYYY-MM) futuros desde el forecast (no OV).
    """
    from datetime import datetime

    mes_actual = datetime.now().strftime("%Y-%m")

    tipo_sql = {
        "Firme": "AND UPPER(TipoForecast) = 'FIRME'",
        "Proyectado": "AND UPPER(TipoForecast) = 'PROYECTADO'",
    }.get(tipo_forecast, "AND UPPER(TipoForecast) IN ('FIRME','PROYECTADO')")

    sql = f"""
        SELECT DISTINCT strftime('%Y-%m', FechEntr) as Mes
        FROM Forecast_Detalle
        WHERE strftime('%Y-%m', FechEntr) >= ?
        {tipo_sql}
        ORDER BY Mes
    """
    df = run_query(sql, db_path, (mes_actual,))
    return df["Mes"].tolist() if not df.empty else []


# â¬‡ï¸ NUEVO o reemplazar versiÃ³n previa
def consultar_forecast_sin_ov(
    meses_yyyy_mm: list[str],
    tipo_forecast: str = "Ambos",
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    """
    Forecast (Firme/Proyectado/Ambos) sin ninguna OV ligada,
    filtrado sÃ³lo para meses >= hoy.
    """
    if not meses_yyyy_mm:
        return pd.DataFrame()

    placeholders = ",".join("?" * len(meses_yyyy_mm))

    tipo_sql = {
        "Firme": "AND UPPER(fd.TipoForecast) = 'FIRME'",
        "Proyectado": "AND UPPER(fd.TipoForecast) = 'PROYECTADO'",
    }.get(tipo_forecast, "AND UPPER(fd.TipoForecast) IN ('FIRME','PROYECTADO')")

    sql = f"""
        WITH F AS (
            SELECT
                fd.CardCode,
                fd.ItemCode,
                fd.OcrCode3,
                fd.TipoForecast,
                fd.Cant,
                strftime('%Y-%m', fd.FechEntr) AS MesYM   -- YYYY-MM
            FROM Forecast_Detalle fd
            WHERE strftime('%Y-%m', fd.FechEntr) IN ({placeholders})
              {tipo_sql}
        ),
        OV AS (
            SELECT DISTINCT
                o.CardCode,
                r.ItemCode,
                r.OcrCode3,
                strftime('%Y-%m', o.DocDate) AS MesYM
            FROM ORDR o
            JOIN RDR1 r ON r.DocEntry = o.DocEntry
            WHERE r.LineStatus IN ('O','C')
              AND strftime('%Y-%m', o.DocDate) IN ({placeholders})
        )
        SELECT  F.*          -- sÃ³lo los que no aparecen en OV
        FROM    F
        LEFT    JOIN OV USING (CardCode, ItemCode, OcrCode3, MesYM)
        WHERE   OV.CardCode IS NULL
        ORDER BY F.CardCode, F.ItemCode, F.TipoForecast, F.MesYM;
    """
    params = tuple(meses_yyyy_mm) * 2
    df = run_query(sql, db_path, params)

    # â”€â”€ MantÃ©n mes numÃ©rico â€œ07â€¦12â€ para el pivot â”€â”€
    if not df.empty:
        df["Mes"] = df["MesYM"].str[-2:]  # '2025-07' âžœ '07'
    return df


# B_ALR003: Vista de alertas por cliente sin filtrar por SlpCode  âˆ‚B_ALR003/âˆ‚B0
def vista_alertas_cliente(slpcode: int) -> None:
    st.markdown("### ðŸ” Ã“rdenes abiertas vs Forecast")

    # ---------- obtener meses y forecast disponibles ----------
    meses_disponibles = obtener_meses_disponibles_OV()
    if not meses_disponibles:
        st.info("No hay Ã³rdenes de venta futuras registradas.")
        return

    mes_actual = datetime.now().strftime("%Y-%m")
    if mes_actual not in meses_disponibles:
        meses_disponibles.insert(0, mes_actual)

    # ---------- filtros principales en una fila ----------
    col1, col2 = st.columns([2, 1])
    with col1:
        meses_sel = st.multiselect(
            "ðŸ“† Mes OV (DocDueDate):", meses_disponibles, default=[mes_actual]
        )
    with col2:
        tipo_forecast_sel = st.selectbox(
            "ðŸ”€ Tipo Forecast:", options=["Firme", "Proyectado", "Ambos"], index=0
        )

    if not meses_sel:
        st.info("Selecciona al menos un mes para ver resultados.")
        return

    # ---------- consulta base y evaluaciÃ³n ----------
    df = consultar_ordenes_venta_alertas_mes(meses_sel, tipo_forecast_sel)
    if df.empty:
        st.success("âœ… No hay OV para los filtros seleccionados.")
        return

    df = evaluar_alertas(df)  # â† aÃ±ade columnas de alerta

    # ---------- normalizaciÃ³n de columna FechEntr ----------
    if "FechEntr" not in df.columns:
        posibles = ["FechEntr_Forecast", "FechEntrForecast", "FechEntr_fc"]
        for alt in posibles:
            if alt in df.columns:
                df = df.rename(columns={alt: "FechEntr"})
                break

    df["FechEntr"] = pd.to_datetime(df["FechEntr"], format="%Y-%m-%d", errors="coerce")
    df["DocEntry"] = df["DocEntry"].astype(str)
    df["FechEntr"] = df["FechEntr"].dt.date

    # ---------- KPIs rÃ¡pidos ----------
    col_tot, col_fech, col_cant = st.columns(3)
    col_tot.metric("ðŸ“¦ LÃ­neas OV", f"{len(df):,}")
    col_fech.metric("âš ï¸ Alerta Fecha", (df["Alerta_Fecha"] != "âœ“").sum())
    col_cant.metric("ðŸ“‰ Alerta Cantidad", (df["Alerta_Cantidad"] != "âœ“").sum())

    # ---------- tabla principal ----------
    columnas_originales = [
        "DocDueDate",
        "DocEntry",
        "ItemCode",
        "Dscription",
        "OpenQty",
        "Price",
        "Currency",
        "CardCode",
        "TipoForecast",
        "Cant_Forecast",
        "Alerta_Fecha",
        "Alerta_Cantidad",
    ]

    columnas_renombradas = {
        "DocDueDate": "FechEntr",
        "DocEntry": "NÂ°Or.",
        "ItemCode": "Cod",
        "Dscription": "DescripciÃ³n",
        "OpenQty": "Qty",
        "Price": "Price",
        "Currency": "$",
        "CardCode": "CardCode",
        "TipoForecast": "Forecast",
        "Cant_Forecast": "Cant_Forecast",
        "Alerta_Fecha": "Alerta_Fecha",
        "Alerta_Cantidad": "Alerta_Cantidad",
    }

    df_vista = df[columnas_originales].rename(columns=columnas_renombradas)

    st.caption(
        "Solo se visualizan lÃ­neas de OV abiertas. La comparaciÃ³n se realiza contra la fecha comprometida (FechEntr) y la cantidad de forecast."
    )
    st.dataframe(df_vista, use_container_width=True)

    # ---------- editor inline ----------
    if st.checkbox("âœï¸ Editar alertas inline"):
        editor_cambios_forecast(df, key="ed_alertas_cliente")


def vista_forecast_sin_ov():
    st.markdown("### ðŸ“„ Forecast sin OV asociada")

    tipo_forecast_sel = "Firme"  # â† Siempre Firme

    # Ahora los meses provienen del forecast real, no de las OV
    meses_disponibles = obtener_meses_disponibles_Forecast(tipo_forecast_sel)
    mes_actual = datetime.now().strftime("%Y-%m")
    if not meses_disponibles:
        st.info("No hay meses futuros con forecast registrado.")
        return

    meses_sel = st.multiselect(
        "Meses a analizar (FechEntr):",
        options=meses_disponibles,
        default=(
            [mes_actual] if mes_actual in meses_disponibles else [meses_disponibles[0]]
        ),
    )
    if not meses_sel:
        st.info("Selecciona al menos un mes.")
        return

    df_sin_ov = consultar_forecast_sin_ov(meses_sel, tipo_forecast_sel)
    if df_sin_ov.empty:
        st.success("âœ… Todo el forecast seleccionado cuenta con OV asociada.")
        return

    # Columnas segÃºn meses seleccionados y presentes en forecast
    meses_col = sorted(set(meses_sel) | set(df_sin_ov["MesYM"].unique()))
    df_pivot = df_sin_ov.pivot_table(
        index=["CardCode", "ItemCode", "OcrCode3", "TipoForecast"],
        columns="MesYM",
        values="Cant",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    for col in meses_col:
        if col not in df_pivot.columns:
            df_pivot[col] = 0
    orden = ["CardCode", "ItemCode", "OcrCode3", "TipoForecast"] + meses_col
    df_pivot = df_pivot[orden].sort_values(["CardCode", "ItemCode"])

    st.dataframe(df_pivot, use_container_width=True)


def render_alertas_forecast(slpcode: int):
    """
    Vista principal de Alertas Forecast, organizada por sub-tabs:
    1. DiagnÃ³stico Forecast vs Realidad
    2. Forecast sin OV
    """
    tabs = st.tabs(["ðŸ” Dif: OV/Forecast", "ðŸ“„ Forecast sin OV"])

    with tabs[0]:
        vista_alertas_cliente(
            slpcode
        )  # Esta funciÃ³n ya incluye resumen macro + desglose

    with tabs[1]:
        vista_forecast_sin_ov()


def _mes(d):
    if pd.isnull(d):
        return ""
    return pd.to_datetime(d, format="%Y-%m-%d", errors="coerce").strftime("%Y-%m")
