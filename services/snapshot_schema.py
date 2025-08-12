# services/snapshot_schema.py
# B_SNAP001: Creación de tablas Snapshot_Forecast y Snapshot_Detalle
import pandas as pd
from datetime import date
from utils.db import run_query, _execute_write, DB_PATH

SQL_CREATE_SNAPSHOT_FORECAST = """
CREATE TABLE IF NOT EXISTS Snapshot_Forecast (
    SnapshotID     INTEGER PRIMARY KEY AUTOINCREMENT,
    SlpCode        INTEGER,
    CardCode       TEXT,
    ForecastID     INTEGER,
    Fecha_Snapshot DATE,
    Observacion    TEXT
);
"""

SQL_CREATE_SNAPSHOT_DETALLE = """
CREATE TABLE IF NOT EXISTS Snapshot_Detalle (
    SnapshotID    INTEGER,
    ItemCode      TEXT,
    TipoForecast  TEXT,
    OcrCode3      TEXT,
    FechEntr      DATE,
    Linea         TEXT,
    Cant_Esperada REAL,
    PrecioUN      REAL,
    DocCur        TEXT,
    Cant_Real     REAL DEFAULT 0,
    Delta         REAL DEFAULT 0,
    Delta_Porc    REAL DEFAULT 0,
    Observacion   TEXT,
    FOREIGN KEY (SnapshotID) REFERENCES Snapshot_Forecast(SnapshotID)
);
"""

# B_SNAP002: Actualización de Snapshot_Detalle con datos reales desde OINV/INV1
def actualizar_snapshot_realidad(db_path: str = DB_PATH) -> None:
    print("🔍 Actualizando Cant_Real / Delta… (masivo)")

    try:
        _execute_write(
            """
            UPDATE Snapshot_Detalle
            SET Cant_Real = COALESCE((
                SELECT SUM(i.Quantity)
                FROM   INV1 i
                JOIN   OINV o ON o.DocEntry = i.DocEntry
                JOIN   Snapshot_Forecast sf ON sf.SnapshotID = Snapshot_Detalle.SnapshotID
                WHERE  o.CardCode = sf.CardCode
                AND  i.ItemCode = Snapshot_Detalle.ItemCode
                AND  o.DocDate BETWEEN
                        date(Snapshot_Detalle.FechEntr,'start of month')
                    AND date(Snapshot_Detalle.FechEntr,'start of month','+1 month','-1 day')
            ),0),
                Delta      = Cant_Real - Cant_Esperada,
                Delta_Porc = CASE
                            WHEN Cant_Esperada > 0
                            THEN ROUND(Cant_Real * 1.0 / Cant_Esperada, 4)
                            ELSE 0
                            END;
            """,
            db_path=db_path,
        )
        print("✅ Actualización masiva terminada.")
    except Exception:
        raise





def obtener_cardcode(snapshot_id: int, db_path: str = DB_PATH) -> str | None:
    df = run_query(
        "SELECT CardCode FROM Snapshot_Forecast WHERE SnapshotID = ?",
        params=(snapshot_id,),
        db_path=db_path
    )
    return df.iloc[0]["CardCode"] if not df.empty else None

def obtener_forecastid_cliente(cardcode: str, db_path: str = DB_PATH) -> int | None:
    df = run_query(
        "SELECT MAX(ForecastID) AS ForecastID FROM Forecast_Detalle WHERE CardCode = ?",
        params=(cardcode,),
        db_path=db_path
    )
    if not df.empty and not pd.isna(df.iloc[0]["ForecastID"]):
        return int(df.iloc[0]["ForecastID"])
    return None

# B_SNAP004: Exportar log ForecastCero

def exportar_forecast_cero_log(db_path: str = DB_PATH, csv_path: str = "forecast_cero_log.csv") -> None:
    df = run_query("SELECT * FROM Snapshot_Detalle WHERE Observacion = 'ForecastCero'", db_path=db_path)
    if df.empty:
        print("📭 No hay registros ForecastCero para exportar.")
    else:
        df.to_csv(csv_path, index=False)
        print(f"📤 Log ForecastCero exportado a {csv_path}")

# B_SNAP003: Ejecución de Snapshot desde Forecast vigente

def ejecutar_snapshot_forecast(db_path: str = DB_PATH) -> None:
    # (sin BEGIN / COMMIT explícitos)
    try:
        df_combos = run_query(
            "SELECT DISTINCT SlpCode, CardCode FROM Forecast_Detalle",
            db_path=db_path,
        )
        if df_combos.empty:
            print("🟡 No hay combinaciones activas en Forecast_Detalle.")
            return

        print(f"📸 Iniciando generación de snapshot para {len(df_combos)} combinaciones…")
        inserts_detalle: list[list] = []

        for _, row in df_combos.iterrows():
            slpcode  = int(row["SlpCode"])
            cardcode = row["CardCode"]

            df_id = run_query(
                """
                SELECT MAX(ForecastID) AS ForecastID
                FROM Forecast_Detalle
                WHERE SlpCode = ? AND CardCode = ?
                """,
                params=(slpcode, cardcode),
                db_path=db_path,
            )
            if df_id.empty or pd.isna(df_id.iloc[0]["ForecastID"]):
                print(f"⚠️ Sin ForecastID para {cardcode}")
                continue

            forecast_id = int(df_id.iloc[0]["ForecastID"])

            df_forecast = run_query(
                """
                SELECT ItemCode, TipoForecast, OcrCode3, FechEntr,
                       Linea, Cant AS Cant_Esperada, PrecioUN, DocCur
                FROM Forecast_Detalle
                WHERE SlpCode = ? AND CardCode = ? AND ForecastID = ?
                """,
                params=(slpcode, cardcode, forecast_id),
                db_path=db_path,
            )
            if df_forecast.empty:
                print(f"🔕 Sin detalle forecast para {cardcode} con ForecastID={forecast_id}")
                continue

            fecha_snapshot = date.today().isoformat()

            # ¿Ya existe snapshot este mes para el cliente?
            if snapshot_existente(cardcode, fecha_snapshot, db_path):
                print(f"🟡 Ya existe snapshot este mes para {cardcode}. Se omite.")
                continue

            print(f"🧾 SlpCode={slpcode} | CardCode={cardcode} | ForecastID={forecast_id}")

            _execute_write(
                """
                INSERT INTO Snapshot_Forecast
                       (SlpCode, CardCode, ForecastID, Fecha_Snapshot, Observacion)
                VALUES (?, ?, ?, ?, '')
                """,
                params=(slpcode, cardcode, forecast_id, fecha_snapshot),
                db_path=db_path,
            )

            snapshot_id = run_query(
                "SELECT MAX(SnapshotID) AS id "
                "FROM   Snapshot_Forecast "
                "WHERE  SlpCode = ? AND CardCode = ?",
                params=(slpcode, cardcode),
                db_path=db_path,
            ).iloc[0]["id"]

            df_forecast["SnapshotID"] = int(snapshot_id)
            df_forecast["Cant_Real"]  = 0
            df_forecast["Delta"]      = 0
            df_forecast["Delta_Porc"] = 0

            cols = [
                "SnapshotID", "ItemCode", "TipoForecast", "OcrCode3", "FechEntr",
                "Linea", "Cant_Esperada", "PrecioUN", "DocCur",
                "Cant_Real", "Delta", "Delta_Porc",
            ]
            inserts_detalle.extend(df_forecast[cols].values.tolist())

        inserts_detalle = filtrar_nuevos_detalles(inserts_detalle, db_path)

        if inserts_detalle:
            print(f"🧩 Insertando {len(inserts_detalle)} registros en Snapshot_Detalle…")
            _execute_write(
                """
                INSERT INTO Snapshot_Detalle (
                    SnapshotID, ItemCode, TipoForecast, OcrCode3, FechEntr,
                    Linea, Cant_Esperada, PrecioUN, DocCur,
                    Cant_Real, Delta, Delta_Porc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                inserts_detalle,
                many=True,
                db_path=db_path,
            )
            print("✅ Snapshot_Detalle insertado correctamente.")
        else:
            print("⚠️ No hubo datos para insertar en Snapshot_Detalle.")

        print("✅ Snapshot generado (autocommit).")

    except Exception:
        # no hay transacción que deshacer, simplemente propagamos
        raise




def forecast_cero_existente(cardcode: str, itemcode: str, fechentr: str,
                            db_path: str = DB_PATH) -> bool:
    """
    Verifica si ya existe una línea ForecastCero para ese cliente/ítem/mes de la venta.
    Usa s.FechEntr como base, no f.Fecha_Snapshot (más preciso).
    """
    query = """
        SELECT 1
        FROM Snapshot_Detalle s
        JOIN Snapshot_Forecast f ON f.SnapshotID = s.SnapshotID
        WHERE f.CardCode = ?
          AND s.ItemCode = ?
          AND strftime('%Y-%m', s.FechEntr) = strftime('%Y-%m', ?)
          AND s.Observacion = 'ForecastCero'
        LIMIT 1
    """
    return not run_query(query, params=(cardcode, itemcode, fechentr), db_path=db_path).empty




def build_clave_set(snapshot_ids: list[int], db_path: str = DB_PATH) -> set[tuple]:
    """
    Devuelve un set (SnapshotID, ItemCode, FechEntr[str]) para los SnapshotID indicados.
    • Normaliza FechEntr a str para comparación con las tuplas candidatas.
    • Parte la consulta en trozos ≤ 900 IDs → evita el límite 999 placeholders de SQLite.
    """
    if not snapshot_ids:
        return set()

    claves: set[tuple] = set()
    CHUNK = 900  # límite seguro

    for i in range(0, len(snapshot_ids), CHUNK):
        chunk = snapshot_ids[i : i + CHUNK]
        placeholders = ",".join("?" * len(chunk))
        df = run_query(
            f"""
              SELECT SnapshotID, ItemCode, FechEntr
              FROM   Snapshot_Detalle
              WHERE  SnapshotID IN ({placeholders})
            """,
            params=chunk,
            db_path=db_path,
        )
        # normalizar fecha → str
        claves |= {
            (sid, itm, str(fech) if fech is not None else "NULL")
            for sid, itm, fech in df.itertuples(index=False, name=None)
        }


    return claves



FECH_IDX = 4  # posición de FechEntr dentro de cada tupla de inserción

def filtrar_nuevos_detalles(candidatos: list[tuple], db_path: str = DB_PATH) -> list[tuple]:
    """
    Filtra 'candidatos' dejando sólo los que NO existen ya en Snapshot_Detalle.
    Mucho más rápido: hace 1 SELECT masivo en lugar de 1 por fila.
    Espera tuplas con (SnapshotID, ItemCode, ..., FechEntr, …) (mismo orden usado en inserciones).
    """
    if not candidatos:
        return []

    # 1️⃣ colectar SnapshotID únicos
    ids = list({t[0] for t in candidatos})
    # --- configuración deduplicación ---
    claves_existentes = build_clave_set(ids, db_path)

    # 2️⃣ filtrar en memoria
    nuevos = [
        t for t in candidatos
        if (t[0], t[1], t[FECH_IDX]) not in claves_existentes   # SnapshotID, ItemCode, FechEntr
    ]
    return nuevos




def obtener_o_insertar_snapshot(
    slpcode: int,
    cardcode: str,
    forecast_id: int | None,
    observacion: str,
    fechentr: str | None = None,
    db_path: str = DB_PATH,
    _depth: int = 0,          # ← nuevo parámetro de control
) -> int:
    """
    Devuelve el SnapshotID existente para el cliente en el mes lógico
    (mes de la venta si se pasa `fechentr`, o el mes actual).  
    Si no existe, lo crea – con protección anti-recursión.
    """
    if _depth > 1:
        raise RuntimeError(
            f"Loop al insertar Snapshot_Forecast para {cardcode} {fechentr or 'hoy'}"
        )

    fecha_base = (fechentr or date.today().isoformat())[:7]   # 'YYYY-MM'

    df = run_query(
        """
        SELECT SnapshotID
        FROM   Snapshot_Forecast
        WHERE  CardCode = ?
          AND  strftime('%Y-%m', Fecha_Snapshot) = ?
        LIMIT 1
        """,
        params=(cardcode, fecha_base),
        db_path=db_path,
    )
    if not df.empty:
        return int(df.iloc[0]["SnapshotID"])

    # crear cabecera (día 1 del mes lógico)
    fecha_snap = f"{fecha_base}-01"
    _execute_write(
        """
        INSERT INTO Snapshot_Forecast
              (SlpCode, CardCode, ForecastID, Fecha_Snapshot, Observacion)
        VALUES (?,      ?,        ?,          ?,              ?)
        """,
        params=(slpcode, cardcode, forecast_id, fecha_snap, observacion),
        db_path=db_path,
    )

    # llamada recursiva protegida
    return obtener_o_insertar_snapshot(
        slpcode,
        cardcode,
        forecast_id,
        observacion,
        fechentr,
        db_path,
        _depth=_depth + 1,
    )


def incluir_forecast_cero(db_path: str = DB_PATH) -> None:
    print("🔍 Buscando facturas sin Forecast asignado…")

    # ――― SELECT que detecta líneas de venta sin snapshot/forecast ―――
    query = """
        SELECT o.SlpCode,
               o.CardCode,
               i.ItemCode,
               i.OcrCode3,
               i.DocDate  AS FechEntr,
               i.LineNum  AS Linea,
               i.Price    AS PrecioUN,
               i.Currency AS DocCur,
               SUM(i.Quantity) AS CantReal
        FROM INV1 i
        JOIN OINV o ON i.DocEntry = o.DocEntry
        LEFT JOIN Snapshot_Forecast f
               ON f.CardCode = o.CardCode
              AND strftime('%Y-%m', f.Fecha_Snapshot) = strftime('%Y-%m', i.DocDate)
        LEFT JOIN Snapshot_Detalle s
               ON s.SnapshotID = f.SnapshotID
              AND s.ItemCode   = i.ItemCode
              AND s.FechEntr   = i.DocDate
              AND s.Observacion IS NULL
        WHERE s.ItemCode IS NULL
        GROUP BY o.SlpCode, o.CardCode, i.ItemCode, i.OcrCode3,
                 i.DocDate, i.LineNum, i.Price, i.Currency
    """

    df_faltantes = run_query(query, db_path=db_path)
    if df_faltantes.empty:
        print("✅ No se detectaron facturas sin forecast.")
        return

    print(f"📎 Intentando insertar {len(df_faltantes)} líneas 'ForecastCero'…")
    inserts_detalle: list[list] = []

    for _, row in df_faltantes.iterrows():
        slpcode  = int(row["SlpCode"])
        cardcode = row["CardCode"]
        itemcode = row["ItemCode"]

        # ya existe un ForecastCero para ese cliente-ítem-mes ⇒ omitir
        if forecast_cero_existente(cardcode, itemcode, row["FechEntr"], db_path):
            print(f"🔁 {cardcode}/{itemcode} ya registrado este mes → omitido.")
            continue

        snapshot_id = obtener_o_insertar_snapshot(
            slpcode=slpcode,
            cardcode=cardcode,
            forecast_id=obtener_forecastid_cliente(cardcode, db_path),
            observacion="ForecastCero",
            fechentr=row["FechEntr"],
            db_path=db_path,
        )

        inserts_detalle.append((
            snapshot_id,
            itemcode,
            "Real",
            row["OcrCode3"],
            row["FechEntr"],
            str(row["Linea"]),
            0,                      # Cant_Esperada
            row["PrecioUN"],
            row["DocCur"],
            row["CantReal"],
            row["CantReal"],        # Delta
            0,                      # Delta_Porc
            "ForecastCero",
        ))

    inserts_detalle = filtrar_nuevos_detalles(inserts_detalle, db_path)

    if not inserts_detalle:
        print("🛈 No se insertó ninguna nueva línea ForecastCero.")
        return

    _execute_write(
        """
        INSERT INTO Snapshot_Detalle (
            SnapshotID, ItemCode, TipoForecast, OcrCode3, FechEntr,
            Linea, Cant_Esperada, PrecioUN, DocCur,
            Cant_Real, Delta, Delta_Porc, Observacion
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        inserts_detalle,
        many=True,
        db_path=db_path,
    )
    print(f"✅ Insertadas {len(inserts_detalle)} líneas ForecastCero nuevas.")




def verificar_forecast_cero(db_path: str = DB_PATH) -> None:
    query = """
        SELECT COUNT(*) AS TotalForecastCero
        FROM Snapshot_Detalle
        WHERE Observacion = 'ForecastCero'
    """
    df = run_query(query, db_path=db_path)
    total = df.iloc[0]["TotalForecastCero"]
    print(f"📊 Total registros ForecastCero detectados: {total}")


def exportar_clientes_sin_forecast(
    db_path: str = DB_PATH,
    export_csv: bool = True,
    csv_path: str = "clientes_sin_forecast.csv"
) -> pd.DataFrame:
    """
    Detecta clientes con ventas en OINV pero sin forecast asignado en Forecast_Detalle.
    Exporta opcionalmente a CSV.
    """
    query = """
        SELECT DISTINCT o.CardCode, o.CardName, o.SlpCode
        FROM OINV o
        LEFT JOIN Forecast_Detalle f ON o.CardCode = f.CardCode
        WHERE f.CardCode IS NULL
        ORDER BY o.CardCode
    """

    df = run_query(query, db_path=db_path)

    if df.empty:
        print("✅ No hay clientes con ventas sin forecast.")
    else:
        print(f"🔍 Se detectaron {len(df)} clientes con ventas sin forecast.")

        if export_csv:
            df.to_csv(csv_path, index=False)
            print(f"📤 Exportado a {csv_path}")

    return df


def obtener_clientes_sin_forecast(db_path: str = DB_PATH) -> pd.DataFrame:
    """
    Retorna clientes con ventas (OINV) pero sin forecast asignado.
    """
    query = """
        SELECT DISTINCT o.CardCode, o.CardName, o.SlpCode
        FROM OINV o
        LEFT JOIN Forecast_Detalle f ON o.CardCode = f.CardCode
        WHERE f.CardCode IS NULL
        ORDER BY o.CardCode
    """
    return run_query(query, db_path=db_path)


def generar_snapshot_completo(db_path: str = DB_PATH) -> pd.DataFrame:
    """
    Ejecuta snapshot y retorna clientes sin forecast para visualización.
    """
    print("🚀 Ejecutando snapshot completo...")
    ejecutar_snapshot_forecast(db_path)
    incluir_forecast_cero(db_path)
    df_no_forecast = obtener_clientes_sin_forecast(db_path)
    print("✅ Proceso completado.")
    return df_no_forecast


def snapshot_existente(cardcode: str, fecha: str, db_path: str = DB_PATH) -> bool:
    """
    Verifica si ya existe un Snapshot_Forecast para el cliente en el mismo mes.
    """
    query = """
        SELECT 1
        FROM Snapshot_Forecast
        WHERE CardCode = ?
          AND strftime('%Y-%m', Fecha_Snapshot) = strftime('%Y-%m', ?)
        LIMIT 1
    """
    df = run_query(query, params=(cardcode, fecha), db_path=db_path)
    return not df.empty








if __name__ == "__main__":
    ejecutar_snapshot_forecast()
    incluir_forecast_cero()
    actualizar_snapshot_realidad()
    verificar_forecast_cero()
    exportar_forecast_cero_log()
