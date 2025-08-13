# B_TRF001: Importaciones principales para transformación y formato de forecast
# ∂B_TRF001/∂B1
import pandas as pd
import re


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _ocr3_a_linea(ocr: str) -> str:
    """
    Mapea el valor de OcrCode3 al concepto de 'Linea'.

    Reglas actuales:
        - 'Pta-' ⭢ 'Planta'
        - 'Trd-' ⭢ 'Trader'
        - Cualquier otro prefijo o valor nulo ⭢ 'Desconocido'
    """
    if not ocr:  # None, NaN o string vacío
        return "Desconocido"
    if re.match(r"(?i)^pta[-_]", ocr):
        return "Planta"
    if re.match(r"(?i)^trd[-_]", ocr):
        return "Trader"
    return "Desconocido"


# ────────────────────────────────────────────────────────────────────────────────
# B_TRF002: Conversión de DataFrame métrico de forecast a formato largo SCANNER
# ∂B_TRF002/∂B1
def df_forecast_metrico_to_largo(
    df: pd.DataFrame,
    anio: int,
    cardcode: str,
    slpcode: int,
    debug: bool = False,
) -> pd.DataFrame:
    """
    Convierte forecast “métrico” (columnas 01–12) a formato largo sin duplicados.

    Reglas:
      - Requiere: ["ItemCode","TipoForecast","OcrCode3","DocCur","Métrica"].
      - Métrica ∈ {"Cantidad","Precio"}.
      - Columnas "01".."12" faltantes → 0.
      - Cant = suma por clave; PrecioUN = último no-cero (si no hay, último valor).
      - FechEntr = primer día de cada mes de `anio` (date).
    """
    import pandas as pd

    _dbg = print if debug else (lambda *a, **k: None)
    _dbg(
        f"[DEBUG-LARGO] ▶ Transformando forecast largo: card={cardcode}, año={anio}, slp={slpcode}"
    )

    columnas_mes = [f"{m:02d}" for m in range(1, 13)]
    columnas_base = ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Métrica"]

    df = df.copy()
    df.columns = df.columns.astype(str)

    # Validaciones base
    faltantes = [c for c in columnas_base if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas necesarias: {faltantes}")

    # Métricas válidas
    valid_metricas = {"Cantidad", "Precio"}
    metricas_distintas = set(df["Métrica"].dropna().unique().tolist())
    no_validas = metricas_distintas - valid_metricas
    if no_validas:
        raise ValueError(
            f"Métrica(s) no válidas: {sorted(no_validas)}. Esperadas: {sorted(valid_metricas)}"
        )

    # Garantizar columnas de mes y tipificarlas a numérico; NaN→0
    for col in columnas_mes:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    _dbg(f"[DEBUG-LARGO] Columnas disponibles: {df.columns.tolist()}")
    _dbg(f"[DEBUG-LARGO] Filas iniciales antes de deduplicar: {len(df)}")

    # Deduplicación previa (conservar última por clave lógica)
    df = df.sort_index().drop_duplicates(
        subset=["ItemCode", "TipoForecast", "OcrCode3", "Métrica"], keep="last"
    )
    _dbg(f"[DEBUG-LARGO] Filas después de deduplicación previa: {len(df)}")

    # Split por métrica
    df_cant = df[df["Métrica"] == "Cantidad"].copy()
    df_prec = df[df["Métrica"] == "Precio"].copy()

    # Melt (Cant)
    df_cant_largo = df_cant.melt(
        id_vars=["ItemCode", "TipoForecast", "OcrCode3", "DocCur"],
        value_vars=columnas_mes,
        var_name="Mes",
        value_name="Cant",
    )
    # Melt (Precio)
    df_prec_largo = df_prec.melt(
        id_vars=["ItemCode", "TipoForecast", "OcrCode3", "DocCur"],
        value_vars=columnas_mes,
        var_name="Mes",
        value_name="PrecioUN",
    )

    # Merge y saneo
    df_largo = (
        pd.merge(
            df_cant_largo,
            df_prec_largo,
            on=["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Mes"],
            how="outer",
        )
        .fillna({"Cant": 0, "PrecioUN": 0})
        .reset_index(drop=True)
    )

    # Consolidación sin duplicados:
    # - Cant: suma
    # - PrecioUN: último no-cero; si todos 0/NaN, último (0 si vacío)
    def _agg_precio(series: pd.Series) -> float:
        s = series.dropna()
        nz = s[s != 0]
        return (
            float(nz.iloc[-1])
            if not nz.empty
            else (float(s.iloc[-1]) if not s.empty else 0.0)
        )

    claves = ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "Mes"]
    df_largo = df_largo.groupby(claves, as_index=False).agg(
        Cant=("Cant", "sum"), PrecioUN=("PrecioUN", _agg_precio)
    )

    # Tipos finales y atributos calculados
    df_largo["Linea"] = df_largo["OcrCode3"].apply(_ocr3_a_linea)

    df_largo["Mes"] = df_largo["Mes"].astype(str).str.zfill(2)
    df_largo["FechEntr"] = pd.to_datetime(
        f"{int(anio)}-" + df_largo["Mes"] + "-01",
        format="%Y-%m-%d",
        errors="coerce",
    ).dt.date

    df_largo["CardCode"] = cardcode
    df_largo["SlpCode"] = slpcode

    # Normaliza tipos numéricos
    df_largo["Cant"] = pd.to_numeric(df_largo["Cant"], errors="coerce").fillna(0.0)
    df_largo["PrecioUN"] = pd.to_numeric(df_largo["PrecioUN"], errors="coerce").fillna(
        0.0
    )

    # Reglas de negocio simples: negativos no permitidos (puedes relajar si hace falta)
    neg = (df_largo["Cant"] < 0) | (df_largo["PrecioUN"] < 0)
    if neg.any():
        raise ValueError(
            f"[LARGO] Valores negativos detectados en {int(neg.sum())} filas."
        )

    columnas_finales = [
        "ItemCode",
        "TipoForecast",
        "OcrCode3",
        "Linea",
        "DocCur",
        "Mes",
        "FechEntr",
        "Cant",
        "PrecioUN",
        "CardCode",
        "SlpCode",
    ]

    _dbg("[DEBUG-LARGO] Preview final:")
    _dbg(df_largo[columnas_finales].head(5).to_string(index=False))

    # Validación clave única BD
    claves_bd = ["ItemCode", "TipoForecast", "OcrCode3", "Mes", "CardCode"]
    duplicados = df_largo.duplicated(subset=claves_bd, keep=False)
    if duplicados.any():
        _dbg(f"[❌ LARGO-ERROR] {duplicados.sum()} duplicados para clave BD:")
        _dbg(
            df_largo[duplicados][claves_bd + ["Cant", "PrecioUN"]]
            .sort_values(claves_bd)
            .to_string(index=False)
        )
        raise ValueError("Duplicados en df_largo respecto a clave única de detalle.")

    return df_largo[columnas_finales]
