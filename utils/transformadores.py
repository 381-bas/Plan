# B_TRF001: Importaciones principales para transformaciÃ³n y formato de forecast
# âˆ‚B_TRF001/âˆ‚B1
import pandas as pd
import re


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Helpers
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _ocr3_a_linea(ocr: str) -> str:
    """
    Mapea el valor de OcrCode3 al concepto de 'Linea'.

    Reglas actuales:
        - 'Pta-' â­¢ 'Planta'
        - 'Trd-' â­¢ 'Trader'
        - Cualquier otro prefijo o valor nulo â­¢ 'Desconocido'
    """
    if not ocr:  # None, NaN o string vacÃ­o
        return "Desconocido"
    if re.match(r"(?i)^pta[-_]", ocr):
        return "Planta"
    if re.match(r"(?i)^trd[-_]", ocr):
        return "Trader"
    return "Desconocido"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# B_TRF002: ConversiÃ³n de DataFrame mÃ©trico de forecast a formato largo SCANNER
# âˆ‚B_TRF002/âˆ‚B1
def df_forecast_metrico_to_largo(
    df: pd.DataFrame,
    anio: int,
    cardcode: str,
    slpcode: int,
    debug: bool = False,
) -> pd.DataFrame:
    """
    Convierte forecast â€œmÃ©tricoâ€ (columnas 01â€“12) a formato largo sin duplicados.

    Reglas:
      - Requiere: ["ItemCode","TipoForecast","OcrCode3","DocCur","MÃ©trica"].
      - MÃ©trica âˆˆ {"Cantidad","Precio"}.
      - Columnas "01".."12" faltantes â†’ 0.
      - Cant = suma por clave; PrecioUN = Ãºltimo no-cero (si no hay, Ãºltimo valor).
      - FechEntr = primer dÃ­a de cada mes de `anio` (date).
    """
    import pandas as pd

    _dbg = print if debug else (lambda *a, **k: None)
    _dbg(
        f"[DEBUG-LARGO] â–¶ Transformando forecast largo: card={cardcode}, aÃ±o={anio}, slp={slpcode}"
    )

    columnas_mes = [f"{m:02d}" for m in range(1, 13)]
    columnas_base = ["ItemCode", "TipoForecast", "OcrCode3", "DocCur", "MÃ©trica"]

    df = df.copy()
    df.columns = df.columns.astype(str)

    # Validaciones base
    faltantes = [c for c in columnas_base if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas necesarias: {faltantes}")

    # MÃ©tricas vÃ¡lidas
    valid_metricas = {"Cantidad", "Precio"}
    metricas_distintas = set(df["MÃ©trica"].dropna().unique().tolist())
    no_validas = metricas_distintas - valid_metricas
    if no_validas:
        raise ValueError(
            f"MÃ©trica(s) no vÃ¡lidas: {sorted(no_validas)}. Esperadas: {sorted(valid_metricas)}"
        )

    # Garantizar columnas de mes y tipificarlas a numÃ©rico; NaNâ†’0
    for col in columnas_mes:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    _dbg(f"[DEBUG-LARGO] Columnas disponibles: {df.columns.tolist()}")
    _dbg(f"[DEBUG-LARGO] Filas iniciales antes de deduplicar: {len(df)}")

    # DeduplicaciÃ³n previa (conservar Ãºltima por clave lÃ³gica)
    df = df.sort_index().drop_duplicates(
        subset=["ItemCode", "TipoForecast", "OcrCode3", "MÃ©trica"], keep="last"
    )
    _dbg(f"[DEBUG-LARGO] Filas despuÃ©s de deduplicaciÃ³n previa: {len(df)}")

    # Split por mÃ©trica
    df_cant = df[df["MÃ©trica"] == "Cantidad"].copy()
    df_prec = df[df["MÃ©trica"] == "Precio"].copy()

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

    # ConsolidaciÃ³n sin duplicados:
    # - Cant: suma
    # - PrecioUN: Ãºltimo no-cero; si todos 0/NaN, Ãºltimo (0 si vacÃ­o)
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

    # Normaliza tipos numÃ©ricos
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

    # ValidaciÃ³n clave Ãºnica BD
    claves_bd = ["ItemCode", "TipoForecast", "OcrCode3", "Mes", "CardCode"]
    duplicados = df_largo.duplicated(subset=claves_bd, keep=False)
    if duplicados.any():
        _dbg(f"[âŒ LARGO-ERROR] {duplicados.sum()} duplicados para clave BD:")
        _dbg(
            df_largo[duplicados][claves_bd + ["Cant", "PrecioUN"]]
            .sort_values(claves_bd)
            .to_string(index=False)
        )
        raise ValueError("Duplicados en df_largo respecto a clave Ãºnica de detalle.")

    return df_largo[columnas_finales]
