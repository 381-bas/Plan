# B_SYN001: Importaciones principales y utilidades para sincronización y guardado multicliente
# # ∂B_SYN001/∂B0
import pandas as pd
from typing import Optional

from utils.db import run_query


def seleccionar_forecast_base(
    slpcode: int, cardcode: str, anio: int, db_path: str
) -> Optional[int]:
    """
    Retorna el ForecastID base más adecuado para enriquecer el forecast actual.

    Orden de prioridad:
    1. Último ForecastID individual del cliente y vendedor para ese año.
    2. Último ForecastID global (sin segmentación por cliente) con datos del cliente.
    3. None si no se encuentra referencia.
    """

    # 1. Buscar ForecastID individual
    sql_indiv = """
        SELECT MAX(ForecastID) as id
        FROM Forecast
        WHERE SlpCode = ?
        AND EXISTS (
            SELECT 1 FROM Forecast_Detalle
            WHERE Forecast_Detalle.ForecastID = Forecast.ForecastID
              AND CardCode = ?
        )
        AND strftime('%Y', date(Fecha_Carga, 'unixepoch')) = ?
    """
    df_indiv = run_query(
        sql_indiv, params=(slpcode, cardcode, str(anio)), db_path=db_path
    )
    if not df_indiv.empty and pd.notna(df_indiv.at[0, "id"]):
        return int(df_indiv.at[0, "id"])

    # 2. Buscar ForecastID global que contenga al cliente
    sql_global = """
        SELECT MAX(ForecastID) as id
        FROM Forecast_Detalle
        WHERE CardCode = ?
    """
    df_global = run_query(sql_global, params=(cardcode,), db_path=db_path)
    if not df_global.empty and pd.notna(df_global.at[0, "id"]):
        return int(df_global.at[0, "id"])

    # 3. No se encontró referencia previa
    return None
