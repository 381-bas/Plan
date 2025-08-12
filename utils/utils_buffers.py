# B_SYN001: Importaciones principales y utilidades para sincronización y guardado multicliente
# # ∂B_SYN001/∂B0
import pandas as pd
import streamlit as st
from typing import Optional

from utils.repositorio_forecast.repositorio_forecast_editor import (
    obtener_buffer_cliente,                      
    actualizar_buffer_global,                      
    sincronizar_buffer_local                        
)
from utils.transformadores import df_forecast_metrico_to_largo
from utils.db import (
    run_query,
    DB_PATH
)  
from services.forecast_engine import (
    insertar_forecast_detalle,
    registrar_log_detalle_cambios,
    obtener_forecast_activo,
    logger
)
from services.sync import guardar_temp_local             # ∂B49
from config.contexto import obtener_slpcode

# B_SYN002: Sincronización y guardado individual de buffer editado para cliente
# # ∂B_SYN002/∂B0
def sincronizar_para_guardado_final(
    key_buffer: str,
    df_editado: pd.DataFrame
) -> pd.DataFrame:
    """
    Sincroniza los datos editados con el buffer en sesión y,
    si hubo modificaciones reales en columnas de mes, deja el
    DataFrame listo para la inserción en BD.

    Devuelve siempre un DataFrame (aunque no haya cambios),
    pero solo marca al cliente como editado cuando hay delta.
    """
    print(f"[DEBUG-SYNC] Iniciando sincronización para {key_buffer}")
    print(f"[DEBUG-SYNC] Tamaño DF editado recibido: {df_editado.shape}")

    # 🔀 1) Unificación de métricas Cantidad + Precio
    df_editado_unificado = pd.concat(
        [
            df_editado[df_editado["Métrica"] == "Cantidad"],
            df_editado[df_editado["Métrica"] == "Precio"],
        ],
        ignore_index=True,
    )
    print(f"[DEBUG-SYNC] Total filas tras unificación: {len(df_editado_unificado)}")
    print(f"[DEBUG-SYNC] Unificados: {df_editado_unificado['Métrica'].value_counts().to_dict()}")

    # 🔄 2) Recuperar buffer actual
    df_base_actual = obtener_buffer_cliente(key_buffer).reset_index()
    print(f"[DEBUG-SYNC] Buffer base recuperado: {df_base_actual.shape}")

    # 🔄 3) Sincronizar (ahora devuelve tupla)
    df_sync, hay_cambios = sincronizar_buffer_local(
        df_base_actual, df_editado_unificado
    )

    if not hay_cambios:
        print("[DEBUG-SYNC] 🟡 Sin cambios reales -> se omite guardado final.")
        return df_base_actual          # ⬅️  nada más que hacer

    # ---------------------------------------------------------------------
    # 🔽 Solo se ejecuta esta parte si hay_cambios == True
    # ---------------------------------------------------------------------
    print("[DEBUG-SYNC] Columnas luego de sincronizar:", df_sync.columns.tolist())
    print("[DEBUG-SYNC] Index cardinalidad post-sync:")
    print(df_sync[["ItemCode", "TipoForecast", "Métrica"]].nunique())

    for col in ["ItemCode", "TipoForecast", "Métrica"]:
        if df_sync[col].isna().any():
            print(f"[⚠️ SYNC] Valores nulos detectados en columna clave: {col}")

    # 👉 Guardar en session_state ordenado por índice compuesto
    df_sync = df_sync.set_index(["ItemCode", "TipoForecast", "Métrica"])
    df_sync = df_sync.sort_index()
    st.session_state[key_buffer] = df_sync

    df_guardar = df_sync.reset_index()
    print(f"[DEBUG-SYNC] Buffer final preparado para guardado. Filas: {len(df_guardar)}")
    print(f"[DEBUG-SYNC] Columnas finales:", df_guardar.columns.tolist())

    guardar_temp_local(key_buffer, df_guardar)
    actualizar_buffer_global(df_guardar, key_buffer)

    # ✅ Marcar cliente como editado
    cliente = key_buffer.replace("forecast_buffer_cliente_", "")
    editados = st.session_state.get("clientes_editados", set())
    editados.add(cliente)
    st.session_state["clientes_editados"] = editados
    print(f"[DEBUG-SYNC] Cliente marcado como editado: {cliente}")

    return df_guardar










# ---------------------------------------------------------------------------
# Helper: obtener el último ForecastID vigente para un cliente/año/vendedor
# ---------------------------------------------------------------------------
def _get_last_id(slpcode: int, cardcode: str, anio: int, db_path: str) -> Optional[int]:
    """
    Devuelve el MAX(ForecastID) **anterior** al que se está creando.
    Si no existe uno individual por cliente-vendedor, intenta recuperar uno base global
    donde el cliente haya participado (cualquier vendedor).
    """
    # 🟢 Intento 1: Forecast individual cliente + vendedor
    qry_individual = """
        SELECT MAX(ForecastID) AS id              
        FROM   Forecast_Detalle
        WHERE  SlpCode  = ?
          AND  CardCode = ?
          AND  strftime('%Y', FechEntr) = ?;
    """
    df = run_query(qry_individual, params=(slpcode, cardcode, str(anio)), db_path=db_path)

    if not df.empty and pd.notna(df.iloc[0].id):
        forecast_id = int(df.iloc[0].id)
        print(f"[DEBUG-ID] ForecastID individual encontrado: {forecast_id}")
        return forecast_id

    # 🟡 Intento 2: Forecast base global por cliente (sin importar SlpCode)
    qry_global = """
        SELECT MAX(ForecastID) AS id              
        FROM   Forecast_Detalle
        WHERE  CardCode = ?
          AND  strftime('%Y', FechEntr) = ?;
    """
    df_global = run_query(qry_global, params=(cardcode, str(anio)), db_path=db_path)

    if not df_global.empty and pd.notna(df_global.iloc[0].id):
        forecast_id = int(df_global.iloc[0].id)
        print(f"[🟡 DEBUG-ID] Fallback a ForecastID base global: {forecast_id}")
        return forecast_id

    print("[⚠️ DEBUG-ID] Sin ForecastID previo (ni individual ni global)")
    return None





# ---------------------------------------------------------------------------
# Helper: enriquecer DF_LARGO con Cant_Anterior y filtrar cambios reales
# ---------------------------------------------------------------------------

def _enriquecer_y_filtrar(
    df_largo: pd.DataFrame,
    forecast_id_prev: Optional[int],
    slpcode: int,
    cardcode: str,
    anio: int,
    db_path: str,
    resolver_duplicados: str = "mean",  # opciones: "mean", "sum", "error"
    incluir_deltas_cero_si_es_individual: bool = False,
) -> pd.DataFrame:
    """Añade columna ``Cant_Anterior`` y filtra filas donde la cantidad cambió."""

    print(f"[DEBUG-FILTRO] ▶ Enriqueciendo forecast cliente {cardcode} con histórico ForecastID={forecast_id_prev}")

    if forecast_id_prev is None:
        print("[DEBUG-FILTRO] 🆕 Cliente sin historial previo. Se parte desde Cant_Anterior = 0")
        df_prev = df_largo[["ItemCode", "TipoForecast", "OcrCode3", "Mes"]].copy()
        df_prev["Cant_Anterior"] = 0
    else:
        print(f"[DEBUG-FILTRO] 🔁 Cliente con historial previo. ForecastID utilizado: {forecast_id_prev}")
        qry_prev = """
            SELECT ItemCode, TipoForecast, OcrCode3,
                   CAST(strftime('%m', FechEntr) AS TEXT) AS Mes,
                   Cant AS Cant_Anterior
            FROM   Forecast_Detalle
            WHERE  ForecastID = ?
        """
        df_prev = run_query(qry_prev, params=(forecast_id_prev,), db_path=db_path)
        df_prev["Mes"] = df_prev["Mes"].astype(str).str.zfill(2)
        print(f"[DEBUG-FILTRO] Registros históricos recuperados: {len(df_prev)}")

        claves = ["ItemCode", "TipoForecast", "OcrCode3", "Mes"]
        duplicados = df_prev.duplicated(subset=claves, keep=False)
        if duplicados.any():
            print(f"[⚠️ DEBUG-FILTRO] {duplicados.sum()} duplicados detectados en histórico por clave compuesta.")
            if resolver_duplicados == "error":
                raise ValueError("Duplicados en histórico de Forecast_Detalle y resolver_duplicados='error'")
            elif resolver_duplicados in {"mean", "sum"}:
                print(f"[DEBUG-FILTRO] Resolviendo con agregación '{resolver_duplicados}' sobre Cant_Anterior")
                df_prev = df_prev.groupby(claves, as_index=False).agg({"Cant_Anterior": resolver_duplicados})
            else:
                raise ValueError(f"Valor no válido en resolver_duplicados: {resolver_duplicados}")

    claves = ["ItemCode", "TipoForecast", "OcrCode3", "Mes"]
    df_enr = df_largo.merge(df_prev, on=claves, how="left")
    df_enr["Cant_Anterior"] = df_enr["Cant_Anterior"].fillna(0)

    faltantes = df_enr[df_enr["Cant_Anterior"].isna()]
    if not faltantes.empty:
        print(f"[⚠️ DEBUG-FILTRO] {len(faltantes)} registros no encontraron Cant_Anterior. ¿Faltan claves en histórico?")

    print("[DEBUG-FILTRO] ▶ Diagnóstico completo previo al filtro de cambios:")
    df_enr["Delta"] = df_enr["Cant"] - df_enr["Cant_Anterior"]
    print(df_enr[["ItemCode", "TipoForecast", "OcrCode3", "Mes", "Cant_Anterior", "Cant", "Delta"]]
          .sort_values(["TipoForecast", "Mes"])
          .to_string(index=False))

    df_sin_delta = df_enr[df_enr["Delta"] == 0].copy()
    if not df_sin_delta.empty:
        print(f"[DEBUG-FILTRO] 🟡 Registros sin cambios reales (Δ = 0): {len(df_sin_delta)}")
        print(df_sin_delta[["ItemCode", "TipoForecast", "Mes", "Cant"]].to_string(index=False))
    else:
        print("[DEBUG-FILTRO] ✅ Todos los registros tenían algún cambio.")

    # ✅ Filtrar según configuración
    if incluir_deltas_cero_si_es_individual:
        df_out = df_enr[df_enr["Cant"] > 0].copy()
        print("[DEBUG-FILTRO] 🚩 Se incluye todo registro con cantidad > 0 por transición individual")
    else:
        df_out = df_enr[df_enr["Delta"] != 0].copy()

    print(f"[DEBUG-FILTRO] Registros con cambio real detectado: {len(df_out)}")
    if not df_out.empty:
        print("[DEBUG-FILTRO] Preview de cambios:")
        print(df_out[["ItemCode", "TipoForecast", "Mes", "Cant_Anterior", "Cant"]].head(5).to_string(index=False))

        delta_total = df_out["Delta"].sum()
        print(f"[DEBUG-FILTRO] Variación total de unidades: {delta_total:.2f}")

        resumen_mes = df_out.groupby("Mes")["Delta"].sum().reset_index()
        print("[DEBUG-FILTRO] Resumen de delta por mes:")
        print(resumen_mes.to_string(index=False))

    from utils.repositorio_forecast.forecast_writer import validate_delta_schema
    df_val = df_out.rename(columns={
        "Cant_Anterior": "CantidadAnterior",
        "Cant": "CantidadNueva"
    })
    validate_delta_schema(df_val, contexto="[VALIDACIÓN ENRIQUECER]")

    claves_bd = ["ItemCode", "TipoForecast", "OcrCode3", "Mes", "CardCode"]
    if "CardCode" in df_out.columns:
        duplicados_out = df_out.duplicated(subset=claves_bd, keep=False)
        if duplicados_out.any():
            print(f"[❌ FILTRO-ERROR] {duplicados_out.sum()} duplicados detectados post-enriquecimiento:")
            print(df_out[duplicados_out][claves_bd + ["Cant", "Cant_Anterior"]].sort_values(claves_bd).to_string(index=False))
            raise ValueError("df_out contiene claves duplicadas que violan la restricción única de Forecast_Detalle.")
    else:
        print("[⚠️ FILTRO] No se encontró columna 'CardCode' en df_out — se omitió validación de duplicados.")

    return df_out






# ---------------------------------------------------------------------------
# Helper: refrescar buffer UI después de guardar
# ---------------------------------------------------------------------------

def _refrescar_buffer_ui(forecast_id: int, key_buffer: str, db_path: str):
    """Reconstruye las dos métricas (Cantidad / Precio) y refresca sesión Streamlit."""

    print(f"[DEBUG-BUFFER] 🔁 Refrescando UI para ForecastID={forecast_id}, buffer={key_buffer}")

    qry = """
        SELECT ItemCode, TipoForecast, OcrCode3, Linea, DocCur,
               CAST(strftime('%m', FechEntr) AS TEXT) AS Mes,
               Cant, PrecioUN
        FROM   Forecast_Detalle
        WHERE  ForecastID = ?
    """
    df_post = run_query(qry, params=(forecast_id,), db_path=db_path)

    if df_post.empty:
        print(f"[DEBUG-BUFFER] ⚠️ No se encontraron registros en Forecast_Detalle para ID={forecast_id}")
        return

    print(f"[DEBUG-BUFFER] Registros recuperados: {len(df_post)}")

    cols_meses = [f"{m:02d}" for m in range(1, 13)]

    # --- Cantidad
    df_cant = (
        df_post.copy()
            .groupby(["ItemCode", "TipoForecast", "OcrCode3", "Linea", "DocCur", "Mes"], as_index=False)
            .agg({"Cant": "sum"})
    )
    df_cant["Métrica"] = "Cantidad"
    pivot_cant = df_cant.pivot_table(
        index=["ItemCode", "TipoForecast", "OcrCode3", "Linea", "DocCur", "Métrica"],
        columns="Mes", values="Cant", aggfunc="first"
    ).reindex(columns=cols_meses, fill_value=0)

    # --- Precio (ahora defensivo también)
    df_prec = (
        df_post.copy()
            .groupby(["ItemCode", "TipoForecast", "OcrCode3", "Linea", "DocCur", "Mes"], as_index=False)
            .agg({"PrecioUN": "mean"})
    )
    df_prec["Métrica"] = "Precio"
    pivot_prec = df_prec.pivot_table(
        index=["ItemCode", "TipoForecast", "OcrCode3", "Linea", "DocCur", "Métrica"],
        columns="Mes", values="PrecioUN", aggfunc="first"
    ).reindex(columns=cols_meses, fill_value=0)

    # --- Unión y limpieza
    df_metrico = pd.concat([pivot_cant, pivot_prec])
    df_metrico = df_metrico.reset_index()

    # Ordenar columnas: fijas + meses
    cols_fijas = ["ItemCode", "TipoForecast", "OcrCode3", "Linea", "DocCur", "Métrica"]
    df_metrico = df_metrico[cols_fijas + cols_meses]

    # Ordenar filas visualmente
    df_metrico = df_metrico.sort_values(by=["ItemCode", "TipoForecast", "Métrica"]).reset_index(drop=True)

    print(f"[DEBUG-BUFFER] Columnas finales: {df_metrico.columns.tolist()}")
    print(f"[DEBUG-BUFFER] Buffer final generado. Filas: {len(df_metrico)}")

    st.session_state[key_buffer] = df_metrico.set_index(["ItemCode", "TipoForecast", "Métrica"])
    # st.dataframe(df_metrico)
    logger.info("Buffer UI refrescado para %s", key_buffer)











# B_SYN003: Guardado estructurado y seguro de buffers editados de todos los clientes
# # ∂B_SYN003/∂B0
def guardar_todos_los_clientes_editados(anio: int, db_path: str = DB_PATH):
    clientes = st.session_state.get("clientes_editados", set()).copy()
    print(f"[DEBUG-GUARDADO] Clientes a procesar: {sorted(clientes)}")
    if not clientes:
        st.info("✅ No hay cambios pendientes por guardar")
        return

    for cliente in clientes:
        key_buffer = f"forecast_buffer_cliente_{cliente}"
        if key_buffer not in st.session_state:
            print(f"[WARN] Buffer no encontrado en sesión para {cliente}")
            st.warning(f"⚠️ No se encontró buffer para cliente {cliente}.")
            continue

        try:
            df_base = st.session_state[key_buffer].reset_index()
            slpcode = int(obtener_slpcode())

            print(f"\n[DEBUG-GUARDADO] CLIENTE {cliente}")
            print(f"[DEBUG-GUARDADO] Paso 1: DF_BASE (filas={len(df_base)}) columnas={df_base.columns.tolist()}")
            print(df_base.head(3).to_string(index=False))

            if df_base.empty:
                print(f"[DEBUG-GUARDADO] DF_BASE está vacío. Se omite cliente {cliente}")
                continue

            df_largo = df_forecast_metrico_to_largo(df_base, anio, cliente, slpcode)
            print(f"[DEBUG-GUARDADO] Paso 2: DF_LARGO generado (filas={len(df_largo)})")
            print(df_largo[["ItemCode", "TipoForecast", "Mes", "Cant"]].head(5).to_string(index=False))

            if df_largo.empty:
                print(f"[DEBUG-GUARDADO] 🟡 DF_LARGO vacío, se omite cliente {cliente}")
                st.info(f"ℹ️ Sin datos para guardar en cliente {cliente}.")
                continue

            forecast_id = obtener_forecast_activo(slpcode, cliente, anio, db_path, force_new=False)
            forecast_id_prev = _get_forecast_id_prev(slpcode, cliente, anio, db_path)
            print(f"[DEBUG-GUARDADO] Paso 3: ForecastID nuevo = {forecast_id}, anterior = {forecast_id_prev}")

            from utils.repositorio_forecast.forecast_writer import existe_forecast_individual
            modo_individual = existe_forecast_individual(slpcode, cliente, anio, db_path)

            df_largo_filtrado = _enriquecer_y_filtrar(
                df_largo, forecast_id_prev, slpcode, cliente, anio, db_path,
                incluir_deltas_cero_si_es_individual=modo_individual
            )
            print(f"[DEBUG-GUARDADO] Paso 4: Cambios reales detectados = {len(df_largo_filtrado)}")
            if df_largo_filtrado.empty:
                print(f"[DEBUG-GUARDADO] ⏩ Sin cambios reales en métricas. Cliente omitido: {cliente}")
                st.info(f"⏩ Cliente {cliente}: sin cambios reales. Se omite inserción.")
                continue

            print("[DEBUG-GUARDADO] Paso 5: Logging de diferencias previas a inserción")
            registrar_log_detalle_cambios(
                slpcode, cliente, anio,
                df_largo_filtrado.copy(),
                db_path,
                forecast_id=forecast_id,
                forecast_id_anterior=forecast_id_prev,
            )

            print("[DEBUG-GUARDADO] Paso 6: Insertando forecast detalle en BD")
            insertar_forecast_detalle(df_largo_filtrado.assign(ForecastID=forecast_id),
                                      forecast_id, anio, db_path)

            print("[DEBUG-GUARDADO] Paso 7: Refrescando buffer UI post-guardado")
            _refrescar_buffer_ui(forecast_id, key_buffer, db_path)

            st.success(f"✅ Cliente {cliente} guardado correctamente (ForecastID={forecast_id}).")

        except Exception as e:
            print(f"[ERROR-GUARDADO] ❌ Excepción durante guardado de cliente {cliente}: {e}")
            st.error(f"❌ Error al guardar cliente {cliente}: {e}")

            try:
                print("[DEBUG-GUARDADO] ↩ Intentando refresco alternativo por error de escritura")
                qry_ultimo = """
                    SELECT ItemCode, TipoForecast, OcrCode3, Linea, DocCur, Mes, 
                        SUM(Cant)       AS Cant, 
                        MAX(PrecioUN)   AS PrecioUN
                    FROM (
                        SELECT 
                            ItemCode,
                            TipoForecast,
                            OcrCode3,
                            Linea,
                            DocCur,
                            CAST(strftime('%m', FechEntr) AS TEXT) AS Mes,
                            Cant,
                            PrecioUN
                        FROM Forecast_Detalle
                        WHERE ForecastID = ?
                    ) AS t
                    GROUP BY ItemCode, TipoForecast, OcrCode3, Linea, DocCur, Mes;
                """
                df_post = run_query(qry_ultimo, params=(forecast_id,), db_path=db_path)

                if not df_post.empty:
                    print("[DEBUG-GUARDADO] Recuperación post-error OK. DF post:", df_post.shape)
                    cols_meses = [f"{m:02d}" for m in range(1, 13)]
                    df_cant = df_post.copy(); df_cant["Métrica"] = "Cantidad"
                    df_prec = df_post.copy(); df_prec["Métrica"] = "Precio"
                    pivot_cant = df_cant.pivot_table(
                        index=["ItemCode", "TipoForecast", "OcrCode3", "Linea", "DocCur", "Métrica"],
                        columns="Mes", values="Cant", aggfunc="first"
                    )
                    pivot_prec = df_prec.pivot_table(
                        index=["ItemCode", "TipoForecast", "OcrCode3", "Linea", "DocCur", "Métrica"],
                        columns="Mes", values="PrecioUN", aggfunc="first"
                    )

                    df_metrico = pd.concat([pivot_cant, pivot_prec]).reset_index().fillna(0)
                    for mes in cols_meses:
                        if mes not in df_metrico.columns:
                            df_metrico[mes] = 0
                    cols_fijas = [c for c in df_metrico.columns if c not in cols_meses]
                    df_metrico = df_metrico[cols_fijas + cols_meses]
                    st.session_state[key_buffer] = df_metrico.set_index(["ItemCode", "TipoForecast", "Métrica"])
                    st.dataframe(df_metrico)
                    st.success(f"✅ Cliente {cliente} guardado correctamente (ForecastID={forecast_id}).")
            except Exception as e2:
                print(f"[ERROR-GUARDADO] ❌ Falló refresco post-error para {cliente}: {e2}")
                st.error(f"❌ Error crítico al intentar refrescar buffer de cliente {cliente}: {e2}")

    print("[DEBUG-GUARDADO] 🧼 Limpiando lista de clientes_editados")
    st.session_state.pop("clientes_editados", None)





def seleccionar_forecast_base(
    slpcode: int,
    cardcode: str,
    anio: int,
    db_path: str
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
    df_indiv = run_query(sql_indiv, params=(slpcode, cardcode, str(anio)), db_path=db_path)
    if not df_indiv.empty and pd.notna(df_indiv.at[0, 'id']):
        return int(df_indiv.at[0, 'id'])

    # 2. Buscar ForecastID global que contenga al cliente
    sql_global = """
        SELECT MAX(ForecastID) as id
        FROM Forecast_Detalle
        WHERE CardCode = ?
    """
    df_global = run_query(sql_global, params=(cardcode,), db_path=db_path)
    if not df_global.empty and pd.notna(df_global.at[0, 'id']):
        return int(df_global.at[0, 'id'])

    # 3. No se encontró referencia previa
    return None


def _get_forecast_id_prev(slpcode: int, cardcode: str, anio: int, db_path: str) -> Optional[int]:
    """
    Busca el ForecastID más reciente para un cliente (CardCode) y vendedor (SlpCode).
    Si no existe Forecast individual, intenta buscar uno global (sin CardCode).
    Retorna None si no se encuentra ningún historial.
    """
    # 1. Intento por cliente específico
    qry_individual = """
        SELECT MAX(fd.ForecastID) AS id
        FROM   Forecast_Detalle fd
        WHERE  fd.SlpCode  = ?
          AND  fd.CardCode = ?
          AND  strftime('%Y', fd.FechEntr) = ?;
    """
    df_ind = run_query(qry_individual, params=(slpcode, cardcode, str(anio)), db_path=db_path)
    if not df_ind.empty and pd.notna(df_ind.iloc[0].id):
        print(f"[DEBUG-HISTORIAL] Se usará ForecastID individual: {df_ind.iloc[0].id}")
        return int(df_ind.iloc[0].id)

    # 2. Intento fallback: Forecast global para el mismo SlpCode (sin filtrar CardCode)
    qry_global = """
        SELECT MAX(fd.ForecastID) AS id
        FROM   Forecast_Detalle fd
        WHERE  fd.SlpCode  = ?
          AND  strftime('%Y', fd.FechEntr) = ?;
    """
    df_glob = run_query(qry_global, params=(slpcode, str(anio)), db_path=db_path)
    if not df_glob.empty and pd.notna(df_glob.iloc[0].id):
        print(f"[DEBUG-HISTORIAL] No existe forecast individual. Se usará ForecastID global: {df_glob.iloc[0].id}")
        return int(df_glob.iloc[0].id)

    print("[DEBUG-HISTORIAL] ⚠️ No se encontró forecast histórico (ni individual ni global). Se parte desde cero.")
    return None
