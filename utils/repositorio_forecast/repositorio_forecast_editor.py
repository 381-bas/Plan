# B_BUF001: Importaciones principales para gestión y persistencia de buffers de forecast
# # ∂B_BUF001/∂B0
import pandas as pd
import streamlit as st
from config.contexto import obtener_mes

# B_BUF002: Generación de clave única de buffer para cliente
# # ∂B_BUF002/∂B0
def get_key_buffer(cliente: str) -> str:
    return f"forecast_buffer_cliente_{cliente}"

# B_BUF003: Inicialización extendida del buffer forecast con estructura y Métrica
# # ∂B_BUF003/∂B0
def inicializar_buffer_cliente(cliente: str, df_base: pd.DataFrame, moneda_default: str = "CLP"):
    key = get_key_buffer(cliente)
    if key in st.session_state:
        return

    df_base = df_base.copy()
    df_base.columns = df_base.columns.astype(str)
    df_base["ItemCode"] = df_base["ItemCode"].astype(str).str.strip()
    df_base["TipoForecast"] = df_base["TipoForecast"].astype(str).str.strip()
    
    # 🧬 Forzar expansión por TipoForecast: cada ItemCode debe tener Firme y Proyectado
    tipos = ["Firme", "Proyectado"]
    df_expandido = []

    for itemcode in df_base["ItemCode"].unique():
        df_item = df_base[df_base["ItemCode"] == itemcode]
        tipos_actuales = df_item["TipoForecast"].unique()

        for tipo in tipos:
            if tipo in tipos_actuales:
                df_expandido.append(df_item[df_item["TipoForecast"] == tipo].copy())
            else:
                df_clon = df_item[df_item["TipoForecast"] == tipos_actuales[0]].copy()
                df_clon["TipoForecast"] = tipo
                meses = [str(m).zfill(2) for m in range(1, 13)]
                df_clon[meses] = 0  # Reiniciar valores de meses
                df_expandido.append(df_clon)

    df_base = pd.concat(df_expandido, ignore_index=True)

    # 🔒 Validar estructura mínima
    columnas_minimas = {"ItemCode", "TipoForecast"}
    if not columnas_minimas.issubset(df_base.columns):
        raise ValueError(f"Faltan columnas esenciales en df_base: {columnas_minimas - set(df_base.columns)}")

    # ⚠️ Captura los valores de PrecioUN antes de eliminar
    precio_un_map = None
    if "PrecioUN" in df_base.columns:
        precio_un_map = df_base[["ItemCode", "TipoForecast", "OcrCode3", "PrecioUN"]].copy()

    # 🧹 Eliminar columnas conflictivas
    columnas_prohibidas = ["PrecioUN", "_PrecioUN_", "PrecioUnitario"]
    df_base = df_base.drop(columns=[c for c in columnas_prohibidas if c in df_base.columns], errors="ignore")

    # Asegurar columnas adicionales
    if "OcrCode3" not in df_base.columns:
        df_base["OcrCode3"] = ""
    if "DocCur" not in df_base.columns:
        df_base["DocCur"] = moneda_default

    columnas_mes = [str(m).zfill(2) for m in range(1, 13)]

    # Crear duplicado por Métrica
    cantidad = df_base.copy()
    cantidad["Métrica"] = "Cantidad"

    precio = df_base.copy()
    precio["Métrica"] = "Precio"

    # ✅ Aplicar PrecioUN a todos los meses si estaba disponible
    if precio_un_map is not None:
        precio = precio.merge(precio_un_map, on=["ItemCode", "TipoForecast", "OcrCode3"], how="left")
        for col in columnas_mes:
            precio[col] = precio["PrecioUN"]
        precio = precio.drop(columns=["PrecioUN"], errors="ignore")
    else:
        precio[columnas_mes] = 0

    df_combo = pd.concat([cantidad, precio], ignore_index=True)

    st.session_state[key] = df_combo.set_index(["ItemCode", "TipoForecast", "Métrica"])

# B_BUF004: Obtención del DataFrame del buffer de cliente desde sesión
# # ∂B_BUF004/∂B0
def obtener_buffer_cliente(cliente: str) -> pd.DataFrame:
    key = get_key_buffer(cliente)
    df = st.session_state.get(key, pd.DataFrame()).copy()
    df.columns = df.columns.astype(str)
    return df

# B_BUF005: Actualización del buffer completo desde edición del usuario
# # ∂B_BUF005/∂B0
def actualizar_buffer_cliente(cliente: str, df_editado: pd.DataFrame):
    key = get_key_buffer(cliente)
    if key not in st.session_state:
        raise ValueError(f"El buffer para el cliente {cliente} no ha sido inicializado.")

    buffer_actual = st.session_state[key].copy()
    df_editado = df_editado.copy()
    df_editado.columns = df_editado.columns.astype(str)
    df_editado["ItemCode"] = df_editado["ItemCode"].astype(str).str.strip()
    df_editado["TipoForecast"] = df_editado["TipoForecast"].astype(str).str.strip()
    df_editado_indexed = df_editado.set_index(["ItemCode", "TipoForecast"])

    if not df_editado_indexed.index.equals(buffer_actual.index):
        raise ValueError("Los índices del DataFrame editado no coinciden con el buffer actual.")

    columnas_comunes = buffer_actual.columns.intersection(df_editado_indexed.columns)
    buffer_actual.update(df_editado_indexed[columnas_comunes])
    st.session_state[key] = buffer_actual

# B_BUF006: Limpieza del buffer para cliente
# # ∂B_BUF006/∂B0
def limpiar_buffer_cliente(cliente: str):
    key = get_key_buffer(cliente)
    if key in st.session_state:
        del st.session_state[key]

# B_BUF007: Sincronización parcial de edición sobre buffer cliente
# # ∂B_BUF007/∂B0
def sincronizar_edicion_parcial(cliente: str, df_editado_parcial: pd.DataFrame):
    key = get_key_buffer(cliente)
    if key not in st.session_state:
        raise ValueError(f"El buffer para el cliente {cliente} no ha sido inicializado.")

    buffer = st.session_state[key].copy()
    df_editado_parcial = df_editado_parcial.copy()
    df_editado_parcial.columns = df_editado_parcial.columns.astype(str)

    df_editado_parcial["ItemCode"] = df_editado_parcial["ItemCode"].astype(str).str.strip()
    df_editado_parcial["TipoForecast"] = df_editado_parcial["TipoForecast"].astype(str).str.strip()
    df_editado_parcial = df_editado_parcial.set_index(["ItemCode", "TipoForecast"])

    mes_actual = obtener_mes()
    columnas_objetivo = [str(m) for m in range(mes_actual, 13)] + ["PrecioUN"]

    faltantes = [col for col in columnas_objetivo if col not in df_editado_parcial.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas requeridas en edición: {faltantes}")

    for col in buffer.columns.intersection(df_editado_parcial.columns):
        if col in columnas_objetivo:
            buffer[col] = pd.to_numeric(buffer[col], errors="coerce").fillna(0)
            df_editado_parcial[col] = pd.to_numeric(df_editado_parcial[col], errors="coerce").fillna(0)

    for idx in df_editado_parcial.index:
        for col in columnas_objetivo:
            if col in df_editado_parcial.columns:
                buffer.at[idx, col] = df_editado_parcial.at[idx, col]

    st.session_state[key] = buffer

# B_VFD001: Validación estructural y de contenido del DataFrame de forecast
# # ∂B_VFD001/∂B0
def validar_forecast_dataframe(df: pd.DataFrame) -> list[str]:
    errores = []
    columnas_mes = [str(m).zfill(2) for m in range(1, 13)]

    df = df.copy()
    df.columns = df.columns.astype(str)

    # Validación básica
    campos_requeridos = ["ItemCode", "TipoForecast", "Métrica", "DocCur"]
    for col in campos_requeridos:
        if col not in df.columns:
            errores.append(f"Falta la columna requerida: {col}")

    # Normalización de valores clave
    if "ItemCode" in df.columns:
        df["ItemCode"] = df["ItemCode"].astype(str).str.strip()
    if "TipoForecast" in df.columns:
        df["TipoForecast"] = df["TipoForecast"].astype(str).str.strip().str.capitalize()
    if "Métrica" in df.columns:
        df["Métrica"] = df["Métrica"].astype(str).str.strip().str.capitalize()
    if "DocCur" in df.columns:
        df["DocCur"] = df["DocCur"].astype(str).str.strip().str.upper()

    # Validaciones de contenido
    if "Métrica" in df.columns:
        if not df["Métrica"].isin(["Cantidad", "Precio"]).all():
            errores.append("La columna 'Métrica' contiene valores inválidos (solo se permite 'Cantidad' o 'Precio').")

    if "DocCur" in df.columns:
        if not df["DocCur"].str.match(r"^[A-Z]{3}$").all():
            errores.append("La columna 'DocCur' debe contener solo códigos de moneda de 3 letras (ej. CLP, USD, EUR).")

    for col in columnas_mes:
        if col not in df.columns:
            errores.append(f"Falta la columna del mes {col}")

    # Validación estructural extendida
    if {"ItemCode", "TipoForecast", "Métrica", "OcrCode3"}.issubset(df.columns):
        if not df[["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]].drop_duplicates().shape[0] == df.shape[0]:
            errores.append("Existen filas duplicadas por [ItemCode, TipoForecast, Métrica, OcrCode3].")

    # Validación de columnas residuales sueltas
    if "PrecioUN" in df.columns and "Métrica" in df.columns:
        if not df[df["Métrica"] == "Precio"].empty and "PrecioUN" not in columnas_mes:
            errores.append("La columna 'PrecioUN' no debe existir como columna suelta. Debe estar distribuida en columnas mensuales.")

    # Validación de columnas inesperadas
    col_extranas = [col for col in df.columns if col.lower() not in 
                    {"itemcode", "tipoforecast", "métrica", "ocrcode3", "doccur"} 
                    and col not in columnas_mes]
    if col_extranas:
        errores.append(f"Columnas inesperadas detectadas: {col_extranas}")

    if errores:
        return errores

    # Validación de tipo de datos y negativos
    df[columnas_mes] = df[columnas_mes].apply(pd.to_numeric, errors="coerce").fillna(0)

    for col in columnas_mes:
        negativos = df[df[col] < 0]
        if not negativos.empty:
            codigos = negativos["ItemCode"].unique().tolist()
            errores.append(f"Valores negativos en mes {col} para: {codigos}")

    if not df["TipoForecast"].isin(["Firme", "Proyectado"]).all():
        errores.append("TipoForecast contiene valores inválidos (solo se permite 'Firme' o 'Proyectado').")

    return errores

# B_HDF001: Hash semántico para DataFrame con control de cambios estructurales
# # ∂B_HDF001/∂B0
def hash_semantico(df):
    return hash(pd.util.hash_pandas_object(df.sort_index(axis=1), index=True).sum())

# B_SYN001: Sincronización persistente del buffer editable con edición de usuario
# # ∂B_SYN001/∂B0
def sincronizar_buffer_edicion(df_buffer: pd.DataFrame, key_buffer: str) -> pd.DataFrame:
    """
    Refuerza persistencia de edición mixta:
    - Aplica cambios históricos del buffer editado a nueva vista df_buffer
    - Usa combinación única (ItemCode, TipoForecast, Métrica, OcrCode3) como clave de actualización
    """
    key_state = f"{key_buffer}_editado"
    if key_state not in st.session_state:
        return df_buffer

    df_editado = st.session_state[key_state]

    # 🧠 NUEVO BLOQUE PARA CORTAR LOOP
    if hash_semantico(df_editado) == hash_semantico(df_buffer):
        print(f"🛑 [SCANNER] Sincronización evitada: edición idéntica para {key_buffer}")
        return df_buffer

    columnas_clave = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
    # ✅ Validar unicidad de clave compuesta antes de indexar
    if df_editado.duplicated(subset=columnas_clave).any():
        print(f"[⚠️ DEBUG-SYNC] df_editado tiene claves duplicadas → update() fallará")
        print(df_editado[df_editado.duplicated(subset=columnas_clave, keep=False)].sort_values(columnas_clave))
        raise ValueError("Claves duplicadas detectadas en df_editado. No se puede sincronizar con update()")

    columnas_mes = [f"{i:02d}" for i in range(1, 13)]

    # 🔒 Filtrar columnas prohibidas
    columnas_prohibidas = ["PrecioUN", "_PrecioUN_", "PrecioUnitario"]
    df_editado = df_editado.drop(columns=[c for c in columnas_prohibidas if c in df_editado.columns], errors="ignore")

    # 🔍 Validar columnas mensuales
    faltantes = [col for col in columnas_mes if col not in df_editado.columns]
    if faltantes:
        raise ValueError(f"El buffer editado carece de columnas mensuales requeridas: {faltantes}")

    df_actualizado = df_buffer.copy()

    try:
        df_actualizado = df_actualizado.set_index(columnas_clave)
        df_editado = df_editado.set_index(columnas_clave)

        # 🧪 Validar cobertura de claves
        claves_faltantes = set(df_actualizado.index) - set(df_editado.index)
        if claves_faltantes:
            print(f"⚠️ Advertencia: {len(claves_faltantes)} combinaciones clave no fueron editadas.")

        # 🛑 Ordenar índices para evitar PerformanceWarning
        df_actualizado = df_actualizado.sort_index()
        df_editado = df_editado.sort_index()

        # 🧠 Evitar update si no hay diferencias
        try:
            iguales = df_actualizado[columnas_mes].equals(df_editado[columnas_mes])
        except Exception as e:
            print(f"[⚠️ COMPARACIÓN FALLIDA] {e}")
            iguales = False

        if not iguales:
            df_actualizado.update(df_editado[columnas_mes])

        # ✅ Restaurar columnas adicionales que no fueron tocadas por edición
        columnas_extra = [col for col in df_buffer.columns if col not in df_actualizado.reset_index().columns]
        for col in columnas_extra:
            df_actualizado[col] = df_buffer.set_index(columnas_clave)[col]

        df_actualizado = df_actualizado.reset_index()

    except Exception as e:
        print(f"[ERROR] No se pudo sincronizar buffer editado: {e}")
        return df_buffer

    return df_actualizado

# B_SYN002: Actualización simbólica y persistente del buffer editado en sesión global
# # ∂B_SYN002/∂B0
def actualizar_buffer_global(df_editado: pd.DataFrame, key_buffer: str):
    """
    Almacena el DataFrame editado en session_state como buffer vivo.
    Usa clave simbólica con sufijo '_editado' para edición persistente.
    """
    key_state = f"{key_buffer}_editado"

    # Validación estructural mínima
    columnas_requeridas = {"ItemCode", "TipoForecast", "Métrica", "OcrCode3"}
    if not columnas_requeridas.issubset(df_editado.columns):
        raise ValueError(f"El DataFrame editado carece de columnas requeridas: {columnas_requeridas}")

    # Limpieza defensiva
    columnas_prohibidas = ["PrecioUN", "_PrecioUN_", "PrecioUnitario"]
    df_editado = df_editado.drop(columns=[c for c in columnas_prohibidas if c in df_editado.columns], errors="ignore")

    st.session_state[key_state] = df_editado.copy()
    
    # ✅ Línea esencial para sincronizar buffer principal
    st.session_state[key_buffer] = df_editado.set_index(["ItemCode", "TipoForecast", "Métrica"])
    # Marca interna de sincronización
    st.session_state["__buffer_editado__"] = True




# B_SYN003: Fusión estructural del buffer activo con edición parcial visual
# # ∂B_SYN003/∂B0
from typing import Tuple
import numpy as np
import pandas as pd

def sincronizar_buffer_local(
    df_buffer: pd.DataFrame,
    df_editado: pd.DataFrame
) -> Tuple[pd.DataFrame, bool]:
    """
    Fusiona los cambios del editor con el buffer activo y
    devuelve (df_final, hay_cambios).
    """
    columnas_clave = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]

    # ── Detectar dinámicamente las columnas-mes ──────────────────────────────
    columnas_mes = sorted(
        [c for c in df_editado.columns if c.isdigit() and len(c) <= 2],
        key=lambda x: int(x)
    )
    columnas_req = columnas_clave + columnas_mes

    print("[DEBUG-SYNC] Iniciando sincronización para forecast_buffer")
    print(f"[DEBUG-SYNC] Tamaño DF editado recibido: {df_editado.shape}")
    print(f"[DEBUG-SYNC] Buffer base recuperado:  {df_buffer.shape}")

    # ── Validación mínima de esquema ────────────────────────────────────────
    faltantes = set(columnas_req) - set(df_editado.columns)
    if faltantes:
        raise ValueError(f"El DataFrame editado carece de columnas requeridas: {faltantes}")

    # ── Índices normalizados ────────────────────────────────────────────────
    buf_idx = df_buffer.set_index(columnas_clave)
    edi_idx = df_editado.set_index(columnas_clave)

    # Ordenarlos una única vez: evita PerformanceWarning y acelera update()
    buf_idx = buf_idx.sort_index()
    edi_idx = edi_idx.sort_index()

    # Unir índices para contemplar filas nuevas/eliminadas
    idx_union = buf_idx.index.union(edi_idx.index)

    # IMPORTANTÍSIMO: reindex devuelve vistas DESORDENADAS → volvemos a ordenar
    buf_idx = buf_idx.reindex(idx_union).sort_index()
    edi_idx = edi_idx.reindex(idx_union).sort_index()

    # ── Comparación de celdas (tolerante a float/NaN) ───────────────────────
    diff_array = ~np.isclose(
        buf_idx[columnas_mes],
        edi_idx[columnas_mes],
        atol=1e-6,
        equal_nan=True
    )
    dif_mask = pd.DataFrame(diff_array, index=buf_idx.index, columns=columnas_mes)

    total_diff  = int(dif_mask.values.sum())
    filas_diff  = int(dif_mask.any(axis=1).sum())
    hay_cambios = total_diff > 0

    if hay_cambios:
        print(f"[DEBUG-SYNC] Total celdas modificadas: {total_diff}")
        print(f"[DEBUG-SYNC] Filas afectadas: {filas_diff}")
        cols_mod = dif_mask.any().pipe(lambda s: s[s].index.tolist())
        print(f"[DEBUG-SYNC] Columnas mensuales modificadas: {cols_mod}")

        # Aplicar cambios
        buf_idx.update(edi_idx[columnas_mes])

        # Filas completamente nuevas
        filas_nuevas = dif_mask.index[dif_mask.all(axis=1)]
        if len(filas_nuevas):
            print(f"[DEBUG-SYNC] Filas nuevas detectadas: {len(filas_nuevas)}")
            buf_idx.loc[filas_nuevas, columnas_mes] = edi_idx.loc[filas_nuevas, columnas_mes]
    else:
        print("[DEBUG-SYNC] No se detectaron diferencias reales.")

    # ── Reconstrucción final con columnas extra ─────────────────────────────
    #    Calculamos todas las columnas NO-mes ni clave presentes en
    #    buffer o editado (ItemName, DocCur, etc.)
    cols_extra_union = [
        c for c in set(df_buffer.columns).union(df_editado.columns)
        if c not in columnas_clave and c not in columnas_mes
    ]

    #   Restaura esos campos priorizando datos de df_editado
    if cols_extra_union:
        # a) Start with values from buffer (may include NaN)
        buf_idx[cols_extra_union] = (
            df_buffer.set_index(columnas_clave)[cols_extra_union]
            .reindex(buf_idx.index)
        )
        # b) Update with non-NaN coming from editado
        edi_extra = (
            df_editado.set_index(columnas_clave)[cols_extra_union]
            .reindex(buf_idx.index)
        )
        buf_idx.update(edi_extra)

    #   Ensamblamos el DataFrame final
    df_final = buf_idx.reset_index().reindex(
        columns=columnas_req + cols_extra_union
    )

    #   Aplicar dtypes solo a columnas presentes
    dtype_map = {
        c: t for c, t in df_buffer.dtypes.items()
        if c in df_final.columns
    }
    df_final = df_final.astype(dtype_map, errors="ignore")

    print(f"[DEBUG-SYNC] Buffer final preparado. Filas: {len(df_final)}")
    print(f"[DEBUG-SYNC] Columnas finales: {list(df_final.columns)}")

    return df_final, hay_cambios



