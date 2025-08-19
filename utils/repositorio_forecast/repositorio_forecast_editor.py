# B_BUF001: Importaciones principales para gestión y persistencia de buffers de forecast
# # ∂B_BUF001/∂B0
import pandas as pd  # noqa: E402
import streamlit as st
from config.contexto import obtener_mes
import numpy as np
from typing import Tuple


# B_BUF002: Generación de clave única de buffer para cliente
# # ∂B_BUF002/∂B0
def get_key_buffer(cliente: str) -> str:
    return f"forecast_buffer_cliente_{cliente}"


# B_BUF003: Inicialización extendida del buffer forecast con estructura y Métrica
# # ∂B_BUF003/∂B0
def inicializar_buffer_cliente(
    cliente: str, df_base: pd.DataFrame, moneda_default: str = "CLP"
):
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
        raise ValueError(
            f"Faltan columnas esenciales en df_base: {columnas_minimas - set(df_base.columns)}"
        )

    # ⚠️ Captura los valores de PrecioUN antes de eliminar
    precio_un_map = None
    if "PrecioUN" in df_base.columns:
        precio_un_map = df_base[
            ["ItemCode", "TipoForecast", "OcrCode3", "PrecioUN"]
        ].copy()

    # 🧹 Eliminar columnas conflictivas
    columnas_prohibidas = ["PrecioUN", "_PrecioUN_", "PrecioUnitario"]
    df_base = df_base.drop(
        columns=[c for c in columnas_prohibidas if c in df_base.columns],
        errors="ignore",
    )

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
        precio = precio.merge(
            precio_un_map, on=["ItemCode", "TipoForecast", "OcrCode3"], how="left"
        )
        for col in columnas_mes:
            precio[col] = precio["PrecioUN"]
        precio = precio.drop(columns=["PrecioUN"], errors="ignore")
    else:
        precio[columnas_mes] = 0

    df_combo = pd.concat([cantidad, precio], ignore_index=True)

    st.session_state[key] = df_combo.set_index(["ItemCode", "TipoForecast", "Métrica"])


# B_BUF004: Obtención del DataFrame del buffer de cliente desde sesión
# # ∂B_BUF004/∂B0
# --- PATCH B: obtener_buffer_cliente seguro (opcional)
def obtener_buffer_cliente(key_buffer: str):
    import pandas as pd

    if key_buffer not in st.session_state:
        # Crear un esqueleto mínimo si llegaran a llamarlo sin bootstrap
        MESES = [f"{m:02d}" for m in range(1, 13)]
        cols_base = [
            "ItemCode",
            "ItemName",
            "TipoForecast",
            "OcrCode3",
            "DocCur",
            "Métrica",
        ]
        st.session_state[key_buffer] = pd.DataFrame(
            columns=cols_base + MESES
        ).set_index(["ItemCode", "TipoForecast", "Métrica"])
        st.session_state.setdefault(
            f"{key_buffer}_editado", st.session_state[key_buffer].copy()
        )
        st.session_state.setdefault(
            f"{key_buffer}_prev", st.session_state[key_buffer].copy()
        )
    return st.session_state[key_buffer]


# B_BUF005: Actualización del buffer completo desde edición del usuario
# # ∂B_BUF005/∂B0
def actualizar_buffer_cliente(cliente: str, df_editado: pd.DataFrame):
    key = get_key_buffer(cliente)
    if key not in st.session_state:
        raise ValueError(
            f"El buffer para el cliente {cliente} no ha sido inicializado."
        )

    buffer_actual = st.session_state[key].copy()
    df_editado = df_editado.copy()
    df_editado.columns = df_editado.columns.astype(str)
    df_editado["ItemCode"] = df_editado["ItemCode"].astype(str).str.strip()
    df_editado["TipoForecast"] = df_editado["TipoForecast"].astype(str).str.strip()
    df_editado_indexed = df_editado.set_index(["ItemCode", "TipoForecast"])

    if not df_editado_indexed.index.equals(buffer_actual.index):
        raise ValueError(
            "Los índices del DataFrame editado no coinciden con el buffer actual."
        )

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
        raise ValueError(
            f"El buffer para el cliente {cliente} no ha sido inicializado."
        )

    buffer = st.session_state[key].copy()
    df_editado_parcial = df_editado_parcial.copy()
    df_editado_parcial.columns = df_editado_parcial.columns.astype(str)

    df_editado_parcial["ItemCode"] = (
        df_editado_parcial["ItemCode"].astype(str).str.strip()
    )
    df_editado_parcial["TipoForecast"] = (
        df_editado_parcial["TipoForecast"].astype(str).str.strip()
    )
    df_editado_parcial = df_editado_parcial.set_index(["ItemCode", "TipoForecast"])

    mes_actual = obtener_mes()
    columnas_objetivo = [str(m) for m in range(mes_actual, 13)] + ["PrecioUN"]

    faltantes = [
        col for col in columnas_objetivo if col not in df_editado_parcial.columns
    ]
    if faltantes:
        raise ValueError(f"Faltan columnas requeridas en edición: {faltantes}")

    for col in buffer.columns.intersection(df_editado_parcial.columns):
        if col in columnas_objetivo:
            buffer[col] = pd.to_numeric(buffer[col], errors="coerce").fillna(0)
            df_editado_parcial[col] = pd.to_numeric(
                df_editado_parcial[col], errors="coerce"
            ).fillna(0)

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
            errores.append(
                "La columna 'Métrica' contiene valores inválidos (solo se permite 'Cantidad' o 'Precio')."
            )

    if "DocCur" in df.columns:
        if not df["DocCur"].str.match(r"^[A-Z]{3}$").all():
            errores.append(
                "La columna 'DocCur' debe contener solo códigos de moneda de 3 letras (ej. CLP, USD, EUR)."
            )

    for col in columnas_mes:
        if col not in df.columns:
            errores.append(f"Falta la columna del mes {col}")

    # Validación estructural extendida
    if {"ItemCode", "TipoForecast", "Métrica", "OcrCode3"}.issubset(df.columns):
        if (
            not df[["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]]
            .drop_duplicates()
            .shape[0]
            == df.shape[0]
        ):
            errores.append(
                "Existen filas duplicadas por [ItemCode, TipoForecast, Métrica, OcrCode3]."
            )

    # Validación de columnas residuales sueltas
    if "PrecioUN" in df.columns and "Métrica" in df.columns:
        if not df[df["Métrica"] == "Precio"].empty and "PrecioUN" not in columnas_mes:
            errores.append(
                "La columna 'PrecioUN' no debe existir como columna suelta. Debe estar distribuida en columnas mensuales."
            )

    # Validación de columnas inesperadas
    col_extranas = [
        col
        for col in df.columns
        if col.lower()
        not in {"itemcode", "tipoforecast", "métrica", "ocrcode3", "doccur"}
        and col not in columnas_mes
    ]
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
        errores.append(
            "TipoForecast contiene valores inválidos (solo se permite 'Firme' o 'Proyectado')."
        )

    return errores


# B_HDF001: Hash semántico para DataFrame con control de cambios estructurales
# # ∂B_HDF001/∂B0
def hash_semantico(df):
    return hash(pd.util.hash_pandas_object(df.sort_index(axis=1), index=True).sum())


# B_SYN001: Sincronización persistente del buffer editable con edición de usuario
# # ∂B_SYN001/∂B0
def sincronizar_buffer_edicion(
    df_buffer: pd.DataFrame, key_buffer: str
) -> pd.DataFrame:
    """
    Mezcla el estado editado (session_state[key_buffer+'_editado']) dentro del buffer activo.
    - Entrada y salida SIEMPRE planas (claves como columnas, no en índice)
    - Claves y meses 01..12 garantizados
    - Merge tolerante a tipos (numérico para meses)
    """
    import numpy as np

    KEYS = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
    MESES = [f"{m:02d}" for m in range(1, 13)]
    key_state = f"{key_buffer}_editado"

    print("\n[SYNC-DEBUG] ============= INICIO SINCRONIZACIÓN =============")
    print(f"[SYNC-DEBUG] Shape buffer (in): {df_buffer.shape}")
    print(f"[SYNC-DEBUG] Cols buffer (in): {df_buffer.columns.tolist()}")
    print(f"[SYNC-DEBUG] Index buffer (in): {getattr(df_buffer.index, 'names', None)}")

    # --- 0) Entradas siempre planas ---
    if isinstance(
        df_buffer.index, (pd.MultiIndex, pd.Index)
    ) and df_buffer.index.names != [None]:
        df_buffer = df_buffer.reset_index()
        print(f"[SYNC-DEBUG] Buffer reset_index → cols: {df_buffer.columns.tolist()}")

    if key_state not in st.session_state:
        print("[SYNC-DEBUG] No existe estado editado previo; retorno buffer plano.")
        # Salida plana garantizada
        return df_buffer.copy()

    df_editado = st.session_state[key_state]
    print(f"[SYNC-DEBUG] Shape editado (raw): {getattr(df_editado, 'shape', None)}")
    if isinstance(
        df_editado.index, (pd.MultiIndex, pd.Index)
    ) and df_editado.index.names != [None]:
        df_editado = df_editado.reset_index()
        print(f"[SYNC-DEBUG] Editado reset_index → cols: {df_editado.columns.tolist()}")

    # --- 1) Garantizar claves en editado (si faltan, tomar de buffer o defaults) ---
    for k, default in [
        ("ItemCode", ""),
        ("TipoForecast", ""),
        ("Métrica", "Cantidad"),
        ("OcrCode3", ""),
    ]:
        if k not in df_editado.columns:
            if k in df_buffer.columns and len(df_buffer) == len(df_editado):
                df_editado[k] = df_buffer[k].values
                print(
                    f"[SYNC-DEBUG] Relleno editado.{k} desde buffer ({len(df_editado)} vals)."
                )
            else:
                df_editado[k] = default
                print(f"[SYNC-DEBUG] Relleno editado.{k} con default='{default}'.")

    # --- 2) Garantizar meses en editado (si falta alguno, tomar de buffer o 0.0) ---
    for m in MESES:
        if m not in df_editado.columns:
            if m in df_buffer.columns and len(df_buffer) == len(df_editado):
                df_editado[m] = df_buffer[m].values
                print(f"[SYNC-DEBUG] Relleno mes {m} desde buffer.")
            else:
                df_editado[m] = 0.0
                print(f"[SYNC-DEBUG] Relleno mes {m} con 0.0 (default).")

    # --- 3) Validación mínima de esquema ---
    falt_buf = [c for c in KEYS if c not in df_buffer.columns]
    if falt_buf:
        print(
            f"[SYNC-DEBUG] Buffer carece de claves {falt_buf}; intentando sanear desde editado/defaults."
        )
        for k in falt_buf:
            df_buffer[k] = (
                df_editado[k].values
                if k in df_editado.columns and len(df_editado) == len(df_buffer)
                else ""
            )
    falt_edi = [c for c in KEYS if c not in df_editado.columns]
    if falt_edi:
        raise ValueError(f"Faltan claves requeridas en editado: {falt_edi}")

    # --- 4) Preparar índices y alinear universo de filas ---
    buf_idx = df_buffer.set_index(KEYS).sort_index()
    edi_idx = df_editado.set_index(KEYS).sort_index()
    idx_union = buf_idx.index.union(edi_idx.index)
    buf_idx = buf_idx.reindex(idx_union).sort_index()
    edi_idx = edi_idx.reindex(idx_union).sort_index()

    # --- 5) Comparación y normalización numérica de meses ---
    buf_num = buf_idx[MESES].apply(pd.to_numeric, errors="coerce")
    edi_num = edi_idx[MESES].apply(pd.to_numeric, errors="coerce")

    diff_array = ~np.isclose(buf_num.values, edi_num.values, atol=1e-6, equal_nan=True)
    hay_cambios = bool(diff_array.sum() > 0)
    print(f"[SYNC-DEBUG] ¿Hay cambios en meses?: {hay_cambios}")

    # Normalizar buffer a numérico antes de update
    buf_idx[MESES] = buf_num

    if hay_cambios:
        buf_idx.update(edi_num)
        print("[SYNC-DEBUG] Aplicados cambios numéricos en meses.")

    # --- 6) Columnas extra (no mes / no clave): prioriza editado sobre buffer ---
    extras = [
        c
        for c in set(df_buffer.columns).union(df_editado.columns)
        if c not in KEYS and c not in MESES
    ]
    if extras:
        buf_idx[extras] = df_buffer.set_index(KEYS)[extras].reindex(buf_idx.index)
        edi_extra = df_editado.set_index(KEYS)[extras].reindex(buf_idx.index)
        buf_idx.update(edi_extra)
        print(f"[SYNC-DEBUG] Extras fusionados: {extras}")

    # --- 7) Salida plana garantizada + orden razonable ---
    df_final = buf_idx.reset_index()
    # Orden: claves + extras + meses (la vista luego reordena si quiere)
    ordered_cols = KEYS + extras + MESES
    df_final = df_final.reindex(
        columns=[c for c in ordered_cols if c in df_final.columns]
    )

    print(f"[SYNC-DEBUG] Shape buffer (out): {df_final.shape}")
    print(f"[SYNC-DEBUG] Cols buffer (out): {df_final.columns.tolist()}")

    return df_final


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
        raise ValueError(
            f"El DataFrame editado carece de columnas requeridas: {columnas_requeridas}"
        )

    # Limpieza defensiva
    columnas_prohibidas = ["PrecioUN", "_PrecioUN_", "PrecioUnitario"]
    df_editado = df_editado.drop(
        columns=[c for c in columnas_prohibidas if c in df_editado.columns],
        errors="ignore",
    )

    st.session_state[key_state] = df_editado.copy()

    # ✅ Línea esencial para sincronizar buffer principal
    st.session_state[key_buffer] = df_editado.set_index(
        ["ItemCode", "TipoForecast", "Métrica"]
    )
    # Marca interna de sincronización
    st.session_state["__buffer_editado__"] = True


# B_SYN003: Fusión estructural del buffer activo con edición parcial visual
# # ∂B_SYN003/∂B0
import pandas as pd  # noqa: E402


def sincronizar_buffer_local(
    df_buffer: pd.DataFrame, df_editado: pd.DataFrame
) -> Tuple[pd.DataFrame, bool]:
    """
    Fusiona los cambios del editor con el buffer activo y
    devuelve (df_final, hay_cambios).
    """
    # --- PRELUDIO: Normalización de entrada (siempre columnas, nunca índices) ---
    MESES = [f"{m:02d}" for m in range(1, 13)]

    df_b = df_buffer.copy()
    df_e = df_editado.copy()

    if "ItemCode" not in df_b.columns:
        df_b = df_b.reset_index()
    if "ItemCode" not in df_e.columns:
        df_e = df_e.reset_index()

    for k, default in [("ItemCode", ""), ("TipoForecast", ""), ("Métrica", "Cantidad")]:
        if k not in df_e.columns:
            if k in df_b.columns and len(df_b) == len(df_e):
                df_e[k] = df_b[k].values
            else:
                df_e[k] = default

    for m in MESES:
        if m not in df_e.columns:
            df_e[m] = df_b[m] if m in df_b.columns and len(df_b) == len(df_e) else 0.0

    df_buffer = df_b
    df_editado = df_e

    columnas_clave = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
    columnas_mes = MESES[:]  # ✅ estable

    columnas_req = columnas_clave + columnas_mes

    print("[DEBUG-SYNC] Iniciando sincronización para forecast_buffer")
    print(f"[DEBUG-SYNC] Tamaño DF editado recibido: {df_editado.shape}")
    print(f"[DEBUG-SYNC] Buffer base recuperado:  {df_buffer.shape}")

    faltantes = set(columnas_req) - set(df_editado.columns)
    if faltantes:
        raise ValueError(
            f"El DataFrame editado carece de columnas requeridas: {faltantes}"
        )

    buf_idx = df_buffer.set_index(columnas_clave).sort_index()
    edi_idx = df_editado.set_index(columnas_clave).sort_index()

    idx_union = buf_idx.index.union(edi_idx.index)
    buf_idx = buf_idx.reindex(idx_union).sort_index()
    edi_idx = edi_idx.reindex(idx_union).sort_index()

    # ✅ Comparación robusta a tipos
    buf_num = buf_idx[columnas_mes].apply(pd.to_numeric, errors="coerce")
    edi_num = edi_idx[columnas_mes].apply(pd.to_numeric, errors="coerce")

    diff_array = ~np.isclose(buf_num.values, edi_num.values, atol=1e-6, equal_nan=True)
    dif_mask = pd.DataFrame(diff_array, index=buf_idx.index, columns=columnas_mes)

    total_diff = int(dif_mask.values.sum())
    filas_diff = int(dif_mask.any(axis=1).sum())
    hay_cambios = total_diff > 0

    if hay_cambios:
        print(f"[DEBUG-SYNC] Total celdas modificadas: {total_diff}")
        print(f"[DEBUG-SYNC] Filas afectadas: {filas_diff}")
        cols_mod = dif_mask.any().pipe(lambda s: s[s].index.tolist())
        print(f"[DEBUG-SYNC] Columnas mensuales modificadas: {cols_mod}")

        # Normaliza a numérico el buffer antes de aplicar cambios
        buf_idx[columnas_mes] = buf_num

        # Aplicar cambios desde el editor (numérico)
        buf_idx.update(edi_num)

        filas_nuevas = dif_mask.index[dif_mask.all(axis=1)]
        if len(filas_nuevas):
            print(f"[DEBUG-SYNC] Filas nuevas detectadas: {len(filas_nuevas)}")
            buf_idx.loc[filas_nuevas, columnas_mes] = edi_num.loc[
                filas_nuevas, columnas_mes
            ]
    else:
        print("[DEBUG-SYNC] No se detectaron diferencias reales.")

    cols_extra_union = [
        c
        for c in set(df_buffer.columns).union(df_editado.columns)
        if c not in columnas_clave and c not in columnas_mes
    ]

    if cols_extra_union:
        buf_idx[cols_extra_union] = df_buffer.set_index(columnas_clave)[
            cols_extra_union
        ].reindex(buf_idx.index)
        edi_extra = df_editado.set_index(columnas_clave)[cols_extra_union].reindex(
            buf_idx.index
        )
        buf_idx.update(edi_extra)

    df_final = buf_idx.reset_index().reindex(columns=columnas_req + cols_extra_union)

    dtype_map = {c: t for c, t in df_buffer.dtypes.items() if c in df_final.columns}
    df_final = df_final.astype(dtype_map, errors="ignore")

    print(f"[DEBUG-SYNC] Buffer final preparado. Filas: {len(df_final)}")
    print(f"[DEBUG-SYNC] Columnas finales: {list(df_final.columns)}")

    # --- POSTLUDIO: salida plana garantizada ---
    if isinstance(df_final.index, pd.MultiIndex) or df_final.index.name is not None:
        df_final = df_final.reset_index()

    for k, default in [("ItemCode", ""), ("TipoForecast", ""), ("Métrica", "Cantidad")]:
        if k not in df_final.columns:
            df_final[k] = default
    for m in MESES:
        if m not in df_final.columns:
            df_final[m] = 0.0

    return df_final, hay_cambios
