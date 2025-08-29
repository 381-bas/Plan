# B_VIN002: Importaciones fundacionales y dependencias funcionales para ventas
# # ∂B_VIN002/∂B0
import hashlib
import sqlite3
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st

from config.contexto import obtener_anio  # ∂
from core.consultas_forecast import (
    obtener_clientes,  # ∂B
)
from core.historico import vista_historico
from core.stock import vista_stock
from modulos.editor_forecast import (
    inicializar_buffer_cliente,  # ∂B
    obtener_forecast_editable,  # ∂B
    persistir_cambios_cliente,
    sincronizar_buffer_local,  # ∂B
    validar_forecast_dataframe,  # ∂B
)
from modulos.ventas_facturas_snippet import mostrar_facturas
from utils.alertas import (
    render_alertas_forecast,
)
from utils.db import DB_PATH


# --- util: reconstruir DF guardable desde el buffer de sesión ---
def _df_to_save_from_session(cardcode: str) -> Optional[pd.DataFrame]:
    key_buffer = f"forecast_buffer_{cardcode}"
    df_buf = st.session_state.get(key_buffer)
    if df_buf is None or not isinstance(df_buf, pd.DataFrame):
        print(f"[BATCH.WARN] buffer ausente para {cardcode}")
        return None
    base_idx = [
        c
        for c in ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
        if c in df_buf.columns
    ]
    try:
        df = df_buf
        if not df.index.names or any(n is None for n in df.index.names):
            # por si quedó reseteado
            df = df.set_index(base_idx).sort_index()
        return df.reset_index()
    except Exception as e:
        print(f"[BATCH.WARN] no se pudo reconstruir df_to_save para {cardcode}: {e}")
        return None


# --- helper para resolver la ruta de la DB de forma consistente ---
def get_db_path() -> str:
    """
    Devuelve el path efectivo de la base de datos.
    Prioriza lo que venga en la sesión y cae al constante DB_PATH.
    """
    try:
        return str(st.session_state.get("DB_PATH", DB_PATH))
    except Exception:
        return str(DB_PATH)


# ---------- Guardado batch de múltiples clientes editados ----------
def _guardar_ediciones_en_lote(
    slpcode: int,
    anio: int,
    db_path: str,
    current_cardcode: str,
    df_to_save_actual: Optional[pd.DataFrame],  # del cliente visible (ya sincronizado)
) -> dict:
    if st.session_state.get("_saving_global", False):
        return {"ok": False, "error": "saving_global_en_curso"}

    st.session_state["_saving_global"] = True
    try:
        editados: set[str] = set(st.session_state.get("clientes_editados", set()))
        # incluir cliente actual si tiene cambios en staging y aún no está en el set
        if df_to_save_actual is not None:
            editados.add(current_cardcode)

        targets = sorted(editados)
        print(f"[BATCH.SAVE] start targets={targets}")

        resultados = []
        for cc in targets:
            df_to_save = (
                df_to_save_actual
                if cc == current_cardcode
                else _df_to_save_from_session(cc)
            )
            if df_to_save is None or df_to_save.empty:
                resultados.append(
                    {"cardcode": cc, "ok": False, "error": "df_to_save vacío o ausente"}
                )
                print(f"[BATCH.SAVE.SKIP] {cc} df vacío")
                continue

            res = persistir_cambios_cliente(
                slpcode=slpcode,
                cardcode=cc,
                anio=anio,
                df_ui_editado=df_to_save,
                db_path=db_path,
                key_buffer=f"forecast_buffer_{cc}",
            )
            resultados.append({"cardcode": cc, **res})

        # Resumen
        oks = [r for r in resultados if r.get("ok")]
        fails = [r for r in resultados if not r.get("ok")]

        print(f"[BATCH.SAVE.OK] clientes_ok={len(oks)} clientes_fail={len(fails)}")
        return {
            "ok": len(fails) == 0,
            "oks": oks,
            "fails": fails,
            "resultados": resultados,
        }
    finally:
        st.session_state["_saving_global"] = False


# ---------- Fingerprint & cachés ligeros ----------
def _db_fingerprint(
    db_path: str | Path | None = None,
) -> tuple[int, int, int, int, int, int]:
    """
    Fingerprint de 6-tuple que invalida cachés cuando cambia la DB.
    Acepta un path explícito; si no se da, usa DB_PATH.
    """
    db_path = str(db_path or DB_PATH)
    p = Path(db_path)
    try:
        s = p.stat()
        with sqlite3.connect(db_path) as con:
            cur = con.cursor()
            # Conteos y máximos “baratos” que se mueven al guardar
            nF, maxF = cur.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(ForecastID), 0)
                FROM Forecast
            """
            ).fetchone()

            nD, maxRowid = cur.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(rowid), 0)
                FROM Forecast_Detalle
            """
            ).fetchone()

        return (
            int(s.st_mtime_ns),  # 1
            int(s.st_size),  # 2
            int(nF),  # 3
            int(maxF),  # 4
            int(nD),  # 5
            int(maxRowid),  # 6
        )
    except Exception as e:
        print(f"[FP][warn] fingerprint fallback: {e}")
        return (0, 0, 0, 0, 0, 0)


@st.cache_data(show_spinner=False, ttl=600, max_entries=256)
def _cached_obtener_clientes(slpcode: int, db_fp: tuple[int, int, int, int, int, int]):
    return obtener_clientes(slpcode)


@st.cache_data(show_spinner=False, ttl=600, max_entries=512)
def _cached_obtener_forecast_editable(
    slpcode: int, cardcode: str, anio: int, db_fp: tuple[int, int, int, int, int, int]
):
    return obtener_forecast_editable(slpcode, cardcode, anio=anio)


# B_HDF002: Generación de hash semántico robusto para buffers editables
# # ∂B_HDF002/∂B0
def hash_df(df: pd.DataFrame) -> int:
    """
    Hash estable del contenido numérico (meses primero), independiente del orden de filas/columnas.
    - Solo columnas numéricas
    - Incluye el índice en el hash
    - NaN -> 0.0, float redondeado (6 decimales) para evitar ruido
    - Devuelve un int derivado de blake2b (16 bytes)
    """

    if not isinstance(df, pd.DataFrame) or df.empty:
        return 0

    # 1) Solo numéricas
    df_num = df.select_dtypes(include=[np.number]).copy()
    if df_num.shape[1] == 0:
        # Sin columnas numéricas → hash neutro pero estable
        # Incluimos shape para distinguir vacío vs no vacío
        h = hashlib.blake2b(digest_size=16)
        h.update(str(df.shape).encode("utf-8"))
        return int.from_bytes(h.digest(), "big")

    # 2) Orden canónico de columnas: meses primero ("01".."12"), luego el resto ordenado
    months = [f"{m:02d}" for m in range(1, 13)]
    months_present = [m for m in months if m in df_num.columns]
    other_cols = sorted([c for c in df_num.columns if c not in months_present])
    df_num = df_num[months_present + other_cols]

    # 3) Normalización de valores + orden estable de filas/columnas
    df_num = (
        df_num.sort_index(axis=0)  # filas
        .sort_index(axis=1)  # columnas (aunque ya están canónicas)
        .fillna(0.0)
        .astype("float64")
        .round(6)  # evita ruido numérico
    )

    # 4) Hash robusto: combinamos hash por-fila de pandas con blake2b
    row_hash = pd.util.hash_pandas_object(df_num, index=True).to_numpy(
        dtype="uint64", copy=False
    )

    h = hashlib.blake2b(digest_size=16)
    # valores
    h.update(row_hash.tobytes())
    # metadatos mínimos: columnas, nombres de índice y shape
    h.update(("|".join(df_num.columns)).encode("utf-8"))
    h.update(str(df_num.index.names).encode("utf-8"))
    h.update(str(df_num.shape).encode("utf-8"))

    return int.from_bytes(h.digest(), "big")


# 👇 ponlo en modulos/ventas.py (arriba del todo)
def ensure_url_reflects_ventas(slpcode: int | str | None) -> None:
    qp = st.query_params
    updates = {}
    if qp.get("modulo") != "ventas":
        updates["modulo"] = "ventas"
    if slpcode is not None and qp.get("vendedor") != str(slpcode):
        updates["vendedor"] = str(slpcode)
    if updates:
        # Importante: update() ya re-ejecuta; no llames st.rerun()
        st.query_params.update(updates)


# 👇 helper pequeño (opcional, pero útil)
def _slp_from_url() -> int | None:
    v = st.query_params.get("vendedor") or st.query_params.get("slpcode")
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


LAST_CLIENT_KEY = "_last_cliente_forecast"


def _canon_for_hash_global(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.columns = df2.columns.astype(str)
    base_idx = [
        c
        for c in ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
        if c in df2.columns
    ]
    if base_idx:
        df2 = df2.set_index(base_idx)
    return df2.sort_index().sort_index(axis=1)


def _sync_prev_cliente_before_change():
    """Se invoca desde el callback del selectbox. Sincroniza el cliente saliente usando LAST_CLIENT_KEY."""
    prev = st.session_state.get(LAST_CLIENT_KEY)
    if not prev:
        print("[PRE-SWITCH] no LAST_CLIENT_KEY — nada que sincronizar")
        return

    key_buffer = f"forecast_buffer_{prev}"
    key_mirror = f"{key_buffer}_editado"
    key_hash_edit = f"{key_buffer}_edithash"
    key_editor = f"editor_forecast_{prev}"
    ui_snapshot_key = f"{key_mirror}__ui_snapshot"

    # 1) elegir fuente UI más fresca (editor -> snapshot -> mirror)
    df_ui = None
    if key_editor in st.session_state and isinstance(
        st.session_state[key_editor], pd.DataFrame
    ):
        df_ui = st.session_state[key_editor]
        print(f"[PRE-SWITCH] using editor DF for prev={prev} shape={df_ui.shape}")
    elif ui_snapshot_key in st.session_state and isinstance(
        st.session_state[ui_snapshot_key], pd.DataFrame
    ):
        df_ui = st.session_state[ui_snapshot_key]
        print(f"[PRE-SWITCH] using UI snapshot for prev={prev} shape={df_ui.shape}")
    elif key_mirror in st.session_state and isinstance(
        st.session_state[key_mirror], pd.DataFrame
    ):
        df_ui = st.session_state[key_mirror].reset_index()
        print(f"[PRE-SWITCH] using mirror for prev={prev} shape={df_ui.shape}")
    else:
        print(f"[PRE-SWITCH] skip prev={prev} (no df_ui disponible)")
        return

    # 2) buffer de staging
    if key_buffer not in st.session_state or not isinstance(
        st.session_state[key_buffer], pd.DataFrame
    ):
        print(f"[PRE-SWITCH] skip prev={prev} (no buffer staging)")
        return
    df_buffer = st.session_state[key_buffer].reset_index()

    # 3) sincronizar
    try:
        df_sync, changed = sincronizar_buffer_local(df_buffer, df_ui)
    except Exception as e:
        print(f"[PRE-SWITCH.ERROR] sincronizar: {e}")
        return

    if not changed:
        print(f"[PRE-SWITCH] prev={prev} sin cambios")
        return

    # 4) aplicar en sesión (buffer y mirror) + hash
    base_idx = [
        c
        for c in ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
        if c in df_sync.columns
    ]
    df_idx = df_sync.set_index(base_idx).sort_index()

    st.session_state[key_buffer] = df_idx
    st.session_state[key_mirror] = df_idx.copy()
    try:
        st.session_state[key_hash_edit] = hash_df(_canon_for_hash_global(df_sync))
    except Exception as e:
        print(f"[PRE-SWITCH.WARN] no se pudo setear hash edit: {e}")

    # marcar como editado (opcional)
    editados = st.session_state.get("clientes_editados", set())
    editados.add(prev)
    st.session_state["clientes_editados"] = editados

    print(f"[PRE-SWITCH] ✅ synced prev={prev} changed=True new_shape={df_sync.shape}")


def _on_cliente_change():
    """
    Callback del selectbox de cliente.
    - Sincroniza el cliente saliente (si hay cambios en UI).
    - No llama a st.rerun(); Streamlit ya re-ejecuta tras el cambio del widget.
    """
    _sync_prev_cliente_before_change()
    # El valor seleccionado ya está en st.session_state['cliente_selectbox'] por el propio widget.


# modulos/ventas.py
def _prefetch_vecinos(
    slpcode: int,
    opciones: list[str],
    current_idx: int,
    anio: int,
    db_fp: tuple[int, int, int, int, int, int],
) -> None:
    """
    Pre-carga en caché los clientes vecinos del select actual (prev/next) para
    que al navegar el editor cargue instantáneo.
    """
    try:
        n = len(opciones)
        if n == 0:
            return

        vecinos: list[str] = []
        # vecino anterior
        if current_idx - 1 >= 0:
            vecinos.append(opciones[current_idx - 1])
        # vecino siguiente
        if current_idx + 1 < n:
            vecinos.append(opciones[current_idx + 1])

        for v in vecinos:
            try:
                # Hacer 'warm-up' del caché
                _cached_obtener_forecast_editable(slpcode, v, anio, db_fp)
                st.session_state[f"_prefetch_{slpcode}_{anio}_{v}"] = True
                print(f"[PREFETCH] precargado forecast de vecino: {v}")
            except Exception as e:
                print(f"[PREFETCH][warn] fallo precarga para {v}: {e}")
    except Exception as e:
        print(f"[PREFETCH][fatal] {_prefetch_vecinos.__name__} -> {e}")


def _forecast_id_visible(
    slpcode: int, cardcode: str, anio: int, db_path: str
) -> int | None:
    # Prioriza el ID que se guardó en esta sesión
    skey = f"forecast_activo_{slpcode}_{cardcode}_{anio}"
    if skey in st.session_state:
        try:
            return int(st.session_state[skey])
        except Exception:
            pass

    ini = f"{anio:04d}-01-01"
    fin = f"{anio+1:04d}-01-01"
    q = """
        SELECT MAX(fd.ForecastID)
        FROM Forecast_Detalle fd
        JOIN Forecast f
          ON f.ForecastID = fd.ForecastID
        WHERE f.SlpCode   = ?
          AND fd.CardCode  = ?
          AND fd.FechEntr >= ?
          AND fd.FechEntr <  ?
    """
    with sqlite3.connect(db_path) as con:
        row = con.execute(q, (int(slpcode), cardcode, ini, fin)).fetchone()
    return int(row[0]) if row and row[0] is not None else None


# B_VFO001: Editor visual controlado y selección de cliente para forecast editable
# # ∂B_VFO001/∂B0
def vista_forecast(slpcode: int, _cardcode: str | None = None):
    """Editor 4×12 (Cantidad/Precio) con cache, buffer de sesión y guardado batch."""
    # ──────────────────────────── Trazas iniciales ────────────────────────────
    print(
        f"[DEBUG-VISTA-INIT] Iniciando vista_forecast - slpcode: {slpcode}, cardcode_arg: {_cardcode}"
    )
    print(
        f"[DEBUG-VISTA-INIT] Estado session_state keys: {list(st.session_state.keys())}"
    )

    # ──────────────────────────── HEADER UI ────────────────────────────
    st.markdown(
        """
        <style>
            .block-container { padding-top: 4rem !important; }
            .titulo-ajustado  { margin: .5rem 0 1rem; font-size: 1.2rem; font-weight: 500; }
        </style>
        <div class="titulo-ajustado">🧬 Editor Forecast Cantidad / Precio</div>
        """,
        unsafe_allow_html=True,
    )

    # ──────────────────────────── Invariantes / DB_PATH / Año ────────────────────────────
    if not isinstance(slpcode, int):
        st.error("Código de vendedor inválido en sesión.")
        st.stop()

    # DB_PATH seguro + fingerprint 6-tuple
    DB_PATH_local = get_db_path()
    db_fp = _db_fingerprint(DB_PATH_local)

    # Año de trabajo (int) con fallback
    try:
        anio = obtener_anio()
    except Exception:
        from datetime import datetime

        anio = datetime.now().year
    if not anio:
        from datetime import datetime

        anio = datetime.now().year
    anio = int(anio)

    # ──────────────────────────── Carga de clientes ────────────────────────────
    clientes = _cached_obtener_clientes(int(slpcode), db_fp)
    if clientes is None or getattr(clientes, "empty", True):
        st.info("Este vendedor no tiene clientes activos.")
        st.stop()

    if "CardCode" not in clientes.columns:
        st.error("Listado de clientes sin CardCode.")
        st.stop()

    opciones = clientes["CardCode"].astype(str).tolist()
    if not opciones:
        st.info("No hay clientes para seleccionar.")
        st.stop()

    # Selección persistente
    KEY_SEL = "cliente_selectbox"
    # columna nombre amigable (busca la primera que exista)
    _name_col = next(
        (c for c in ("Nombre", "CardName", "Cliente", "Name") if c in clientes.columns),
        None,
    )
    nombre_by_code = (
        dict(zip(clientes["CardCode"].astype(str), clientes[_name_col]))
        if _name_col
        else {c: c for c in opciones}
    )

    # Determinar cardcode inicial (argumento → último en sesión → primero)
    if KEY_SEL not in st.session_state or st.session_state[KEY_SEL] not in opciones:
        if isinstance(_cardcode, str) and _cardcode in opciones:
            st.session_state[KEY_SEL] = _cardcode
        elif st.session_state.get("_last_cliente_forecast") in opciones:
            st.session_state[KEY_SEL] = st.session_state["_last_cliente_forecast"]
        else:
            st.session_state[KEY_SEL] = opciones[0]

    col1, col2 = st.columns([2, 2])
    with col1:
        cardcode = st.selectbox(
            "Cliente:",
            opciones,
            key=KEY_SEL,
            format_func=lambda x: f"{x} - {nombre_by_code.get(x, '')}",
            help="La selección persiste entre reruns.",
            on_change=_on_cliente_change,  # tu callback
        )

        # Mostrar el último ForecastID visible (consultando DB)
        try:
            fid = _forecast_id_visible(
                int(slpcode), str(cardcode), int(anio), DB_PATH_local
            )
            if fid is not None:
                st.caption(f"Último ForecastID detectado → {fid}")
        except Exception as e:
            print(f"[FID][warn] {e}")

    # Persistir selección actual para próximos reruns
    st.session_state["_last_cliente_forecast"] = cardcode

    # Prefetch de vecinos (no debe romper si el helper no existe)
    try:
        current_idx = opciones.index(cardcode)
        _prefetch_vecinos(int(slpcode), opciones, current_idx, int(anio), db_fp)
    except Exception as e:
        print(f"[PREFETCH][warn] {e}")

    # ──────────────────────────── Obtener forecast base ────────────────────────────
    df_forecast = _cached_obtener_forecast_editable(
        int(slpcode), str(cardcode), int(anio), db_fp
    )
    if df_forecast is None or df_forecast.empty:
        st.info("⚠️ Forecast vacío para este cliente/año.")
        st.stop()

    # Hash canónico para detectar cambios de baseline
    def _canon_for_hash(df: pd.DataFrame) -> pd.DataFrame:
        df2 = df.copy()
        df2.columns = df2.columns.astype(str)
        base_idx = [
            c
            for c in ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]
            if c in df2.columns
        ]
        if base_idx:
            df2 = df2.set_index(base_idx)
        return df2.sort_index().sort_index(axis=1)

    key_buffer = f"forecast_buffer_{cardcode}"
    key_mirror = f"{key_buffer}_editado"
    key_hash_base = f"{key_buffer}_basehash"
    key_hash_edit = f"{key_buffer}_edithash"

    base_hash = hash_df(_canon_for_hash(df_forecast))
    need_init = (key_buffer not in st.session_state) or (
        st.session_state.get(key_hash_base) != base_hash
    )

    if need_init:
        inicializar_buffer_cliente(
            cliente=cardcode,
            df_base=df_forecast,
            moneda_default="CLP",
            precio_autofill_mode="strict",
            force_reinit=True,
            oitm_df=None,
        )
        st.session_state[key_hash_base] = base_hash
        if key_mirror not in st.session_state:
            st.session_state[key_mirror] = st.session_state[key_buffer].copy()
        # reset flags propios del cliente al cambiar baseline
        st.session_state.pop(f"_do_save_{cardcode}", None)
        st.session_state.pop(f"_saving_{cardcode}", None)
        print(
            f"[DEBUG-VISTA] Buffer inicializado/actualizado para cliente '{cardcode}' con clave '{key_buffer}'"
        )

    # ──────────────────────────── Tomar staging (buffer) y mirror ────────────────────────────
    if key_mirror in st.session_state and isinstance(
        st.session_state[key_mirror], pd.DataFrame
    ):
        df_src = st.session_state[key_mirror]
    else:
        df_src = st.session_state.get(key_buffer)

    if df_src is None or not isinstance(df_src, pd.DataFrame):
        st.error("No hay buffer en sesión para este cliente.")
        st.stop()

    df_buffer = st.session_state[key_buffer].reset_index()  # staging (guardable)
    df_mostrar = df_src.reset_index()  # visual (mirror)

    # ──────────────────────────── Filtro de producto ────────────────────────────
    with col2:
        itemcode_filtro = st.selectbox(
            "🔍 Filtrar producto:",
            ["Todos"] + sorted(df_mostrar["ItemCode"].astype(str).unique().tolist()),
            key=f"filtro_producto_{cardcode}",
        )

    if itemcode_filtro != "Todos":
        df_mostrar = df_mostrar[
            df_mostrar["ItemCode"].astype(str) == itemcode_filtro
        ].copy()
        df_buffer = df_buffer[
            df_buffer["ItemCode"].astype(str) == itemcode_filtro
        ].copy()

    # ──────────────────────────── Orden/columnas editor ────────────────────────────
    campos_fijos = [
        "ItemCode",
        "ItemName",
        "TipoForecast",
        "OcrCode3",
        "DocCur",
        "Métrica",
    ]
    presentes = [c for c in campos_fijos if c in df_mostrar.columns]
    columnas_ordenadas = presentes + [
        c for c in df_mostrar.columns if c not in presentes
    ]
    df_mostrar = df_mostrar[columnas_ordenadas].sort_values(
        ["ItemCode", "TipoForecast", "Métrica"], kind="stable"
    )

    # ──────────────────────────── Config Editor ────────────────────────────
    column_config_forecast = {
        "ItemCode": st.column_config.TextColumn(label="Cod"),
        "TipoForecast": st.column_config.TextColumn(label="Tipo"),
        "OcrCode3": st.column_config.TextColumn(label="Linea"),
        "DocCur": st.column_config.TextColumn(label="$"),
    }
    for mes in range(1, 13):
        col = f"{mes:02d}"
        column_config_forecast[col] = st.column_config.NumberColumn(
            label=col, disabled=False  # ajusta si quieres bloquear meses
        )

    # Alto en función del nº de filas, con un pequeño ajuste ("fudge") para evitar espacio extra
    rows = len(df_mostrar)

    ROW_PX = 36  # alto estimado por fila
    HEADER_PX = 40  # cabecera/toolbar aproximada
    FUDGE_PX = 12  # recorte para que no quede "una fila vacía"

    height_px = max(120, HEADER_PX + rows * ROW_PX - FUDGE_PX)

    df_editado = st.data_editor(
        df_mostrar,
        key=f"editor_forecast_{cardcode}",
        use_container_width=True,
        num_rows="fixed",
        height=height_px,
        column_order=columnas_ordenadas,
        column_config=column_config_forecast,
        hide_index=False,  # o True si prefieres ocultar el índice
    )

    # Snapshot de UI
    try:
        st.session_state[f"{key_mirror}__ui_snapshot"] = df_editado.copy()
    except Exception as _e:
        print(f"[UI-SNAPSHOT.WARN] {_e}")

    # ──────────────────────────── Sincronización de cambios ────────────────────────────
    df_actualizado, hay_cambios = sincronizar_buffer_local(df_buffer, df_editado)
    print(f"[DEBUG-VISTA] Cliente actual: {cardcode}")
    print(f"[DEBUG-VISTA] hay_cambios_real: {hay_cambios}")

    BASE_IDX = ["ItemCode", "TipoForecast", "Métrica", "OcrCode3"]

    # Estado de cambios/hash/validación (sin F841)
    prev_edit_hash = st.session_state.get(key_hash_edit)
    edit_hash = None
    valid_err = None
    changed = False

    if hay_cambios:
        try:
            _df_for_edit_hash = _canon_for_hash(df_actualizado)
            edit_hash = hash_df(_df_for_edit_hash)
            changed = prev_edit_hash != edit_hash
            # persistir el hash para este cliente en la sesión
            st.session_state[key_hash_edit] = edit_hash
        except Exception as e:
            print(f"[ERROR-VISTA] No se pudo calcular edit_hash: {e}")

        try:
            validar_forecast_dataframe(df_editado)
        except Exception as e:
            valid_err = str(e)

    if valid_err:
        st.error(f"❌ Error de validación estructural: {valid_err}")

    # ──────────────────────────── Botón Guardar ────────────────────────────
    save_flag_key = f"_do_save_{cardcode}"
    saving_key = f"_saving_{cardcode}"

    def _mark_do_save(flag_key=save_flag_key):
        st.session_state[flag_key] = True

    # Actualizar set de pendientes a partir del cambio real
    pend_set = set(st.session_state.get("clientes_editados", set()))
    if changed:
        pend_set.add(cardcode)
        st.session_state["clientes_editados"] = pend_set
    num_pend = len(pend_set)

    btn_label = (
        "💾 Guardar forecast en base de datos"
        if num_pend <= 1
        else f"💾 Guardar ({num_pend} clientes)"
    )

    st.button(
        btn_label,
        key=f"btn_save_{cardcode}",
        disabled=bool(valid_err)
        or st.session_state.get(saving_key, False)
        or st.session_state.get("_saving_global", False),
        on_click=_mark_do_save,
    )

    # ──────────────────────────── Guardado batch ────────────────────────────
    if st.session_state.get(save_flag_key):
        st.session_state.pop(save_flag_key, None)

        if st.session_state.get("_saving_global", False):
            st.warning("⏳ Guardado global en curso…")
            st.session_state[LAST_CLIENT_KEY] = cardcode
            return

        if st.session_state.get(saving_key):
            st.warning("⏳ Guardado en curso…")
            st.session_state[LAST_CLIENT_KEY] = cardcode
            return

        st.session_state[saving_key] = True
        try:
            print(
                f"[DEBUG-SAVE] Iniciando proceso de guardado (batch) cliente_actual={cardcode}"
            )

            df_to_save_actual = None
            if changed:
                base_idx_presentes = [
                    c for c in BASE_IDX if c in df_actualizado.columns
                ]
                if not base_idx_presentes:
                    raise ValueError(
                        "No se encontraron columnas base para indexar (ItemCode/TipoForecast/Métrica/OcrCode3)."
                    )
                df_to_save_actual = (
                    df_actualizado.set_index(base_idx_presentes)
                    .sort_index()
                    .reset_index()
                )

            res_batch = _guardar_ediciones_en_lote(
                slpcode=int(slpcode),
                anio=int(anio),
                db_path=DB_PATH_local,
                current_cardcode=str(cardcode),
                df_to_save_actual=df_to_save_actual,
            )

            if not isinstance(res_batch, dict):
                raise RuntimeError("Batch save devolvió un resultado inválido")

            oks = res_batch.get("oks", [])
            fails = res_batch.get("fails", [])

            if oks:
                st.success("✅ Guardado OK: " + ", ".join([r["cardcode"] for r in oks]))
            if fails:
                for r in fails:
                    st.error(
                        f"❌ {r.get('cardcode','?')}: {r.get('error') or 'Error desconocido'}"
                    )
            if not oks and not fails:
                st.info("No hay cambios por guardar.")

        except Exception as e:
            print(f"[ERROR-SAVE] {e}")
            st.error(f"❌ Error durante el guardado: {e}")
        finally:
            st.session_state[saving_key] = False

    # ──────────────────────────── Estado para próximos cambios ────────────────────────────
    st.session_state[LAST_CLIENT_KEY] = cardcode


# B_AYD001: Visualización de ayuda e Instrucciones para el usuario
# # ∂B_AYD001/∂B0
def vista_ayuda():
    st.markdown("### 🧠 Ayuda e Instrucciones")
    st.info(
        """
    - Puedes editar cantidades y precios directamente.
    - Usa los filtros de producto si hay muchos ítems.
    - Verifica que 'Firme' y 'Proyectado' estén bien separados.
    - Al finalizar, guarda los cambios desde el botón inferior.
    """
    )


# B_RUN001: Ejecutor principal con TABS (UX original)
# # ∂B_RUN001/∂B0
def run():
    print("[DEBUG-RUN] Iniciando función run()")

    # 0) hidratar SlpCode desde la URL si la sesión está vacía
    if "SlpCode" not in st.session_state:
        slp_from_qp = _slp_from_url()
        if slp_from_qp is not None:
            st.session_state["SlpCode"] = slp_from_qp

    # ✅ ÚNICA FUENTE efectiva
    slpcode = st.session_state.get("SlpCode")

    print(f"[DEBUG-RUN] Session state SlpCode: {slpcode}")
    if not isinstance(slpcode, int):
        st.error("No hay vendedor activo en la sesión. Vuelve al inicio y elige uno.")
        st.stop()

    # 🔗 asegura URL canónica ?modulo=ventas&vendedor=...
    ensure_url_reflects_ventas(slpcode)

    try:
        print("[DEBUG-RUN] Iniciando creación de tabs")
        tabs = st.tabs(
            ["Forecast", "Stock", "Histórico", "Ayuda", "Alertas", "Facturas"]
        )
        with tabs[0]:
            vista_forecast(slpcode)
        with tabs[1]:
            vista_stock(slpcode, None)
        with tabs[2]:
            vista_historico(slpcode, None)
        with tabs[3]:
            vista_ayuda()
        with tabs[4]:
            render_alertas_forecast(slpcode)
        with tabs[5]:
            mostrar_facturas()
    except BaseException as e:
        cls = e.__class__.__name__
        if cls in ("RerunException", "RerunData", "StopException"):
            print(f"[RUN.RERUN] controlado class={cls}")
            raise
        print(f"[RUN.ERROR] {cls}: {e}")
        st.error(f"No se pudo cargar la vista de ventas: {e}")
        st.stop()
