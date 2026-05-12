#!/usr/bin/env python
# coding: utf-8

"""
ETL incremental simulado para dw_universidad.

Este script no reconstruye todo el DWH ni ejecuta TRUNCATE. Simula una carga
incremental tomando registros nuevos de staging según `fecha_carga` y aplicando:

- Inserción incremental de Tiempo.
- SCD Tipo 2 básico para Alumno y Dictado.
- Inserción/actualización incremental de hechos con UPSERT cuando aplica.
- Remapeos de trazabilidad (estudiantes/inscripciones repetidas) y consolidación
    de intentos en exámenes cuando corresponde.

La transformación se reutiliza desde `TP2/2-ETL_CargaInicial/transformacion.py`
para mantener los mismos criterios de limpieza, normalización y validez.
"""

import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text

try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd().resolve()

PROJECT_TP2_DIR = SCRIPT_DIR.parent
CARGA_INICIAL_DIR = PROJECT_TP2_DIR / "2-ETL_CargaInicial"

if str(PROJECT_TP2_DIR) not in sys.path:
    sys.path.append(str(PROJECT_TP2_DIR))
if str(CARGA_INICIAL_DIR) not in sys.path:
    sys.path.append(str(CARGA_INICIAL_DIR))

import transformacion as base_etl
from logging_config import LoggerManager

logger = LoggerManager.configurar(
    "carga_incremental",
    ruta_raiz=str(SCRIPT_DIR),
    carpeta_logs="logs",
)

CONTROL_DIR = SCRIPT_DIR / "datos_control"
CONTROL_DIR.mkdir(exist_ok=True)
CONTROL_FILE = CONTROL_DIR / "ultima_extraccion.json"

LIMITE_DELTA_INICIAL = int(os.getenv("LIMITE_DELTA_INICIAL", "1000"))

TABLAS_STAGING = {
    "facultades": "stg_facultad",
    "departamentos": "stg_departamento",
    "programas": "stg_programa",
    "cursos": "stg_curso",
    "docentes": "stg_docente",
    "estudiantes": "stg_estudiante",
    "dictados": "stg_dictado",
    "inscripciones": "stg_inscripcion",
    "examenes": "stg_examen",
    "evaluaciones": "stg_evaluacion_curso",
}

TRANSFORMACIONES_BASE = {
    "facultades": base_etl.transformar_facultad_base,
    "departamentos": base_etl.transformar_departamento_base,
    "programas": base_etl.transformar_programa_base,
    "cursos": base_etl.transformar_curso_base,
    "docentes": base_etl.transformar_docente_base,
    "estudiantes": base_etl.transformar_estudiante_base,
    "dictados": base_etl.transformar_dictado_base,
    "inscripciones": base_etl.transformar_inscripcion_base,
    "examenes": base_etl.transformar_examen_base,
    "evaluaciones": base_etl.transformar_evaluacion_base,
}


def cargar_control() -> Dict:
    if not CONTROL_FILE.exists():
        return {
            "fecha_ultima_extraccion": None,
            "ejecuciones": [],
        }

    with CONTROL_FILE.open("r", encoding="utf-8") as archivo:
        return json.load(archivo)


def guardar_control(control: Dict) -> None:
    with CONTROL_FILE.open("w", encoding="utf-8") as archivo:
        json.dump(control, archivo, indent=2, ensure_ascii=False, default=str)


def leer_delta_staging(tabla: str, ultima_extraccion: Optional[str]) -> pd.DataFrame:
    if ultima_extraccion:
        query = text(
            f"SELECT * FROM {tabla} WHERE fecha_carga > :ultima ORDER BY row_id"
        )
        return pd.read_sql(
            query, con=base_etl.engine_stg, params={"ultima": ultima_extraccion}
        )

    # Primera ejecución incremental simulada: se toma una muestra reciente para no reprocesar todo.
    query = text(f"SELECT * FROM {tabla} ORDER BY row_id DESC LIMIT :limite")
    df = pd.read_sql(
        query, con=base_etl.engine_stg, params={"limite": LIMITE_DELTA_INICIAL}
    )
    return df.sort_values("row_id") if "row_id" in df.columns else df


def leer_staging_completo(tabla: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {tabla}", con=base_etl.engine_stg)


def limpiar_delta(clave: str, df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    funcion = TRANSFORMACIONES_BASE[clave]
    return funcion(df_raw)


def actualizar_mapeos_duplicados(
    deltas_raw: Dict[str, pd.DataFrame]
) -> Tuple[Dict[int, int], Dict[int, int]]:
    """
    Caja blanca: detección ACTIVA de duplicados en cada corrida incremental.

    Objetivo:
    - Evitar la ceguera ante nuevos IDs repetidos (DNI repetido en el origen).
    - Recalcular y persistir mapas de duplicados con datos completos + delta.

    Estrategia:
    1) Unimos staging completo con el delta actual (si existe) para asegurar
       que los duplicados nuevos queden visibles aunque el delta sea parcial.
    2) Construimos y persistimos la tabla `stg_estudiantes_repetidos`.
    3) Limpiamos inscripciones, remapeamos por estudiantes canónicos y
       construimos/persistimos `stg_inscripciones_repetidas`.
    4) Devolvemos ambos mapas para remapeos posteriores.
    """
    # 1) Estudiantes: preparar dataset mínimo para detección de duplicados.
    est_full = leer_staging_completo("stg_estudiante")
    est_delta = deltas_raw.get("estudiantes", pd.DataFrame())
    est_union = (
        pd.concat([est_full, est_delta], ignore_index=True, sort=False)
        if not est_delta.empty
        else est_full
    )

    estudiantes_mapeo = base_etl.preparar_estudiantes_para_mapeo_duplicados(est_union)
    dup_est = base_etl.construir_mapeo_estudiantes_duplicados(estudiantes_mapeo)
    base_etl.persistir_registros_estudiantes_duplicados(dup_est)
    mapa_estudiantes_dup = base_etl.obtener_mapa_estudiantes_duplicados()

    # 2) Inscripciones: limpiar staging completo y remapear id_estudiante.
    ins_full = leer_staging_completo("stg_inscripcion")
    ins_delta = deltas_raw.get("inscripciones", pd.DataFrame())
    ins_union = (
        pd.concat([ins_full, ins_delta], ignore_index=True, sort=False)
        if not ins_delta.empty
        else ins_full
    )
    ins_limpio, _ = base_etl.transformar_inscripcion_base(ins_union)
    if mapa_estudiantes_dup:
        ins_limpio, _ = base_etl.remapear_ids(
            ins_limpio,
            mapa_estudiantes_dup,
            "id_estudiante",
            etiqueta="inscripciones.id_estudiante",
        )

    # 3) Inscripciones duplicadas: solo para estudiantes canónicos duplicados.
    canonicos = set(mapa_estudiantes_dup.values()) if mapa_estudiantes_dup else set()
    dup_ins = base_etl.construir_mapeo_inscripciones_duplicadas(
        ins_limpio,
        estudiantes_canonicos_duplicados=canonicos,
    )
    base_etl.persistir_registros_inscripciones_duplicadas(dup_ins)
    mapa_inscripciones_dup = base_etl.obtener_mapa_inscripciones_duplicadas()

    return mapa_estudiantes_dup or {}, mapa_inscripciones_dup or {}


def normalizar_valor_bd(valor):
    if pd.isna(valor):
        return None
    if hasattr(valor, "item"):
        return valor.item()
    return valor


def dataframe_a_registros(df: pd.DataFrame) -> List[Dict]:
    registros = []
    for registro in df.to_dict(orient="records"):
        registros.append(
            {clave: normalizar_valor_bd(valor) for clave, valor in registro.items()}
        )
    return registros


def insert_ignore_dataframe(df: pd.DataFrame, tabla: str) -> int:
    if df.empty:
        return 0

    columnas = list(df.columns)
    columnas_sql = ", ".join(f"`{columna}`" for columna in columnas)
    parametros = [f"p{i}" for i, _ in enumerate(columnas)]
    valores_sql = ", ".join(f":{parametro}" for parametro in parametros)
    query = text(f"INSERT IGNORE INTO {tabla} ({columnas_sql}) VALUES ({valores_sql})")

    registros = []
    for registro in dataframe_a_registros(df):
        registros.append(
            {
                parametro: registro[columna]
                for parametro, columna in zip(parametros, columnas)
            }
        )
    with base_etl.engine_dwh.begin() as conn:
        resultado = conn.execute(query, registros)
        return int(resultado.rowcount or 0)


def upsert_dataframe(df: pd.DataFrame, tabla: str, columnas_update: List[str]) -> int:
    """
    Caja blanca: UPSERT genérico para hechos con clave única.

    - Inserta filas nuevas.
    - Si la clave única ya existe, actualiza columnas relevantes (por ejemplo,
      `nota` o `estado`) para no perder correcciones del sistema origen.

    Nota: usa `ON DUPLICATE KEY UPDATE`, por eso requiere una UNIQUE/PK en tabla.
    """
    if df.empty:
        return 0

    columnas = list(df.columns)
    columnas_sql = ", ".join(f"`{columna}`" for columna in columnas)
    parametros = [f"p{i}" for i, _ in enumerate(columnas)]
    valores_sql = ", ".join(f":{parametro}" for parametro in parametros)

    columnas_update_final = [c for c in columnas_update if c in columnas]
    if not columnas_update_final:
        return insert_ignore_dataframe(df, tabla)

    set_clause = ", ".join(
        f"`{columna}` = VALUES(`{columna}`)" for columna in columnas_update_final
    )
    query = text(
        f"INSERT INTO {tabla} ({columnas_sql}) VALUES ({valores_sql}) "
        f"ON DUPLICATE KEY UPDATE {set_clause}"
    )

    registros = []
    for registro in dataframe_a_registros(df):
        registros.append(
            {
                parametro: registro[columna]
                for parametro, columna in zip(parametros, columnas)
            }
        )

    with base_etl.engine_dwh.begin() as conn:
        resultado = conn.execute(query, registros)
        return int(resultado.rowcount or 0)


def insertar_tiempo_incremental(fechas: Iterable[Optional[date]]) -> int:
    dim_tiempo, _ = base_etl.construir_dim_tiempo(fechas)
    return insert_ignore_dataframe(dim_tiempo, "dim_tiempo")


def obtener_fila_actual(tabla: str, clave_natural: str, valor) -> Optional[Dict]:
    query = text(
        f"SELECT * FROM {tabla} WHERE {clave_natural} = :valor AND es_actual = TRUE LIMIT 1"
    )
    with base_etl.engine_dwh.connect() as conn:
        fila = (
            conn.execute(query, {"valor": normalizar_valor_bd(valor)})
            .mappings()
            .first()
        )
        return dict(fila) if fila else None


def expirar_dimension(tabla: str, sk_columna: str, sk_valor) -> None:
    query = text(
        f"UPDATE {tabla} SET valid_to = :valid_to, es_actual = FALSE "
        f"WHERE {sk_columna} = :sk_valor"
    )
    with base_etl.engine_dwh.begin() as conn:
        conn.execute(
            query, {"valid_to": date.today(), "sk_valor": normalizar_valor_bd(sk_valor)}
        )


def insertar_dimension(df: pd.DataFrame, tabla: str) -> int:
    if df.empty:
        return 0
    df.to_sql(
        name=tabla,
        con=base_etl.engine_dwh,
        if_exists="append",
        index=False,
        method="multi",
    )
    return len(df)


def valores_cambiaron(
    fila_actual: Dict, registro_nuevo: Dict, columnas_comparables: List[str]
) -> bool:
    for columna in columnas_comparables:
        actual = fila_actual.get(columna)
        nuevo = registro_nuevo.get(columna)
        if str(actual) != str(nuevo):
            return True
    return False


def actualizar_dimension_scd1(
    tabla: str, sk_columna: str, sk_valor, registro: Dict, columnas_scd1: List[str]
) -> None:
    columnas_presentes = [columna for columna in columnas_scd1 if columna in registro]
    if not columnas_presentes:
        return

    set_clause = ", ".join(
        f"`{columna}` = :{columna}" for columna in columnas_presentes
    )
    query = text(f"UPDATE {tabla} SET {set_clause} WHERE {sk_columna} = :sk_valor")
    parametros = {
        columna: normalizar_valor_bd(registro[columna])
        for columna in columnas_presentes
    }
    parametros["sk_valor"] = normalizar_valor_bd(sk_valor)

    with base_etl.engine_dwh.begin() as conn:
        conn.execute(query, parametros)


def aplicar_scd_estudiante(dim_estudiante_delta: pd.DataFrame) -> Dict[str, int]:
    columnas_scd2 = ["anio_plan_prog"]
    columnas_scd1 = [
        "genero",
        "egreso_carrera",
        "anio_egreso",
        "abandono_carrera",
        "anio_abandono",
    ]
    return aplicar_scd_generico(
        df_delta=dim_estudiante_delta,
        tabla="dim_estudiante",
        clave_natural="id_estudiante",
        sk_columna="estudiante_skey",
        columnas_scd2=columnas_scd2,
        columnas_scd1=columnas_scd1,
    )


def aplicar_scd_dictado(dim_dictado_delta: pd.DataFrame) -> Dict[str, int]:
    columnas_scd2 = [
        "periodo",
        "turno",
        "horas_teoria",
        "horas_practica",
        "horas_lab",
        "nivel_curso",
        "nombre_docente",
        "apellido_docente",
        "titulo_docente",
        "categoria_docente",
        "dedicacion_docente",
    ]
    columnas_scd1 = ["aula", "cupo_max", "nombre_curso"]
    return aplicar_scd_generico(
        df_delta=dim_dictado_delta,
        tabla="dim_dictado",
        clave_natural="id_dictado",
        sk_columna="dictado_skey",
        columnas_scd2=columnas_scd2,
        columnas_scd1=columnas_scd1,
    )


def aplicar_scd_generico(
    df_delta: pd.DataFrame,
    tabla: str,
    clave_natural: str,
    sk_columna: str,
    columnas_scd2: List[str],
    columnas_scd1: List[str],
) -> Dict[str, int]:
    insertados = 0
    actualizados_scd2 = 0
    actualizados_scd1 = 0
    sin_cambios = 0

    if df_delta.empty:
        return {
            "insertados": 0,
            "actualizados_scd2": 0,
            "actualizados_scd1": 0,
            "sin_cambios": 0,
        }

    for registro in dataframe_a_registros(df_delta):
        valor_clave = registro[clave_natural]
        fila_actual = obtener_fila_actual(tabla, clave_natural, valor_clave)
        registro_df = pd.DataFrame([registro])

        if fila_actual is None:
            insertados += insertar_dimension(registro_df, tabla)
            continue

        if valores_cambiaron(fila_actual, registro, columnas_scd2):
            expirar_dimension(tabla, sk_columna, fila_actual[sk_columna])
            insertados += insertar_dimension(registro_df, tabla)
            actualizados_scd2 += 1
        elif valores_cambiaron(fila_actual, registro, columnas_scd1):
            actualizar_dimension_scd1(
                tabla, sk_columna, fila_actual[sk_columna], registro, columnas_scd1
            )
            actualizados_scd1 += 1
        else:
            sin_cambios += 1

    return {
        "insertados": insertados,
        "actualizados_scd2": actualizados_scd2,
        "actualizados_scd1": actualizados_scd1,
        "sin_cambios": sin_cambios,
    }


def construir_lookups_completos() -> Dict[str, pd.DataFrame]:
    lookups = {}
    for clave in [
        "facultades",
        "departamentos",
        "programas",
        "cursos",
        "docentes",
        "dictados",
    ]:
        df_raw = leer_staging_completo(TABLAS_STAGING[clave])
        lookups[clave], _ = limpiar_delta(clave, df_raw)
    return lookups


def obtener_historial_examenes(
    pares: pd.DataFrame, tamanio_lote: int = 500
) -> Dict[Tuple[int, int], Dict[str, int]]:
    """
    Caja blanca: consulta el DWH para conocer intentos previos por
    (alumnoSKey, dictadoSKey). Esto evita validar exámenes solo con el delta.
    """
    if pares.empty:
        return {}

    historial: Dict[Tuple[int, int], Dict[str, int]] = {}
    pares_unicos = pares.drop_duplicates().astype(int)
    total = len(pares_unicos)

    for i in range(0, total, tamanio_lote):
        bloque = pares_unicos.iloc[i : i + tamanio_lote]
        valores = ", ".join(
            f"({int(row.alumnoSKey)}, {int(row.dictadoSKey)})" for row in bloque.itertuples()
        )
        if not valores:
            continue

        query = text(
            "SELECT alumnoSKey, dictadoSKey, COUNT(*) AS intentos, "
            "MAX(aprobado) AS aprobado "
            "FROM fact_examen_estudiante "
            f"WHERE (alumnoSKey, dictadoSKey) IN ({valores}) "
            "GROUP BY alumnoSKey, dictadoSKey"
        )
        df_hist = pd.read_sql(query, con=base_etl.engine_dwh)
        for row in df_hist.itertuples():
            historial[(int(row.alumnoSKey), int(row.dictadoSKey))] = {
                "intentos": int(row.intentos or 0),
                "aprobado": int(row.aprobado or 0),
            }

    return historial


def filtrar_examenes_por_historial(
    examenes: pd.DataFrame,
    inscripciones: pd.DataFrame,
    mapa_estudiante: Dict[int, int],
    mapa_dictado: Dict[int, int],
    max_intentos: int = 3,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Caja blanca: aplica la regla de intentos con contexto histórico del DWH.

    - Si el alumno ya aprobó, no se aceptan nuevos intentos.
    - Si ya tiene 3 intentos, se rechazan nuevos.
    - Si hay lugar, se aceptan intentos en orden cronológico del delta,
      cortando al primer aprobado.
    """
    if examenes.empty:
        return examenes.copy(), {"aceptados": 0, "rechazados": 0}

    ins_map = inscripciones[["id_inscripcion", "id_estudiante", "id_dictado"]].drop_duplicates()
    df = examenes.merge(ins_map, on="id_inscripcion", how="left")
    df["alumnoSKey"] = df["id_estudiante"].map(mapa_estudiante)
    df["dictadoSKey"] = df["id_dictado"].map(mapa_dictado)

    validos = df["alumnoSKey"].notna() & df["dictadoSKey"].notna()
    df_validos = df[validos].copy()
    pares = df_validos[["alumnoSKey", "dictadoSKey"]].drop_duplicates()
    historial = obtener_historial_examenes(pares)

    aceptados = []
    rechazados = 0
    columnas_originales = list(examenes.columns)

    for (alumno_skey, dictado_skey), grupo in df_validos.groupby(
        ["alumnoSKey", "dictadoSKey"], sort=False
    ):
        info = historial.get((int(alumno_skey), int(dictado_skey)), {"intentos": 0, "aprobado": 0})
        if info.get("aprobado", 0):
            rechazados += len(grupo)
            continue

        intentos_previos = int(info.get("intentos", 0))
        disponibles = max_intentos - intentos_previos
        if disponibles <= 0:
            rechazados += len(grupo)
            continue

        # Caja blanca: el numero_intento real debe continuar desde el historial
        # del DWH para no pisar intentos previos en el UPSERT.
        intento_real_actual = intentos_previos + 1

        grupo = grupo.sort_values(["fecha", "id_examen"])
        for _, fila in grupo.iterrows():
            if disponibles <= 0:
                rechazados += 1
                continue

            fila_corregida = fila[columnas_originales].copy()
            fila_corregida["numero_intento"] = intento_real_actual
            aceptados.append(fila_corregida)
            disponibles -= 1
            intento_real_actual += 1

            if str(fila.get("resultado", "")).strip().lower() == "aprobado":
                # Si aprueba en el delta, no se permiten intentos posteriores.
                disponibles = 0

    if not aceptados:
        return examenes.iloc[0:0].copy(), {"aceptados": 0, "rechazados": rechazados}

    resultado = pd.DataFrame(aceptados, columns=columnas_originales)
    return resultado, {"aceptados": len(resultado), "rechazados": rechazados}


def procesar_incremental() -> Dict:
    """
    Caja blanca: orquesta la carga incremental por delta.
    1) Detecta cambios por fecha_carga.
    2) Limpia y normaliza con la misma lógica del ETL base.
    3) Aplica trazabilidad leyendo mapas persistidos de duplicados.
    4) Actualiza dimensiones con SCD1/SCD2.
    5) Inserta hechos con INSERT IGNORE y consolida exámenes cuando aplica.
    """
    print("\n=== Carga incremental simulada STG -> DWH ===")
    control = cargar_control()
    ultima = control.get("fecha_ultima_extraccion")
    inicio_ejecucion = datetime.now(timezone.utc).isoformat()

    print(f"Última extracción registrada: {ultima or 'sin registro previo'}")

    deltas_raw: Dict[str, pd.DataFrame] = {}
    deltas_limpios: Dict[str, pd.DataFrame] = {}
    stats_limpieza: Dict[str, Dict] = {}

    print("[1/4] Detectando deltas en staging...")
    for clave, tabla in TABLAS_STAGING.items():
        df_delta = leer_delta_staging(tabla, ultima)
        deltas_raw[clave] = df_delta
        if not df_delta.empty:
            print(f"  {tabla}: {len(df_delta)} registros delta")

    if all(df.empty for df in deltas_raw.values()):
        print("No se detectaron cambios para procesar.")
        control["fecha_ultima_extraccion"] = inicio_ejecucion
        control["ejecuciones"].append({"fecha": inicio_ejecucion, "cambios": 0})
        guardar_control(control)
        return {"cambios": 0}

    print("[2/4] Limpiando y normalizando deltas...")
    for clave, df_raw in deltas_raw.items():
        if df_raw.empty:
            deltas_limpios[clave] = pd.DataFrame()
            continue
        deltas_limpios[clave], stats = limpiar_delta(clave, df_raw)
        stats_limpieza[clave] = stats
        if stats["rechazados"] > 0 or stats["duplicados"] > 0:
            print(
                f"  Atención {TABLAS_STAGING[clave]}: rechazados={stats['rechazados']} | duplicados={stats['duplicados']}"
            )

    # Paso de trazabilidad ACTIVA: recalcula mapas de duplicados con datos completos.
    mapa_estudiantes_dup, mapa_inscripciones_dup = actualizar_mapeos_duplicados(deltas_raw)

    # Remapeo inmediato del delta de inscripciones para mantener coherencia en hechos.
    if mapa_estudiantes_dup and not deltas_limpios.get("inscripciones", pd.DataFrame()).empty:
        deltas_limpios["inscripciones"], _ = base_etl.remapear_ids(
            deltas_limpios["inscripciones"],
            mapa_estudiantes_dup,
            "id_estudiante",
            etiqueta="inscripciones.id_estudiante",
        )

    lookups = construir_lookups_completos()

    print("[3/4] Aplicando dimensiones incrementales...")
    fechas_tiempo = []
    if not deltas_limpios.get("inscripciones", pd.DataFrame()).empty:
        fechas_tiempo.extend(
            deltas_limpios["inscripciones"]["fecha_inscripcion"].tolist()
        )
    if not deltas_limpios.get("examenes", pd.DataFrame()).empty:
        fechas_tiempo.extend(deltas_limpios["examenes"]["fecha"].tolist())
    if not deltas_limpios.get("evaluaciones", pd.DataFrame()).empty:
        fechas_tiempo.extend(
            deltas_limpios["evaluaciones"]["fecha_evaluacion"].tolist()
        )

    tiempo_insertados = insertar_tiempo_incremental(fechas_tiempo)

    dim_estudiante_delta = pd.DataFrame()
    if not deltas_limpios.get("estudiantes", pd.DataFrame()).empty:
        dim_estudiante_delta, _ = base_etl.construir_dim_estudiante(
            deltas_limpios["estudiantes"], lookups["programas"]
        )
    scd_estudiante = aplicar_scd_estudiante(dim_estudiante_delta)

    dim_dictado_delta = pd.DataFrame()
    if not deltas_limpios.get("dictados", pd.DataFrame()).empty:
        dim_dictado_delta, _ = base_etl.construir_dim_dictado(
            deltas_limpios["dictados"],
            lookups["cursos"],
            lookups["docentes"],
            lookups["departamentos"],
            lookups["facultades"],
        )
    scd_dictado = aplicar_scd_dictado(dim_dictado_delta)

    print(
        f"  Tiempo insertados={tiempo_insertados} | "
        f"Estudiante nuevos={scd_estudiante['insertados']} scd2={scd_estudiante['actualizados_scd2']} scd1={scd_estudiante['actualizados_scd1']} | "
        f"Dictado nuevos={scd_dictado['insertados']} scd2={scd_dictado['actualizados_scd2']} scd1={scd_dictado['actualizados_scd1']}"
    )

    print("[4/4] Insertando hechos incrementales...")
    mapa_estudiante = base_etl.obtener_mapa_estudiante()
    mapa_dictado = base_etl.obtener_mapa_dictado()
    mapa_tiempo = base_etl.obtener_mapa_tiempo()

    hechos_insertados = {
        "fact_inscripcion": 0,
        "fact_examen_estudiante": 0,
        "fact_evaluacion_dictado": 0,
    }

    if not deltas_limpios.get("inscripciones", pd.DataFrame()).empty:
        # Construye el fact con la firma correcta (4 argumentos).
        fact_inscripcion, _ = base_etl.construir_fact_inscripcion(
            deltas_limpios["inscripciones"],
            mapa_estudiante,
            mapa_dictado,
            mapa_tiempo,
        )
        # UPSERT para mantener consistencia si el estado cambia.
        # Nota: NO se actualiza tiempoSKey para no perder la fecha original.
        hechos_insertados["fact_inscripcion"] = upsert_dataframe(
            fact_inscripcion,
            "fact_inscripcion",
            columnas_update=["estado", "abandono"],
        )

    if not deltas_limpios.get("examenes", pd.DataFrame()).empty:
        inscripciones_base = deltas_limpios.get("inscripciones")
        if inscripciones_base is None or inscripciones_base.empty:
            inscripciones_raw = leer_staging_completo("stg_inscripcion")
            inscripciones_base, _ = base_etl.transformar_inscripcion_base(
                inscripciones_raw
            )
            if mapa_estudiantes_dup:
                inscripciones_base, _ = base_etl.remapear_ids(
                    inscripciones_base,
                    mapa_estudiantes_dup,
                    "id_estudiante",
                    etiqueta="inscripciones.id_estudiante",
                )

        # Remapeo de id_inscripcion en exámenes usando la tabla persistida.
        if mapa_inscripciones_dup:
            deltas_limpios["examenes"], _ = base_etl.remapear_ids(
                deltas_limpios["examenes"],
                mapa_inscripciones_dup,
                "id_inscripcion",
                etiqueta="examenes.id_inscripcion",
            )

            # Consolidación local: solo para casos impactados por duplicados.
            deltas_limpios["examenes"], _ = base_etl.consolidar_examenes_duplicados(
                deltas_limpios["examenes"],
                inscripciones_base if inscripciones_base is not None else pd.DataFrame(),
                mapa_inscripciones_dup,
            )

        # Validación con contexto histórico del DWH (regla de intentos).
        deltas_limpios["examenes"], _ = filtrar_examenes_por_historial(
            deltas_limpios["examenes"],
            inscripciones_base if inscripciones_base is not None else pd.DataFrame(),
            mapa_estudiante,
            mapa_dictado,
            max_intentos=3,
        )

        fact_examen, _ = base_etl.construir_fact_examen_estudiante(
            deltas_limpios["examenes"],
            inscripciones_base,
            mapa_estudiante,
            mapa_dictado,
            mapa_tiempo,
        )
        # UPSERT para reflejar correcciones de nota/aprobado.
        # Nota: NO se actualiza tiempoSKey para no alterar la fecha original.
        hechos_insertados["fact_examen_estudiante"] = upsert_dataframe(
            fact_examen,
            "fact_examen_estudiante",
            columnas_update=["nota", "aprobado"],
        )

    if not deltas_limpios.get("evaluaciones", pd.DataFrame()).empty:
        fact_evaluacion, _ = base_etl.construir_fact_evaluacion_dictado(
            deltas_limpios["evaluaciones"], mapa_dictado, mapa_tiempo
        )
        hechos_insertados["fact_evaluacion_dictado"] = insert_ignore_dataframe(
            fact_evaluacion, "fact_evaluacion_dictado"
        )

    print(
        f"  fact_inscripcion={hechos_insertados['fact_inscripcion']} | "
        f"fact_examen_estudiante={hechos_insertados['fact_examen_estudiante']} | "
        f"fact_evaluacion_dictado={hechos_insertados['fact_evaluacion_dictado']}"
    )

    total_delta = sum(len(df) for df in deltas_raw.values())
    control["fecha_ultima_extraccion"] = inicio_ejecucion
    control["ejecuciones"].append(
        {
            "fecha": inicio_ejecucion,
            "fecha_anterior": ultima,
            "registros_delta": total_delta,
            "tiempo_insertados": tiempo_insertados,
            "scd_estudiante": scd_estudiante,
            "scd_dictado": scd_dictado,
            "hechos_insertados": hechos_insertados,
        }
    )
    guardar_control(control)

    print("\n[OK] Carga incremental simulada finalizada")
    print(f"Control actualizado en: {CONTROL_FILE}")

    return {
        "registros_delta": total_delta,
        "tiempo_insertados": tiempo_insertados,
        "scd_estudiante": scd_estudiante,
        "scd_dictado": scd_dictado,
        "hechos_insertados": hechos_insertados,
    }


if __name__ == "__main__":
    procesar_incremental()