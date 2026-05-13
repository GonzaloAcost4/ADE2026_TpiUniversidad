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

import importlib.util
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

from logging_config import LoggerManager

# Carga dinamica para evitar error de resolucion en el import de transformacion.
_transformacion_path = CARGA_INICIAL_DIR / "transformacion.py"
_transformacion_spec = importlib.util.spec_from_file_location(
    "transformacion", _transformacion_path
)
if _transformacion_spec is None or _transformacion_spec.loader is None:
    raise ImportError(f"No se pudo cargar transformacion desde {_transformacion_path}")
base_etl = importlib.util.module_from_spec(_transformacion_spec)
_transformacion_spec.loader.exec_module(base_etl)

LoggerManager.reiniciar()
logger = LoggerManager.configurar(
    "CargaIncremental",
    ruta_raiz=str(SCRIPT_DIR),
    carpeta_logs="logs",
)

AUDITORIA_TABLE = "etl_auditoria_incremental"

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


def asegurar_tabla_auditoria() -> None:
    """
    Caja blanca: crea la tabla de auditoria si no existe.
    Mantiene la marca de agua en la BD para evitar inconsistencias
    por fallos entre escritura del DWH y archivos locales.
    """
    query = text(
        f"""
        CREATE TABLE IF NOT EXISTS {AUDITORIA_TABLE} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            inicio DATETIME NOT NULL,
            fin DATETIME NULL,
            ultima_extraccion DATETIME NULL,
            nueva_extraccion DATETIME NULL,
            estado VARCHAR(20) NOT NULL,
            registros_delta INT DEFAULT 0,
            mensaje_error TEXT NULL
        ) ENGINE=InnoDB
        """
    )
    with base_etl.engine_stg.begin() as conn:
        conn.execute(query)


def obtener_ultima_extraccion() -> Optional[str]:
    """
    Caja blanca: obtiene la ultima marca de agua exitosa desde auditoria.
    """
    asegurar_tabla_auditoria()
    query = text(
        f"SELECT nueva_extraccion FROM {AUDITORIA_TABLE} "
        "WHERE estado = 'OK' ORDER BY id DESC LIMIT 1"
    )
    with base_etl.engine_stg.connect() as conn:
        valor = conn.execute(query).scalar()
    return valor.isoformat() if valor else None


def registrar_inicio_ejecucion(ultima_extraccion: Optional[str]) -> int:
    """
    Caja blanca: registra el inicio de la ejecucion incremental en auditoria.
    """
    asegurar_tabla_auditoria()
    query = text(
        f"INSERT INTO {AUDITORIA_TABLE} "
        "(inicio, ultima_extraccion, estado) VALUES (:inicio, :ultima, 'RUNNING')"
    )
    with base_etl.engine_stg.begin() as conn:
        result = conn.execute(
            query, {"inicio": datetime.now(), "ultima": ultima_extraccion}
        )
        return int(result.lastrowid)


def registrar_fin_ejecucion(
    ejecucion_id: int,
    nueva_extraccion: str,
    registros_delta: int,
    estado: str,
    mensaje_error: Optional[str] = None,
) -> None:
    """
    Caja blanca: cierra la ejecucion incremental en auditoria.
    """
    query = text(
        f"UPDATE {AUDITORIA_TABLE} "
        "SET fin = :fin, nueva_extraccion = :nueva, registros_delta = :delta, "
        "estado = :estado, mensaje_error = :error "
        "WHERE id = :id"
    )
    with base_etl.engine_stg.begin() as conn:
        conn.execute(
            query,
            {
                "fin": datetime.now(),
                "nueva": nueva_extraccion,
                "delta": registros_delta,
                "estado": estado,
                "error": mensaje_error,
                "id": ejecucion_id,
            },
        )


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


def actualizar_mapeos_duplicados() -> Tuple[Dict[int, int], Dict[int, int]]:
    """
    Caja blanca: detección ACTIVA de duplicados en cada corrida incremental.

    Objetivo:
    - Evitar la ceguera ante nuevos IDs repetidos (DNI repetido en el origen).
    - Recalcular y persistir mapas de duplicados con datos completos + delta.

    Estrategia:
    1) Detectar duplicados por DNI directamente en SQL y persistir.
    2) Detectar duplicados de inscripcion remapeando IDs en SQL y persistir.
    3) Leer mapas desde STG para remapeos posteriores.
    """
    query_estudiantes_trunc = text("TRUNCATE TABLE stg_estudiantes_repetidos")
    query_estudiantes_insert = text(
        """
        INSERT INTO stg_estudiantes_repetidos (archivo_origen, id_repetido, id_tomado)
        SELECT b.archivo_origen,
               b.id_estudiante AS id_repetido,
               t.id_tomado
        FROM (
            SELECT
                se.archivo_origen,
                CAST(se.id_estudiante_raw AS SIGNED) AS id_estudiante,
                CAST(se.dni_raw AS SIGNED) AS dni
            FROM stg_estudiante se
            WHERE se.id_estudiante_raw REGEXP '^[0-9]+$'
              AND se.dni_raw REGEXP '^[0-9]+$'
              AND CAST(se.dni_raw AS SIGNED) BETWEEN 1000000 AND 99999999
        ) b
        JOIN (
            SELECT dni, MIN(id_estudiante) AS id_tomado
            FROM (
                SELECT
                    CAST(se.dni_raw AS SIGNED) AS dni,
                    CAST(se.id_estudiante_raw AS SIGNED) AS id_estudiante
                FROM stg_estudiante se
                WHERE se.id_estudiante_raw REGEXP '^[0-9]+$'
                  AND se.dni_raw REGEXP '^[0-9]+$'
                  AND CAST(se.dni_raw AS SIGNED) BETWEEN 1000000 AND 99999999
            ) x
            GROUP BY dni
        ) t ON b.dni = t.dni
        WHERE b.id_estudiante <> t.id_tomado
        """
    )

    base_inscripciones = """
        SELECT
            si.archivo_origen,
            CAST(si.id_inscripcion_raw AS SIGNED) AS id_inscripcion,
            COALESCE(CAST(sr.id_tomado AS SIGNED), CAST(si.id_estudiante_raw AS SIGNED)) AS id_estudiante_canon,
            CAST(si.id_dictado_raw AS SIGNED) AS id_dictado
        FROM stg_inscripcion si
        LEFT JOIN stg_estudiantes_repetidos sr
            ON CAST(si.id_estudiante_raw AS SIGNED) = CAST(sr.id_repetido AS SIGNED)
        WHERE si.id_inscripcion_raw REGEXP '^[0-9]+$'
          AND si.id_estudiante_raw REGEXP '^[0-9]+$'
          AND si.id_dictado_raw REGEXP '^[0-9]+$'
    """

    query_inscripciones_trunc = text("TRUNCATE TABLE stg_inscripciones_repetidas")
    query_inscripciones_insert = text(
        f"""
        INSERT INTO stg_inscripciones_repetidas (archivo_origen, id_repetido, id_tomado)
        SELECT b.archivo_origen,
               b.id_inscripcion AS id_repetido,
               t.id_tomado
        FROM (
            {base_inscripciones}
        ) b
        JOIN (
            SELECT id_estudiante_canon, id_dictado, MIN(id_inscripcion) AS id_tomado
            FROM (
                {base_inscripciones}
            ) x
            GROUP BY id_estudiante_canon, id_dictado
        ) t
        ON b.id_estudiante_canon = t.id_estudiante_canon
       AND b.id_dictado = t.id_dictado
        WHERE b.id_inscripcion <> t.id_tomado
        """
    )

    with base_etl.engine_stg.begin() as conn:
        conn.execute(query_estudiantes_trunc)
        conn.execute(query_estudiantes_insert)
        conn.execute(query_inscripciones_trunc)
        conn.execute(query_inscripciones_insert)

    mapa_estudiantes_dup = base_etl.leer_mapeo_duplicados("stg_estudiantes_repetidos")
    mapa_inscripciones_dup = base_etl.leer_mapeo_duplicados(
        "stg_inscripciones_repetidas"
    )

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

        # Normalizar nulos (por si pandas dejó algún NaN en el dict)
        if pd.isna(actual):
            actual = None
        if pd.isna(nuevo):
            nuevo = None

        if actual == nuevo:
            continue
        if actual is None and nuevo is None:
            continue

        # Si uno es nulo y el otro no, cambiaron
        if actual is None or nuevo is None:
            return True

        # Comparación robusta para números (evita que 5.0 == 5 de False por ser str)
        try:
            if float(actual) == float(nuevo):
                continue
        except (ValueError, TypeError):
            pass

        # Fallback a string
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
    columnas_scd2 = [
        "nombrePrograma",
        "tipoPrograma",
        "duracionAniosPrograma",
        "anioPlanPrograma",
    ]
    columnas_scd1 = [
        "genero",
        "egresoCarrera",
        "anioEgreso",
        "abandonoCarrera",
        "anioAbandono",
    ]
    return aplicar_scd_generico(
        df_delta=dim_estudiante_delta,
        tabla="dim_estudiante",
        clave_natural="idalumno",
        sk_columna="alumnoSKey",
        columnas_scd2=columnas_scd2,
        columnas_scd1=columnas_scd1,
    )


def aplicar_scd_dictado(dim_dictado_delta: pd.DataFrame) -> Dict[str, int]:
    columnas_scd2 = [
        "periodo",
        "turno",
        "horasTeoCurso",
        "horasPracCurso",
        "horasLabCurso",
        "nivelCurso",
        "nombreDocente",
        "apellidoDocente",
        "tituloDocente",
        "categoriaDocente",
        "dedicacionDocente",
    ]
    columnas_scd1 = ["aula", "cupoMax", "nombreCurso"]
    return aplicar_scd_generico(
        df_delta=dim_dictado_delta,
        tabla="dim_dictado",
        clave_natural="idDictado",
        sk_columna="dictadoSKey",
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
            f"({int(row.alumnoSKey)}, {int(row.dictadoSKey)})"
            for row in bloque.itertuples()
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

    ins_map = (
        inscripciones[["id_inscripcion", "id_estudiante", "id_dictado"]]
        .drop_duplicates()
        .copy()
    )
    ins_map["id_inscripcion"] = ins_map["id_inscripcion"].astype("Int64")
    df = examenes.copy()
    df["id_inscripcion"] = df["id_inscripcion"].astype("Int64")
    df = df.merge(ins_map, on="id_inscripcion", how="left")
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
        info = historial.get(
            (int(alumno_skey), int(dictado_skey)), {"intentos": 0, "aprobado": 0}
        )
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
    3) Recalcula trazabilidad en SQL y lee mapas persistidos de duplicados.
    4) Actualiza dimensiones con SCD1/SCD2.
    5) Inserta hechos con INSERT IGNORE y consolida exámenes cuando aplica.
    """
    logger.info("=== Carga incremental simulada STG -> DWH ===")
    ultima = obtener_ultima_extraccion()
    inicio_ejecucion = datetime.now().isoformat()
    ejecucion_id = registrar_inicio_ejecucion(ultima)

    try:
        logger.info(f"Última extracción registrada: {ultima or 'sin registro previo'}")

        deltas_raw: Dict[str, pd.DataFrame] = {}
        deltas_limpios: Dict[str, pd.DataFrame] = {}
        stats_limpieza: Dict[str, Dict] = {}

        logger.info("[1/4] Detectando deltas en staging...")
        for clave, tabla in TABLAS_STAGING.items():
            df_delta = leer_delta_staging(tabla, ultima)
            deltas_raw[clave] = df_delta
            if not df_delta.empty:
                logger.info(f"  {tabla}: {len(df_delta)} registros delta")

        if all(df.empty for df in deltas_raw.values()):
            logger.info("No se detectaron cambios para procesar.")
            registrar_fin_ejecucion(ejecucion_id, inicio_ejecucion, 0, "OK")
            return {"cambios": 0}

        logger.info("[2/4] Limpiando y normalizando deltas...")
        for clave, df_raw in deltas_raw.items():
            if df_raw.empty:
                deltas_limpios[clave] = pd.DataFrame()
                continue
            deltas_limpios[clave], stats = limpiar_delta(clave, df_raw)
            stats_limpieza[clave] = stats
            if stats["rechazados"] > 0 or stats["duplicados"] > 0:
                logger.warning(
                    f"  Atención {TABLAS_STAGING[clave]}: rechazados={stats['rechazados']} | duplicados={stats['duplicados']}"
                )

        # Paso de trazabilidad ACTIVA: recalcula mapas de duplicados con datos completos.
        mapa_estudiantes_dup, mapa_inscripciones_dup = actualizar_mapeos_duplicados()

        # Filtrar estudiantes duplicados para no crear nuevas dimensiones falsas
        if (
            mapa_estudiantes_dup
            and not deltas_limpios.get("estudiantes", pd.DataFrame()).empty
        ):
            df_est = deltas_limpios["estudiantes"]
            ids_repetidos = list(mapa_estudiantes_dup.keys())
            deltas_limpios["estudiantes"] = df_est[
                ~df_est["id_estudiante"].isin(ids_repetidos)
            ].copy()

        # Remapeo inmediato del delta de inscripciones para mantener coherencia en hechos.
        if (
            mapa_estudiantes_dup
            and not deltas_limpios.get("inscripciones", pd.DataFrame()).empty
        ):
            deltas_limpios["inscripciones"], _ = base_etl.remapear_ids(
                deltas_limpios["inscripciones"],
                mapa_estudiantes_dup,
                "id_estudiante",
                etiqueta="inscripciones.id_estudiante",
            )

        # Filtrar inscripciones duplicadas
        if (
            mapa_inscripciones_dup
            and not deltas_limpios.get("inscripciones", pd.DataFrame()).empty
        ):
            df_ins = deltas_limpios["inscripciones"]
            ids_ins_repetidos = list(mapa_inscripciones_dup.keys())
            deltas_limpios["inscripciones"] = df_ins[
                ~df_ins["id_inscripcion"].isin(ids_ins_repetidos)
            ].copy()

        # Remapear id_inscripcion en exámenes
        if (
            mapa_inscripciones_dup
            and not deltas_limpios.get("examenes", pd.DataFrame()).empty
        ):
            deltas_limpios["examenes"], _ = base_etl.remapear_ids(
                deltas_limpios["examenes"],
                mapa_inscripciones_dup,
                "id_inscripcion",
                etiqueta="examenes.id_inscripcion",
            )

        lookups = construir_lookups_completos()

        logger.info("[3/4] Aplicando dimensiones incrementales...")
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

        logger.info(
            f"  Tiempo insertados={tiempo_insertados} | "
            f"Estudiante nuevos={scd_estudiante['insertados']} scd2={scd_estudiante['actualizados_scd2']} scd1={scd_estudiante['actualizados_scd1']} | "
            f"Dictado nuevos={scd_dictado['insertados']} scd2={scd_dictado['actualizados_scd2']} scd1={scd_dictado['actualizados_scd1']}"
        )

        logger.info("[4/4] Insertando hechos incrementales...")
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
                ids_necesarios = [
                    int(x)
                    for x in deltas_limpios["examenes"]["id_inscripcion"]
                    .dropna()
                    .unique()
                ]
                ids_str = ",".join(f"'{id}'" for id in ids_necesarios)
                query = text(
                    f"SELECT * FROM stg_inscripcion WHERE id_inscripcion_raw IN ({ids_str})"
                )
                inscripciones_raw = pd.read_sql(query, con=base_etl.engine_stg)
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
                    inscripciones_base
                    if inscripciones_base is not None
                    else pd.DataFrame(),
                    mapa_inscripciones_dup,
                )

            # Validación con contexto histórico del DWH (regla de intentos).
            deltas_limpios["examenes"], _ = filtrar_examenes_por_historial(
                deltas_limpios["examenes"],
                inscripciones_base
                if inscripciones_base is not None
                else pd.DataFrame(),
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

        logger.info(
            f"  fact_inscripcion={hechos_insertados['fact_inscripcion']} | "
            f"fact_examen_estudiante={hechos_insertados['fact_examen_estudiante']} | "
            f"fact_evaluacion_dictado={hechos_insertados['fact_evaluacion_dictado']}"
        )

        total_delta = sum(len(df) for df in deltas_raw.values())
        registrar_fin_ejecucion(ejecucion_id, inicio_ejecucion, total_delta, "OK")

        logger.info("[OK] Carga incremental simulada finalizada")
        logger.info("Auditoria actualizada en base de datos")

        return {
            "registros_delta": total_delta,
            "tiempo_insertados": tiempo_insertados,
            "scd_estudiante": scd_estudiante,
            "scd_dictado": scd_dictado,
            "hechos_insertados": hechos_insertados,
        }
    except Exception as exc:
        logger.error(f"Error en carga incremental: {str(exc)}", exc_info=True)
        registrar_fin_ejecucion(
            ejecucion_id,
            inicio_ejecucion,
            0,
            "ERROR",
            mensaje_error=str(exc),
        )
        raise


if __name__ == "__main__":
    procesar_incremental()
