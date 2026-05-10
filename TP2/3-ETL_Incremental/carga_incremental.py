#!/usr/bin/env python
# coding: utf-8

"""
ETL incremental simulado para dw_universidad.

Este script no reconstruye todo el DWH ni ejecuta TRUNCATE. Simula una carga
incremental tomando registros nuevos de staging según `fecha_carga` y aplicando:

- Inserción incremental de Tiempo.
- SCD Tipo 2 básico para Alumno y Dictado.
- Inserción incremental de hechos con INSERT IGNORE para evitar duplicados.

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


def insertar_tiempo_incremental(fechas: Iterable[Optional[date]]) -> int:
    dim_tiempo, _ = base_etl.construir_dim_tiempo(fechas)
    return insert_ignore_dataframe(dim_tiempo, "Tiempo")


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


def aplicar_scd2_alumno(dim_alumno_delta: pd.DataFrame) -> Dict[str, int]:
    columnas_comparables = [
        "dni",
        "nombre",
        "apellido",
        "genero",
        "fechaNacim",
        "nacionalidad",
        "añoIngreso",
        "edadIngreso",
        "nombrePrograma",
        "tipoPrograma",
        "duracionAñosPrograma",
    ]
    return aplicar_scd2_generico(
        df_delta=dim_alumno_delta,
        tabla="Alumno",
        clave_natural="idalumno",
        sk_columna="alumnoSKey",
        columnas_comparables=columnas_comparables,
    )


def aplicar_scd2_dictado(dim_dictado_delta: pd.DataFrame) -> Dict[str, int]:
    columnas_comparables = [
        "periodo",
        "turno",
        "aula",
        "cupoMax",
        "codigoCurso",
        "nombreCurso",
        "horasTeoCurso",
        "horasPracCurso",
        "horasLabCurso",
        "nivelCurso",
        "nombreDocente",
        "apellidoDocente",
        "tituloDocente",
        "categoriaDocente",
        "dedicacionDocente",
        "nombreDep",
        "nombreFac",
        "ciudadFac",
        "provFac",
    ]
    return aplicar_scd2_generico(
        df_delta=dim_dictado_delta,
        tabla="Dictado",
        clave_natural="idDictado",
        sk_columna="dictadoSKey",
        columnas_comparables=columnas_comparables,
    )


def aplicar_scd2_generico(
    df_delta: pd.DataFrame,
    tabla: str,
    clave_natural: str,
    sk_columna: str,
    columnas_comparables: List[str],
) -> Dict[str, int]:
    insertados = 0
    actualizados = 0
    sin_cambios = 0

    if df_delta.empty:
        return {"insertados": 0, "actualizados": 0, "sin_cambios": 0}

    for registro in dataframe_a_registros(df_delta):
        valor_clave = registro[clave_natural]
        fila_actual = obtener_fila_actual(tabla, clave_natural, valor_clave)
        registro_df = pd.DataFrame([registro])

        if fila_actual is None:
            insertados += insertar_dimension(registro_df, tabla)
            continue

        if valores_cambiaron(fila_actual, registro, columnas_comparables):
            expirar_dimension(tabla, sk_columna, fila_actual[sk_columna])
            insertados += insertar_dimension(registro_df, tabla)
            actualizados += 1
        else:
            sin_cambios += 1

    return {
        "insertados": insertados,
        "actualizados": actualizados,
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


def procesar_incremental() -> Dict:
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

    mapa_ids_estudiante_duplicados = base_etl.obtener_mapa_ids_estudiante_duplicados()
    if not deltas_limpios.get("inscripciones", pd.DataFrame()).empty:
        deltas_limpios["inscripciones"], _ = base_etl.remapear_ids_estudiante(
            deltas_limpios["inscripciones"], mapa_ids_estudiante_duplicados
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

    dim_alumno_delta = pd.DataFrame()
    if not deltas_limpios.get("estudiantes", pd.DataFrame()).empty:
        dim_alumno_delta, _ = base_etl.construir_dim_alumno(
            deltas_limpios["estudiantes"], lookups["programas"]
        )
    scd_alumno = aplicar_scd2_alumno(dim_alumno_delta)

    dim_dictado_delta = pd.DataFrame()
    if not deltas_limpios.get("dictados", pd.DataFrame()).empty:
        dim_dictado_delta, _ = base_etl.construir_dim_dictado(
            deltas_limpios["dictados"],
            lookups["cursos"],
            lookups["docentes"],
            lookups["departamentos"],
            lookups["facultades"],
        )
    scd_dictado = aplicar_scd2_dictado(dim_dictado_delta)

    print(
        f"  Tiempo insertados={tiempo_insertados} | "
        f"Alumno nuevos={scd_alumno['insertados']} actualizados={scd_alumno['actualizados']} | "
        f"Dictado nuevos={scd_dictado['insertados']} actualizados={scd_dictado['actualizados']}"
    )

    print("[4/4] Insertando hechos incrementales...")
    mapa_alumno = base_etl.obtener_mapa_alumno()
    mapa_dictado = base_etl.obtener_mapa_dictado()
    mapa_tiempo = base_etl.obtener_mapa_tiempo()

    hechos_insertados = {
        "fact_inscripcion": 0,
        "fact_examen_alumno": 0,
        "fact_evaluacion_dictado": 0,
    }

    if not deltas_limpios.get("inscripciones", pd.DataFrame()).empty:
        fact_inscripcion, _ = base_etl.construir_fact_inscripcion(
            deltas_limpios["inscripciones"],
            lookups["dictados"],
            mapa_alumno,
            mapa_dictado,
            mapa_tiempo,
        )
        hechos_insertados["fact_inscripcion"] = insert_ignore_dataframe(
            fact_inscripcion, "fact_inscripcion"
        )

    if not deltas_limpios.get("examenes", pd.DataFrame()).empty:
        inscripciones_base = deltas_limpios.get("inscripciones")
        if inscripciones_base is None or inscripciones_base.empty:
            inscripciones_raw = leer_staging_completo("stg_inscripcion")
            inscripciones_base, _ = base_etl.transformar_inscripcion_base(
                inscripciones_raw
            )
            inscripciones_base, _ = base_etl.remapear_ids_estudiante(
                inscripciones_base, mapa_ids_estudiante_duplicados
            )
        fact_examen, _ = base_etl.construir_fact_examen_alumno(
            deltas_limpios["examenes"],
            inscripciones_base,
            mapa_alumno,
            mapa_dictado,
            mapa_tiempo,
        )
        hechos_insertados["fact_examen_alumno"] = insert_ignore_dataframe(
            fact_examen, "fact_examen_alumno"
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
        f"fact_examen_alumno={hechos_insertados['fact_examen_alumno']} | "
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
            "scd_alumno": scd_alumno,
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
        "scd_alumno": scd_alumno,
        "scd_dictado": scd_dictado,
        "hechos_insertados": hechos_insertados,
    }


if __name__ == "__main__":
    procesar_incremental()
