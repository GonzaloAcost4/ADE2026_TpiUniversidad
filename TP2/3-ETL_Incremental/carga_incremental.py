#!/usr/bin/env python
# coding: utf-8

################################################################################
# SCRIPT: carga_incremental.py
################################################################################
# PROPSITO GENERAL:
# Script ETL que simula una carga INCREMENTAL hacia el DWH.
# 
# DIFERENCIA CON CARGA INICIAL:
# - CARGA INICIAL: Lee TODO staging, transforma TODO, trunca+carga TODO en DWH
#   -> Reconstruye DWH completo desde cero
#   -> Ejecutar: Primera vez, o cuando necesitas full refresh
# 
# - CARGA INCREMENTAL: Lee SOLO registros NUEVOS desde staging, 
#   transforma solo esos, inserta solo esos en DWH
#   -> Mantiene datos anteriores intactos
#   -> Ejecutar: Regularmente (diaria, horaria, segn poltica)
#   -> Ms rpido, menos impacto en BD
#
# CONCEPTO DE DELTA (DIFERENCIAL):
# Delta = cambios desde ltima extraccin
# - Se detecta por: fecha_carga > fecha_ltima_extraccin
# - Se controla en: datos_control/ultima_extraccion.json
#
# ESTRATEGIA DE CARGA INCREMENTAL:
#
# 1. DIMENSIONES: SCD Tipo 2 bsico
#    - Si registro NO exista -> Insertar (INSERT)
#    - Si registro EXISTE y cambi -> Expirar viejo + Insertar nuevo (UPDATE + INSERT)
#    - Si registro EXISTE y NO cambi -> No hacer nada
#
# 2. HECHOS: INSERT IGNORE
#    - Intenta insertar cada fila
#    - Si ya existe (por clave nica) -> ignora
#    - Garantiza NO duplicados sin borrar existentes
#
# 3. TIEMPO: INSERT IGNORE
#    - Inserta solo fechas nuevas
#    - Evita duplicados de fechas
#
# REUTILIZACIN DE CDIGO:
# - Llama funciones de transformacion.py (transformar_*_base)
# - Reutiliza la lgica de limpieza y validacin
# - Garantiza consistencia entre carga inicial e incremental
#
# CONTROL DE ESTADO:
# - Archivo de control: datos_control/ultima_extraccion.json
# - Registra: fecha ltima extraccin, ejecuciones, cambios procesados
# - Permite: reanudar desde donde se qued, auditora de cambios
#
# OUTPUT:
# - Logs: 3-ETL_Incremental/logs/carga_incremental_YYYYMMDD_HHMMSS.log
# - BD: Tablas DWH actualizadas (sin TRUNCATE, solo INSERTS/UPDATES)
# - JSON: datos_control/ultima_extraccion.json (estado actualizado)
#
################################################################################

import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text

# Determinar directorio del script
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd().resolve()

# Directorio de carga inicial (para reutilizar transformaciones)
PROJECT_TP2_DIR = SCRIPT_DIR.parent
CARGA_INICIAL_DIR = PROJECT_TP2_DIR / "2-ETL_CargaInicial"

# Agregar rutas al path
if str(PROJECT_TP2_DIR) not in sys.path:
    sys.path.append(str(PROJECT_TP2_DIR))
if str(CARGA_INICIAL_DIR) not in sys.path:
    sys.path.append(str(CARGA_INICIAL_DIR))

# REUTILIZACIN: Importar transformacion.py de carga inicial
# Esto nos da acceso a:
# - engine_stg, engine_dwh (conexiones)
# - DataCleaner (lgica de limpieza)
# - transformar_*_base() (funciones de transformacin)
# - construir_dim_* (construccin de dimensiones)
# - construir_fact_* (construccin de hechos)
import transformacion as base_etl
from logging_config import LoggerManager

# Configurar logging
logger = LoggerManager.configurar(
    "carga_incremental",
    ruta_raiz=str(SCRIPT_DIR),
    carpeta_logs="logs",
)

################################################################################
# CONFIGURACIN: DIRECTORIO DE CONTROL
################################################################################

# Directorio para archivos de control (estado de ejecuciones)
# Ubicacin: 3-ETL_Incremental/datos_control/
CONTROL_DIR = SCRIPT_DIR / "datos_control"
CONTROL_DIR.mkdir(exist_ok=True)  # Crear si no existe

# Archivo JSON de control: ltimas fechas/estadsticas de extraccin
# Contenido: {"fecha_ultima_extraccion": "2024-05-10T15:30:45", "ejecuciones": [...]}
CONTROL_FILE = CONTROL_DIR / "ultima_extraccion.json"

################################################################################
# CONFIGURACIN: PARMETROS DE INCREMENTO
################################################################################

# LMITE_DELTA_INICIAL: En primera ejecucin (sin fecha anterior),
# cuntos registros recuperar como "base"? (para no procesar TODO)
# Valor: 1000 registros (ltimos 1000 por row_id)
LIMITE_DELTA_INICIAL = int(os.getenv("LIMITE_DELTA_INICIAL", "1000"))

################################################################################
# MAPEO: TABLAS STAGING -> TABLAS OPERACIONALES
################################################################################

# Diccionario que mapea:
# clave_lgica -> nombre_tabla_staging
# Usado para: leer deltas, transformar, cargar
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

################################################################################
# MAPEO: TRANSFORMACIONES BASE A LLAMAR
################################################################################

# Diccionario que mapea:
# clave_lgica -> funcin_transformacin_de_carga_inicial
# Reutilizacin: estas funciones limpian y validan datos
# Ubicacin: transformacion.py (importado como base_etl)
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
    """
    ================================================================================
    FUNCIN: cargar_control()
    ================================================================================
    
    INPUT: (ninguno)
    OUTPUT: Dict con estado de ejecuciones anteriores
    
    PROPSITO:
    Leer archivo de control (JSON) que contiene:
    - ltima extraccin: cundo fue la ltima carga incremental
    - Historial: lista de ejecuciones anteriores
    
    ESTRUCTURA DEL JSON:
    {
        "fecha_ultima_extraccion": "2024-05-10T15:30:45" o None,
        "ejecuciones": [
            {"fecha": "2024-05-10T15:30:45", "registros": 150, "estado": "OK"},
            ...
        ]
    }
    
    UBICACIN:
    datos_control/ultima_extraccion.json
    
    PRIMERA EJECUCIN:
    Si archivo no existe: retorna Dict vaco con None
    """
    if not CONTROL_FILE.exists():
        return {
            "fecha_ultima_extraccion": None,
            "ejecuciones": [],
        }

    with CONTROL_FILE.open("r", encoding="utf-8") as archivo:
        return json.load(archivo)


def guardar_control(control: Dict) -> None:
    """
    ================================================================================
    FUNCIN: guardar_control(control)
    ================================================================================
    
    INPUT: control (Dict) - Diccionario con estado actualizado
    OUTPUT: (ninguno, solo escribe archivo)
    
    PROPSITO:
    Persistir estado de ejecucin en JSON para prxima ejecucin.
    
    PASOS:
    1. Abrir archivo en modo escritura
    2. Serializar Dict a JSON con formato legible (indent=2)
    3. Soporte para caracteres especiales (ensure_ascii=False)
    4. Manejar objetos date/datetime (default=str)
    
    ACTUALIZACIN TPICA:
    >>> control = cargar_control()
    >>> control['fecha_ultima_extraccion'] = '2024-05-10T16:00:00'
    >>> control['ejecuciones'].append({...})
    >>> guardar_control(control)  # Persistir cambios
    \"\"\"\n    with CONTROL_FILE.open(\"w\", encoding=\"utf-8\") as archivo:
        json.dump(control, archivo, indent=2, ensure_ascii=False, default=str)

  """ 
def leer_delta_staging(tabla: str, ultima_extraccion: Optional[str]) -> pd.DataFrame:
    if ultima_extraccion:
        query = text(
            f"SELECT * FROM {tabla} WHERE fecha_carga > :ultima ORDER BY row_id"
        )
        return pd.read_sql(
            query, con=base_etl.engine_stg, params={"ultima": ultima_extraccion}
        )

    # Primera ejecucin incremental simulada: se toma una muestra reciente para no reprocesar todo.
    query = text(f"SELECT * FROM {tabla} ORDER BY row_id DESC LIMIT :limite")
    df = pd.read_sql(
        query, con=base_etl.engine_stg, params={"limite": LIMITE_DELTA_INICIAL}
    )
    return df.sort_values("row_id") if "row_id" in df.columns else df


def leer_staging_completo(tabla: str) -> pd.DataFrame:
    """
    ================================================================================
    FUNCIN: leer_staging_completo(tabla)
    ================================================================================
    
    INPUT: tabla (str) - Nombre de tabla STG_* a leer completamente
    OUTPUT: DataFrame con TODOS los registros de la tabla
    
    PROPSITO:
    Leer 100% de una tabla (sin filtro por delta/fecha).
    til para:
    - Construir lookups (tablas de referencia completas)
    - Reconstruir mapeos de IDs
    - Validaciones de integridad
    
    REUTILIZACIN:
    Se usa en `construir_lookups_completos()` para cargar:
    - Facultades, Departamentos, Programas, Cursos, Docentes
    
    EJEMPLO:
    >>> df_cursos = leer_staging_completo('stg_curso')
    >>> len(df_cursos)  # Todos los cursos
    250
    """
    return pd.read_sql(f"SELECT * FROM {tabla}", con=base_etl.engine_stg)


def limpiar_delta(clave: str, df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    ================================================================================
    FUNCIN: limpiar_delta(clave, df_raw)
    ================================================================================
    
    INPUT:
    - clave: identificador lgico de tabla (ej: 'estudiantes', 'dictados')
    - df_raw: DataFrame crudo de staging (columnas con _raw)
    
    OUTPUT: (DataFrame limpio/transformado, Dict de estadsticas)
    
    PROPSITO:
    Reutilizar funciones de transformacin de carga_inicial.py
    Garantiza consistencia: mismo criterio de limpieza en ambos procesos.
    
    REUTILIZACIN:
    Llama a transformaciones_base['clave']() que son funciones
    importadas de transformacion.py (transformar_estudiante_base, etc)
    
    ESTADSTICAS RETORNADAS:
    {
        'total': N registros entrada,
        'vlidos': N que pasaron validacin,
        'rechazados': N que fallaron validacin,
        'duplicados': N eliminados por duplicacin
    }
    
    EJEMPLO:
    >>> df_raw = leer_delta_staging('stg_estudiante', None)
    >>> df_limpio, stats = limpiar_delta('estudiantes', df_raw)
    >>> stats['rechazados']  # Cuntos fueron rechazados
    15
    """
    funcion = TRANSFORMACIONES_BASE[clave]
    return funcion(df_raw)


def normalizar_valor_bd(valor):
    """
    ================================================================================
    FUNCIN: normalizar_valor_bd(valor)
    ================================================================================
    
    INPUT: valor - Cualquier tipo (numpy scalar, float, str, None, etc)
    OUTPUT: Valor normalizado para DB (Python native type o None)
    
    CONVERSIN:
    1. Si NaN/None -> None
    2. Si es numpy scalar (.item()) -> extrae valor nativo
    3. Si es tipo nativo -> retorna como est
    
    PROPSITO:
    SQLAlchemy a veces pasa valores como numpy.int64, numpy.float64, etc.
    BD espera tipos Python nativos (int, float, str, None).
    
    EJEMPLO:
    >>> normalizar_valor_bd(np.int64(123))
    123  # int
    >>> normalizar_valor_bd(np.float64(3.14))
    3.14  # float
    >>> normalizar_valor_bd(None)
    None
    """
    if pd.isna(valor):
        return None
    if hasattr(valor, "item"):
        return valor.item()
    return valor


def dataframe_a_registros(df: pd.DataFrame) -> List[Dict]:
    """
    ================================================================================
    FUNCIN: dataframe_a_registros(df)
    ================================================================================
    
    INPUT: df - DataFrame de pandas
    OUTPUT: Lista de Dicts (uno por fila), con valores normalizados
    
    TRANSFORMACIN:
    1. Convertir DataFrame -> list of dicts (orient='records')
    2. Normalizar CADA valor usando normalizar_valor_bd()
    3. Retornar lista de registros limpios
    
    PROPSITO:
    Preparar datos para insercin SQL con parmetros.
    Asegura que valores numpy se conviertan a tipos nativos.
    
    EJEMPLO:
    >>> df = pd.DataFrame({'id': [1, 2], 'nombre': ['Juan', 'Mara']})
    >>> registros = dataframe_a_registros(df)
    >>> registros
    [{'id': 1, 'nombre': 'Juan'}, {'id': 2, 'nombre': 'Mara'}]
    """
    registros = []
    for registro in df.to_dict(orient="records"):
        registros.append(
            {clave: normalizar_valor_bd(valor) for clave, valor in registro.items()}
        )
    return registros


def insert_ignore_dataframe(df: pd.DataFrame, tabla: str) -> int:
    """
    ================================================================================
    FUNCIN: insert_ignore_dataframe(df, tabla)
    ================================================================================
    
    INPUT:
    - df: DataFrame con datos a insertar
    - tabla: Nombre de tabla DWH destino
    
    OUTPUT: int - Cantidad de registros insertados exitosamente
    
    ALGORITMO (8 pasos):
    1. Si DataFrame vaco -> retornar 0
    2. Extraer nombres de columnas
    3. Generar SQL: INSERT IGNORE INTO tabla (cols) VALUES (...)
    4. Crear parmetros: :p0, :p1, :p2, etc
    5. Convertir DataFrame -> lista de dicts
    6. Para cada dict: mapear columnas -> parmetros
    7. Ejecutar INSERT batch con transaccin
    8. Retornar rowcount
    
    ESTRATEGIA INSERT IGNORE:
    - Intenta insertar cada fila
    - Si YA EXISTE (clave nica) -> ignora silenciosamente
    - Resultado: sin duplicados, sin errores
    
    VENTAJA sobre INSERT regular:
    - No falla si registro ya existe
    - Idempotencia: ejecutar 2x = mismo resultado
    
    EJEMPLO:
    >>> df_tiempo = pd.DataFrame({'fecha_sk': [20240510], 'fecha': ['2024-05-10']})
    >>> insertados = insert_ignore_dataframe(df_tiempo, 'Tiempo')
    >>> print(f"Insertados: {insertados}")
    Insertados: 1
    """
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
    """
    ================================================================================
    FUNCIN: insertar_tiempo_incremental(fechas)
    ================================================================================
    
    INPUT: fechas - Lista/iterable de objetos date (puede incluir None)
    OUTPUT: int - Cantidad de fechas insertadas en DWH
    
    PROCESO (3 pasos):
    1. Llamar construir_dim_tiempo() para generar dimensin
       -> convierte dates -> Surrogate Keys (YYYYMMDD)
       -> agrega atributos calendar (mes, trimestre, etc)
    2. Pasar resultado a insert_ignore_dataframe()
       -> usa INSERT IGNORE (evita duplicados)
    3. Retornar cantidad insertada
    
    PROPSITO:
    Insertar fechas nuevas de delta (inscripciones, exmenes, evaluaciones)
    en Dimensin Tiempo, evitando duplicados.
    
    EJEMPLO:
    >>> fechas = [date(2024, 5, 10), date(2024, 5, 11), None]
    >>> insertados = insertar_tiempo_incremental(fechas)
    >>> print(f"Fechas insertadas: {insertados}")
    Fechas insertadas: 2
    """
    dim_tiempo, _ = base_etl.construir_dim_tiempo(fechas)
    return insert_ignore_dataframe(dim_tiempo, "Tiempo")


def obtener_fila_actual(tabla: str, clave_natural: str, valor) -> Optional[Dict]:
    """
    ================================================================================
    FUNCIN: obtener_fila_actual(tabla, clave_natural, valor)
    ================================================================================
    
    INPUT:
    - tabla: Nombre tabla DWH (ej: 'Alumno', 'Dictado')
    - clave_natural: Columna de clave natural (ej: 'idalumno', 'idDictado')
    - valor: Valor de la clave a buscar
    
    OUTPUT: Dict con la fila actual, o None si no existe
    
    QUERY:
    SELECT * FROM tabla 
    WHERE clave_natural = valor 
      AND es_actual = TRUE
    LIMIT 1
    
    PROPSITO SCD TIPO 2:
    En dimensiones con historiales, cada entidad tiene mltiples versiones.
    es_actual=TRUE marca la versin vigente.
    Esta funcin obtiene LA VERSIN ACTUAL de un registro.
    
    EJEMPLO:
    >>> fila = obtener_fila_actual('Alumno', 'idalumno', 123)
    >>> if fila:
    ...     print(fila['nombre'])
    ... else:
    ...     print("Alumno no existe en DWH")
    """
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
    """
    ================================================================================
    FUNCIN: expirar_dimension(tabla, sk_columna, sk_valor)
    ================================================================================
    
    INPUT:
    - tabla: Nombre tabla DWH (ej: 'Alumno', 'Dictado')
    - sk_columna: Columna Surrogate Key (ej: 'alumnoSKey')
    - sk_valor: Valor de la SK a expirar
    
    OUTPUT: (ninguno, solo ejecuta UPDATE)
    
    ACTUALIZACIN (SCD TIPO 2):
    UPDATE tabla 
    SET valid_to = hoy, es_actual = FALSE
    WHERE sk_columna = sk_valor
    
    PROPSITO:
    Marcar un registro como "VENCIDO" en historial dimensional.
    Cuando cambia un atributo de alumno/dictado:
    - Se expira la versin VIEJA (es_actual=FALSE, valid_to=hoy)
    - Se inserta versin NUEVA (es_actual=TRUE, valid_from=hoy)
    
    EJEMPLO DE FLUJO:
    1. Obtener fila actual del alumno
    2. Detectar cambios en atributos
    3. Si cambi:
       a. expirar_dimension() -> marca vieja como inactiva
       b. insertar_dimension() -> carga nueva versin
    """
    query = text(
        f"UPDATE {tabla} SET valid_to = :valid_to, es_actual = FALSE "
        f"WHERE {sk_columna} = :sk_valor"
    )
    with base_etl.engine_dwh.begin() as conn:
        conn.execute(
            query, {"valid_to": date.today(), "sk_valor": normalizar_valor_bd(sk_valor)}
        )


def insertar_dimension(df: pd.DataFrame, tabla: str) -> int:
    """
    ================================================================================
    FUNCIN: insertar_dimension(df, tabla)
    ================================================================================
    
    INPUT:
    - df: DataFrame con registros a insertar
    - tabla: Nombre tabla DWH (ej: 'Alumno', 'Dictado')
    
    OUTPUT: int - Cantidad de registros insertados
    
    PROPSITO:
    Insertar nuevos registros en dimensin (APPEND, no TRUNCATE).
    til en SCD Tipo 2 para historiales.
    
    PARMETROS to_sql():
    - if_exists='append': agrega filas sin borrar existentes
    - index=False: no incluye ndice de pandas
    - method='multi': mltiples INSERT en 1 statement (rpido)
    
    EJEMPLO:
    >>> df_nuevo = pd.DataFrame([{'alumnoSKey': 1001, 'idalumno': 50, ...}])
    >>> insertados = insertar_dimension(df_nuevo, 'Alumno')
    >>> print(f"Filas: {insertados}")
    Filas: 1
    """
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
    """
    ================================================================================
    FUNCIN: valores_cambiaron(fila_actual, registro_nuevo, columnas_comparables)
    ================================================================================
    
    INPUT:
    - fila_actual: Dict con registro actual en DWH
    - registro_nuevo: Dict con nuevo registro de STAGING
    - columnas_comparables: Lista de columnas a comparar
    
    OUTPUT: bool - True si algn valor cambi, False si idnticos
    
    ALGORITMO:
    1. Para CADA columna en columnas_comparables:
    2. Obtener valor actual y nuevo
    3. Convertir ambos a string (normaliza comparacin)
    4. Si differ -> retornar True (cambi)
    5. Si ninguno cambi -> retornar False
    
    CONVERSIN A STRING:
    Importante para comparar diferentes tipos:
    - str(None) == 'None'
    - str(1.0) == '1.0'
    - str(date(2024,5,10)) == '2024-05-10'
    
    EJEMPLO:
    >>> fila_actual = {'nombre': 'Juan', 'edad': '20'}
    >>> fila_nueva = {'nombre': 'Juan', 'edad': '21'}
    >>> valores_cambiaron(fila_actual, fila_nueva, ['nombre', 'edad'])
    True  # Edad cambi de 20 a 21
    """
    for columna in columnas_comparables:
        actual = fila_actual.get(columna)
        nuevo = registro_nuevo.get(columna)
        if str(actual) != str(nuevo):
            return True
    return False


def aplicar_scd2_alumno(dim_alumno_delta: pd.DataFrame) -> Dict[str, int]:
    """
    ================================================================================
    FUNCIN: aplicar_scd2_alumno(dim_alumno_delta)
    ================================================================================
    
    INPUT: dim_alumno_delta - DataFrame con cambios de ALUMNOS desde staging
    OUTPUT: Dict con estadsticas {'insertados': N, 'actualizados': N, 'sin_cambios': N}
    
    PROPSITO:
    Aplicar SCD (Slowly Changing Dimension) Tipo 2 a dimensin Alumno.
    
    COLUMNAS COMPARABLES:
    Atributos de alumno que si cambian, generan nueva versin histrica:
    - dni, nombre, apellido, genero, fecha_nacimiento, nacionalidad
    - ao_ingreso, edad_ingreso
    - nombre_programa, tipo_programa, duracin_aos_programa
    
    LGICA (delegada a aplicar_scd2_generico):
    1. Si alumno NO existe en DWH -> INSERTAR nueva versin
    2. Si alumno EXISTE y cambi algn atributo -> EXPIRAR vieja + INSERTAR nueva
    3. Si alumno EXISTE pero SIN CAMBIOS -> No hacer nada
    
    RESULTADO:
    - insertados: filas nuevas agregadas
    - actualizados: versionamientos por cambio
    - sin_cambios: registros sin cambios
    
    EJEMPLO:
    >>> delta_alumnos = {...}  # 50 estudiantes del delta
    >>> stats = aplicar_scd2_alumno(delta_alumnos)
    >>> stats
    {'insertados': 2, 'actualizados': 3, 'sin_cambios': 45}
    # 2 nuevos, 3 actualizados, 45 sin cambios
    """
    columnas_comparables = [
        "dni",
        "nombre",
        "apellido",
        "genero",
        "fechaNacim",
        "nacionalidad",
        "aoIngreso",
        "edadIngreso",
        "nombrePrograma",
        "tipoPrograma",
        "duracionAosPrograma",
    ]
    return aplicar_scd2_generico(
        df_delta=dim_alumno_delta,
        tabla="Alumno",
        clave_natural="idalumno",
        sk_columna="alumnoSKey",
        columnas_comparables=columnas_comparables,
    )


def aplicar_scd2_dictado(dim_dictado_delta: pd.DataFrame) -> Dict[str, int]:
    """
    ================================================================================
    FUNCIN: aplicar_scd2_dictado(dim_dictado_delta)
    ================================================================================
    
    INPUT: dim_dictado_delta - DataFrame con cambios de DICTADOS desde staging
    OUTPUT: Dict con estadsticas {'insertados': N, 'actualizados': N, 'sin_cambios': N}
    
    PROPSITO:
    Aplicar SCD (Slowly Changing Dimension) Tipo 2 a dimensin Dictado.
    
    COLUMNAS COMPARABLES:
    Atributos de dictado que si cambian, generan nueva versin histrica:
    - perodo, turno, aula, cupo_maximo
    - cdigo_curso, nombre_curso, horas_terica, horas_prctica, horas_laboratorio, nivel_curso
    - nombre_docente, apellido_docente, ttulo_docente, categora_docente, dedicacin_docente
    - nombre_departamento, nombre_facultad, ciudad_facultad, provincia_facultad
    
    LGICA (delegada a aplicar_scd2_generico):
    1. Si dictado NO existe en DWH -> INSERTAR nueva versin
    2. Si dictado EXISTE y cambi -> EXPIRAR viejo + INSERTAR nuevo
    3. Si dictado EXISTE pero SIN CAMBIOS -> No hacer nada
    
    RESULTADO:
    - insertados: filas nuevas agregadas
    - actualizados: versionamientos por cambio
    - sin_cambios: registros sin cambios
    
    EJEMPLO:
    >>> delta_dictados = {...}  # 30 dictados del delta
    >>> stats = aplicar_scd2_dictado(delta_dictados)
    >>> stats
    {'insertados': 5, 'actualizados': 1, 'sin_cambios': 24}
    """
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
    """
    ================================================================================
    FUNCIN: aplicar_scd2_generico(df_delta, tabla, clave_natural, sk_columna, ...) 
    ================================================================================
    
    INPUT:
    - df_delta: DataFrame con cambios a procesar
    - tabla: Nombre tabla DWH (ej: 'Alumno', 'Dictado')
    - clave_natural: Columna identificador lgico (ej: 'idalumno')
    - sk_columna: Columna Surrogate Key (ej: 'alumnoSKey')
    - columnas_comparables: Lista columnas a verificar cambios
    
    OUTPUT: Dict {'insertados': N, 'actualizados': N, 'sin_cambios': N}
    
    ALGORITMO SCD TIPO 2 (8 pasos):
    
    1. Para CADA registro en df_delta:
    
    2. Obtener clave natural y buscar versin ACTUAL en BD
    
    3. Si NO EXISTE en BD:
       -> INSERTAR registro nuevo (es_actual=TRUE, valid_from=hoy)
       -> Contador: insertados++
    
    4. Si EXISTE:
       4a. Comparar valores en columnas_comparables
       4b. Si ALGN valor cambi:
           -> expirar_dimension(): marca viejo como inactivo
           -> insertar_dimension(): carga nueva versin
           -> Contador: actualizados++
       4c. Si TODO igual:
           -> No hacer nada
           -> Contador: sin_cambios++
    
    HISTORIAL RESULTANTE (ejemplo):
    Alumno con ID=100 v1: nombre='Juan', vlido_hasta=null, es_actual=TRUE
    Despus de cambio:
    Alumno con ID=100 v1: nombre='Juan', vlido_hasta=2024-05-10, es_actual=FALSE
    Alumno con ID=100 v2: nombre='John', vlido_desde=2024-05-11, es_actual=TRUE
    
    EJEMPLO:
    >>> dim_alumno_delta = {...}  # 50 alumnos
    >>> stats = aplicar_scd2_generico(
    ...     df_delta=dim_alumno_delta,
    ...     tabla='Alumno',
    ...     clave_natural='idalumno',
    ...     sk_columna='alumnoSKey',
    ...     columnas_comparables=['nombre', 'apellido', 'genero', 'nacionalidad']
    ... )
    >>> print(stats)
    {'insertados': 10, 'actualizados': 3, 'sin_cambios': 37}
    """
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
    """
    ================================================================================
    FUNCIN: construir_lookups_completos()
    ================================================================================
    
    INPUT: (ninguno)
    OUTPUT: Dict con DataFrames de lookup (tablas de referencia)
    
    PROPSITO:
    Construir mapeos (lookups) de TODOS los identificadores en staging.
    tiles para transformaciones posteriores (ej: mapear idalumno -> alumnoSKey).
    
    LOOKUP CONSTRUIDO:
    {
        'facultades': DataFrame de stg_facultad transformado,
        'departamentos': DataFrame de stg_departamento transformado,
        'programas': DataFrame de stg_programa transformado,
        'cursos': DataFrame de stg_curso transformado,
        'docentes': DataFrame de stg_docente transformado
    }
    
    PROCESO (por cada lookup):
    1. Leer tabla staging COMPLETA (leer_staging_completo)
    2. Aplicar transformacin base (limpiar_delta)
    3. Guardar resultado limpio en dict
    
    REUTILIZACIN:
    Se usa en procesar_incremental() para:
    - construir_dim_alumno(deltas_limpios['estudiantes'], lookups['programas'])
    - construir_dim_dictado(..., lookups['cursos'], lookups['docentes'], ...)
    
    EJEMPLO:
    >>> lookups = construir_lookups_completos()
    >>> lookups['facultades'].shape
    (8, 4)  # 8 facultades
    >>> lookups['docentes'].shape
    (125, 6)  # 125 docentes
    """
    lookups = {}
    for clave in ["facultades", "departamentos", "programas", "cursos", "docentes"]:
        df_raw = leer_staging_completo(TABLAS_STAGING[clave])
        lookups[clave], _ = limpiar_delta(clave, df_raw)
    return lookups


def procesar_incremental() -> Dict:
    """
    ================================================================================
    FUNCIN: procesar_incremental()
    ================================================================================
    
    INPUT: (ninguno)
    OUTPUT: Dict con estadsticas de ejecucin
    
    PROPSITO - FUNCIN PRINCIPAL DEL ETL INCREMENTAL:
    Orquestar la carga incremental completa (4 fases):
    1. DETECTAR deltas
    2. LIMPIAR y normalizar
    3. APLICAR SCD Tipo 2 a dimensiones
    4. INSERTAR hechos
    
    FASES (8 pasos principales):
    
    [FASE 1] Deteccin de deltas:
    - Cargar estado anterior (ltima_extraccin)
    - Leer SOLO registros nuevos desde staging (fecha > ltima)
    - Primera ejecucin: toma ltimo 1000 registros (LIMITE_DELTA_INICIAL)
    
    [FASE 2] Limpieza:
    - Aplicar transformaciones base a cada delta
    - Rechazar registros invlidos
    - Deduplicar por claves naturales
    
    [FASE 3] Dimensiones (SCD Tipo 2):
    - Construir lookups completos (facultades, programas, etc)
    - Aplicar SCD a Alumno: detectar nuevos vs cambios
    - Aplicar SCD a Dictado: detectar nuevos vs cambios
    - Insertar Tiempo: fechas de inscripciones/exmenes
    
    [FASE 4] Hechos (INSERT IGNORE):
    - Inscripcin: una por lnea en delta
    - ExamenAlumno: notas de exmenes
    - EvaluacionDictado: puntajes de evaluaciones
    - Evita duplicados con INSERT IGNORE
    
    RESULTADO:
    {
        'registros_delta': cantidad cambios detectados,
        'tiempo_insertados': fechas nuevas,
        'scd_alumno': {'insertados': N, 'actualizados': N, 'sin_cambios': N},
        'scd_dictado': {...},
        'hechos_insertados': {'Inscripcion': N, 'ExamenAlumno': N, 'EvaluacionDictado': N}
    }
    
    PERSISTENCIA:
    Actualiza archivo de control (JSON):
    - fecha_ultima_extraccion = inicio_ejecucion
    - ejecuciones: agrega registro con estadsticas
    """
    print("\n=== Carga incremental simulada STG -> DWH ===")
    control = cargar_control()
    ultima = control.get("fecha_ultima_extraccion")
    inicio_ejecucion = datetime.now(timezone.utc).isoformat()

    print(f"ltima extraccin registrada: {ultima or 'sin registro previo'}")

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
                f"  Atencin {TABLAS_STAGING[clave]}: rechazados={stats['rechazados']} | duplicados={stats['duplicados']}"
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

    hechos_insertados = {"Inscripcion": 0, "ExamenAlumno": 0, "EvaluacionDictado": 0}

    if not deltas_limpios.get("inscripciones", pd.DataFrame()).empty:
        fact_inscripcion, _ = base_etl.construir_fact_inscripcion(
            deltas_limpios["inscripciones"], mapa_alumno, mapa_dictado, mapa_tiempo
        )
        hechos_insertados["Inscripcion"] = insert_ignore_dataframe(
            fact_inscripcion, "Inscripcion"
        )

    if not deltas_limpios.get("examenes", pd.DataFrame()).empty:
        inscripciones_base = deltas_limpios.get("inscripciones")
        if inscripciones_base is None or inscripciones_base.empty:
            inscripciones_raw = leer_staging_completo("stg_inscripcion")
            inscripciones_base, _ = base_etl.transformar_inscripcion_base(
                inscripciones_raw
            )
        fact_examen, _ = base_etl.construir_fact_examen_alumno(
            deltas_limpios["examenes"],
            inscripciones_base,
            mapa_alumno,
            mapa_dictado,
            mapa_tiempo,
        )
        hechos_insertados["ExamenAlumno"] = insert_ignore_dataframe(
            fact_examen, "ExamenAlumno"
        )

    if not deltas_limpios.get("evaluaciones", pd.DataFrame()).empty:
        fact_evaluacion, _ = base_etl.construir_fact_evaluacion_dictado(
            deltas_limpios["evaluaciones"], mapa_dictado, mapa_alumno, mapa_tiempo
        )
        hechos_insertados["EvaluacionDictado"] = insert_ignore_dataframe(
            fact_evaluacion, "EvaluacionDictado"
        )

    print(
        f"  Inscripcion={hechos_insertados['Inscripcion']} | "
        f"ExamenAlumno={hechos_insertados['ExamenAlumno']} | "
        f"EvaluacionDictado={hechos_insertados['EvaluacionDictado']}"
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
