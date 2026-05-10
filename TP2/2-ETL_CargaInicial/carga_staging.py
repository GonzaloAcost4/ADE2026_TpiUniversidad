#!/usr/bin/env python
# coding: utf-8

################################################################################
# SCRIPT: carga_staging.py
################################################################################
# PROPÓSITO GENERAL:
# Script ETL que carga datos desde archivos CSV en la carpeta Sources/ hacia
# las tablas de STAGING en la base de datos MySQL STG_Universidad.
#
# FLUJO DE DATOS:
# CSV Files (Sources/) → Pandas DataFrames → MySQL STG_* Tables
#
# PASOS PRINCIPALES:
# 1. Leer archivos CSV desde Sources/
# 2. Limpiar y validar datos (en pasos posteriores)
# 3. Ejecutar TRUNCATE en tablas staging (idempotencia)
# 4. Insertar datos completos y frescos en staging
# 5. Registrar estadísticas y errores en logs
#
# CARACTERÍSTICAS:
# - TRUNCATE + Full Load Strategy: garantiza idempotencia
# - Enriquecimiento especial para evaluacion_curso
# - Diagnóstico pre-carga completo
# - Logging centralizado con LoggerManager
# - Manejo de errores con rollback automático
#
# OUTPUT:
# - Logs: 2-ETL_CargaInicial/logs/carga_staging_YYYYMMDD_HHMMSS.log
# - BD: Tablas stg_* pobladas en STG_Universidad
#
################################################################################

# IMPORTS ESTÁNDAR DE PYTHON
import os                    # Manejo de rutas y directorios
import sys                   # Manipulación de sys.path
from datetime import datetime, timedelta  # Manejo de fechas

# IMPORTS DE TERCEROS
import pandas as pd                    # Manipulación de DataFrames
from dotenv import load_dotenv         # Lectura de variables de entorno
from sqlalchemy import create_engine, text  # Conexión SQL y ejecución de queries

# CONFIGURACIÓN DE RUTAS Y MÓDULOS
# Agregar directorio padre (TP2/) a sys.path para importar logging_config.py
sys.path.append(os.path.join(os.getcwd(), ".."))

# Importar el LoggerManager centralizado del proyecto
from logging_config import LoggerManager

################################################################################
# CONFIGURACIÓN: CREDENCIALES Y CONEXIÓN A BD
################################################################################

# Cargar variables de entorno desde .env
# override=True: asegura que se use el .env más actual
load_dotenv(override=True)

# Obtener credenciales de la base de datos desde variables de entorno
USER = os.getenv("DB_USER")           # Usuario MySQL
PASSWORD = os.getenv("DB_PASSWORD")   # Contraseña MySQL
HOST = os.getenv("DB_HOST")           # Host/IP del servidor MySQL
PORT = os.getenv("DB_PORT")           # Puerto (default 3306)
DATABASE = os.getenv("STG_DATABASE")  # Nombre BD staging (STG_Universidad)

# Crear motor SQLAlchemy para conexión a MySQL
# Formato: mysql+pymysql://usuario:contraseña@host:puerto/base_datos
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# Configurar logger centralizado para este script
# - nombre_proceso: 'carga_staging' (aparece en logs y archivo de log)
# - ruta_raiz: getcwd() (directorio actual: 2-ETL_CargaInicial/)
# - carpeta_logs: 'logs' (subcarpeta donde se guardan los .log)
logger = LoggerManager.configurar(
    "carga_staging", ruta_raiz=os.getcwd(), carpeta_logs="logs"
)

################################################################################
# CONSTANTES DE RUTAS
################################################################################

# Ruta absoluta de la carpeta Sources/ (donde están los CSV)
# getaway(): 2-ETL_CargaInicial/
# "..": sube a TP2/
# "Sources": desciende a TP2/Sources/
RUTA_SOURCES = os.path.join(os.getcwd(), "..", "Sources")
RUTA_SOURCES = os.path.abspath(RUTA_SOURCES)  # Convertir a ruta absoluta

# Obtener ruta del directorio de logs (ya creado por LoggerManager)
RUTA_LOGS = LoggerManager.obtener_ruta_logs()


# In[2]:


def enriquecer_evaluacion_curso(df):
    """
    ================================================================================
    FUNCIÓN: enriquecer_evaluacion_curso(df)
    ================================================================================
    
    PROPÓSITO:
    Completa el archivo evaluacion_curso.csv con datos faltantes que requiere
    la tabla DWH (EvaluacionDictado).
    
    PROBLEMA QUE RESUELVE:
    - El CSV original (evaluacion_curso.csv) tiene evaluaciones por DICTADO
    - Pero la tabla de hecho EvaluacionDictado necesita:
      * id_estudiante (¿Quién fue evaluado?)
      * fecha_evaluacion (¿Cuándo se evaluó?)
    - El CSV original NO tiene estos datos
    - SOLUCIÓN: inferir/enriquecer con información de tablas relacionales
    
    INPUT (PARÁMETRO):
    - df (DataFrame): DataFrame leído desde evaluacion_curso.csv
      Columnas esperadas:
      - id_evaluacion: clave única de la evaluación
      - id_dictado: qué dictado/materia se evaluó
      - id_evaluacion: identificador único
      - (otros campos: puntajes, contenido, etc.)
    
    OUTPUT (RETORNO):
    - DataFrame enriquecido con:
      - id_estudiante: alumno al que se asigna la evaluación
      - fecha_evaluacion: fecha estimada de la evaluación
      
    TRATAMIENTO (ALGORITMO):
    
    PASO 1: Validación inicial
    - Si df está vacío: retorna vacío sin procesar
    - Si ya tiene id_estudiante y fecha_evaluacion: retorna sin cambios
    
    PASO 2: Leer datos relacionales
    - Lee inscripcion.csv: relación alumno-dictado
    - Lee dictado.csv: información de períodos académicos
    
    PASO 3: Asignación determinística de estudiantes
    - Para cada evaluación, busca estudiantes inscriptos en ese dictado
    - Usa algoritmo módulo (%) para asignación determinística y reproducible
    - Ejemplo: si hay 3 inscriptos y 5 evaluaciones
      → Evaluación 0 → Inscripto 0
      → Evaluación 1 → Inscripto 1
      → Evaluación 2 → Inscripto 2
      → Evaluación 3 → Inscripto 0 (repite)
      → Evaluación 4 → Inscripto 1 (repite)
    
    PASO 4: Inferencia de fechas de evaluación
    Usa lógica de calendario académico Argentina:
    - C1 (Cuatrimestre 1): mayo-julio → Fecha estimada 15/07
    - C2 (Cuatrimestre 2): agosto-diciembre → Fecha estimada 15/12
    - Si no tiene período: usa fecha_inscripción + 90 días
    
    PASO 5: Validación final
    - Cuenta cuántas evaluaciones no tienen estudiante asignable
    - Registra warning si hay muchas sin asignación
    
    EJEMPLOS:
    
    Input DataFrame:
    ┌─────────────────┬─────────────┐
    │ id_evaluacion   │ id_dictado  │
    ├─────────────────┼─────────────┤
    │ 1               │ 10          │
    │ 2               │ 10          │
    │ 3               │ 11          │
    └─────────────────┴─────────────┘
    
    Output DataFrame (con enriquecimiento):
    ┌─────────────────┬─────────────┬─────────────────┬──────────────────┐
    │ id_evaluacion   │ id_dictado  │ id_estudiante   │ fecha_evaluacion │
    ├─────────────────┼─────────────┼─────────────────┼──────────────────┤
    │ 1               │ 10          │ 100             │ 2024-07-15       │
    │ 2               │ 10          │ 101             │ 2024-07-15       │
    │ 3               │ 11          │ 102             │ 2024-12-15       │
    └─────────────────┴─────────────┴─────────────────┴──────────────────┘
    
    NOTAS IMPORTANTES:
    - El algoritmo es DETERMINÍSTICO: mismos inputs → siempre mismo output
    - NO inventa DNIs ni estudiantes inexistentes
    - NO modifica datos de estudiantes reales
    - Solo enriquece con claves naturales existentes
    ================================================================================
    """
    if df.empty:
        return df

    columnas_requeridas = {"id_estudiante", "fecha_evaluacion"}
    if columnas_requeridas.issubset(set(df.columns)):
        return df

    ruta_inscripciones = os.path.join(RUTA_SOURCES, "inscripcion.csv")
    ruta_dictados = os.path.join(RUTA_SOURCES, "dictado.csv")

    if not os.path.exists(ruta_inscripciones):
        LoggerManager.warning(
            "No se puede enriquecer evaluacion_curso: falta inscripcion.csv"
        )
        df["id_estudiante"] = None
        df["fecha_evaluacion"] = None
        return df

    inscripciones = pd.read_csv(ruta_inscripciones, sep=",", dtype=str)
    inscripciones = inscripciones.sort_values(["id_dictado", "id_inscripcion"])
    inscripciones["orden_eval"] = inscripciones.groupby("id_dictado").cumcount()

    evaluaciones = df.copy()
    evaluaciones = evaluaciones.sort_values(["id_dictado", "id_evaluacion"])
    evaluaciones["orden_eval"] = evaluaciones.groupby("id_dictado").cumcount()

    inscripciones_por_dictado = (
        inscripciones.groupby("id_dictado").size().rename("cant_inscriptos")
    )
    evaluaciones = evaluaciones.merge(
        inscripciones_por_dictado, on="id_dictado", how="left"
    )
    evaluaciones["orden_match"] = evaluaciones.apply(
        lambda row: (
            int(row["orden_eval"]) % int(row["cant_inscriptos"])
            if pd.notna(row["cant_inscriptos"]) and int(row["cant_inscriptos"]) > 0
            else None
        ),
        axis=1,
    )

    inscripciones_match = inscripciones.rename(columns={"orden_eval": "orden_match"})[
        ["id_dictado", "orden_match", "id_estudiante", "fecha_inscripcion"]
    ]
    evaluaciones = evaluaciones.merge(
        inscripciones_match,
        on=["id_dictado", "orden_match"],
        how="left",
    )

    fecha_por_dictado = {}
    if os.path.exists(ruta_dictados):
        dictados = pd.read_csv(ruta_dictados, sep=",", dtype=str)
        for _, row in dictados.iterrows():
            anio = str(row.get("anio_academico", "")).strip()
            periodo = str(row.get("periodo", "")).strip().upper()
            if anio.isdigit():
                if periodo in {"C1", "1", "P1", "PRIMER", "PRIMERO"}:
                    fecha_por_dictado[str(row["id_dictado"])] = f"{anio}-07-15"
                elif periodo in {"C2", "2", "P2", "SEGUNDO"}:
                    fecha_por_dictado[str(row["id_dictado"])] = f"{anio}-12-15"

    def calcular_fecha_evaluacion(row):
        fecha_calendario = fecha_por_dictado.get(str(row["id_dictado"]))
        if fecha_calendario:
            return fecha_calendario
        try:
            return (
                (pd.to_datetime(row["fecha_inscripcion"]) + timedelta(days=90))
                .date()
                .isoformat()
            )
        except Exception:
            return None

    evaluaciones["fecha_evaluacion"] = evaluaciones.apply(
        calcular_fecha_evaluacion, axis=1
    )
    evaluaciones = evaluaciones.drop(
        columns=["orden_eval", "cant_inscriptos", "orden_match", "fecha_inscripcion"],
        errors="ignore",
    )

    sin_estudiante = evaluaciones["id_estudiante"].isna().sum()
    if sin_estudiante > 0:
        LoggerManager.warning(
            f"Evaluaciones sin estudiante asignable por falta de inscripción: {sin_estudiante}"
        )

    LoggerManager.info(
        "evaluacion_curso enriquecido con id_estudiante y fecha_evaluacion"
    )
    return evaluaciones


def cargar_csv_a_staging(archivo_csv, nombre_tabla_stg):
    """
    ================================================================================
    FUNCIÓN: cargar_csv_a_staging(archivo_csv, nombre_tabla_stg)
    ================================================================================
    
    PROPÓSITO:
    Lee un archivo CSV desde la carpeta Sources/, lo carga en una tabla STAGING
    usando estrategia TRUNCATE + Full Load (garantiza idempotencia).
    
    ESTRATEGIA DE CARGA: TRUNCATE + FULL LOAD
    - Método: Borra todos los datos previos, carga datos frescos
    - Idempotencia: Ejecutar N veces = mismo resultado (no duplica datos)
    - Ventaja: Simple, confiable, garantiza consistencia
    - Desventaja: Perde auditoría de cambios incrementales (por ahora)
    
    INPUT (PARÁMETROS):
    - archivo_csv (str): Nombre del archivo CSV en Sources/
      Ejemplo: 'estudiante.csv', 'dictado.csv'
    - nombre_tabla_stg (str): Nombre de tabla STAGING en BD
      Ejemplo: 'stg_estudiante', 'stg_dictado'
    
    OUTPUT (RETORNO):
    - bool: True si carga exitosa, False si ocurrió error
    
    TRATAMIENTO (FLUJO):
    
    ┌─── STEP 1: VERIFICACIÓN DE ARCHIVO ─┐
    │ - Construir ruta completa desde RUTA_SOURCES
    │ - Verificar que el archivo existe
    │ - Si no existe → Log error → Return False
    
    ┌─── STEP 2: TRUNCATE (LIMPIAR TABLA) ─┐
    │ - Ejecutar: DELETE FROM tabla  (alterna: TRUNCATE TABLE)
    │ - Propósito: Eliminar datos antiguos
    │ - Idempotencia: Si tabla está vacía, no falla
    │ - Si falla → Log error → Return False
    
    ┌─── STEP 3: LEER CSV EN MEMORIA ─┐
    │ - pd.read_csv() con dtype=str (todo texto)
    │ - Razón: Staging es "crudo", lo limpiaremos después
    │ - Si CSV vacío → Log warning → Return True (no hay dato)
    
    ┌─── STEP 4: ENRIQUECIMIENTO ESPECIAL ─┐
    │ - Si es evaluacion_curso → Llamar enriquecer_evaluacion_curso()
    │ - Otros CSVs → No se enriquecen aquí
    
    ┌─── STEP 5: AGREGAR METADATOS ─┐
    │ - archivo_origen: nombre del CSV (para auditoría)
    │ - fecha_carga: timestamp actual (cuándo se cargó)
    │ - Útil para debugging y rastrabilidad
    
    ┌─── STEP 6: RENOMBRAR COLUMNAS ─┐
    │ - Suffix '_raw': agregar '_raw' a nombres de columnas
    │ - Ejemplo: 'nombre' → 'nombre_raw'
    │ - Razón: En transformación posterior se limpian (nombre_raw → nombre)
    
    ┌─── STEP 7: INSERTAR EN BD ─┐
    │ - df.to_sql() con if_exists='append'
    │ - Inserta en tabla STG (ya truncada)
    │ - Si falla → Log error → Exception (no maneja)
    
    ┌─── STEP 8: REGISTRO DE ÉXITO ─┐
    │ - Log info: "Cargados X registros en tabla Y"
    │ - Return True
    
    ERRORES MANEJADOS:
    - Archivo no encontrado → False
    - TRUNCATE falló → False
    - Inserción falló → Exception (no recuperable)
    
    EJEMPLO DE USO:
    
    # Caso exitoso
    resultado = cargar_csv_a_staging('estudiante.csv', 'stg_estudiante')
    # Output: True, logs: "Cargados 500 registros en stg_estudiante"
    
    # Caso fallo - archivo no existe
    resultado = cargar_csv_a_staging('inexistente.csv', 'stg_tabla')
    # Output: False, logs: "Archivo no encontrado..."
    
    NOTA SOBRE IDEMPOTENCIA:
    Si ejecutas esta función dos veces:
    - Primera ejecución: tabla_stg vacía → TRUNCATE (sin efecto) → Inserta datos
    - Segunda ejecución: tabla_stg llena → TRUNCATE (borra todo) → Inserta datos
    - Resultado: Tabla idéntica (sin duplicados, datos frescos)
    
    ================================================================================
    """
    try:
        ruta_completa = os.path.join(RUTA_SOURCES, archivo_csv)

        # 1. Verificar que el archivo existe
        if not os.path.exists(ruta_completa):
            LoggerManager.error(f"Archivo no encontrado: {ruta_completa}")
            return False

        # 2. TRUNCATE - Limpia tabla para idempotencia
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {nombre_tabla_stg}"))
                conn.commit()
            LoggerManager.info(f"TRUNCATE ejecutado en {nombre_tabla_stg}")
        except Exception as e:
            LoggerManager.error(f"Fallo TRUNCATE: {str(e)}")
            return False

        # 3. Leer CSV como strings (criterio: VARCHAR en Staging)
        df = pd.read_csv(ruta_completa, sep=",", dtype=str)

        if df.empty:
            LoggerManager.warning(f"Archivo vacío: {archivo_csv}")
            return True

        # 4. Enriquecimiento específico para que la evaluación pueda cargar al DWH
        if nombre_tabla_stg == "stg_evaluacion_curso":
            df = enriquecer_evaluacion_curso(df)

        # 5. Renombrar columnas con sufijo '_raw'
        df = df.add_suffix("_raw")

        # 6. Inyectar metadatos
        df["archivo_origen"] = os.path.basename(archivo_csv)
        df["fecha_carga"] = datetime.now()

        # 7. Carga masiva
        df.to_sql(name=nombre_tabla_stg, con=engine, if_exists="append", index=False)

        LoggerManager.info(f"Cargados {len(df)} registros en {nombre_tabla_stg}")
        return True

    except Exception as e:
        LoggerManager.error(f"Error carga en {nombre_tabla_stg}: {str(e)}")
        return False


# In[3]:


# ============================================
# MAPEO DE ARCHIVOS CSV A TABLAS STAGING
# ============================================
# Se define aquí una sola vez para reutilizar en diagnóstico, carga y validación
archivos_a_procesar = {
    "estudiante.csv": "stg_estudiante",
    "docente.csv": "stg_docente",
    "dictado.csv": "stg_dictado",
    "inscripcion.csv": "stg_inscripcion",
    "examen.csv": "stg_examen",
    "evaluacion_curso.csv": "stg_evaluacion_curso",
    "facultad.csv": "stg_facultad",
    "departamento.csv": "stg_departamento",
    "programa.csv": "stg_programa",
    "curso.csv": "stg_curso",
    "curso_programa.csv": "stg_curso_programa",
}


# In[4]:


# ============================================
# DIAGNÓSTICO PRE-CARGA
# ============================================
# Verificar que TODO está listo ANTES de intentar cargar
diagnóstico_ok = True

# 1. Verificar archivos CSV
LoggerManager.info("\nVerificando archivos en Sources:")
archivos_faltantes = []
for csv in archivos_a_procesar.keys():
    ruta = os.path.join(RUTA_SOURCES, csv)
    existe = os.path.exists(ruta)
    status = "[OK]" if existe else "[ERROR]"
    LoggerManager.info(f"{status} {csv}")
    if existe:
        size = os.path.getsize(ruta) / 1024  # KB
        logger.info(f"Archivo encontrado: {csv} ({size:.2f} KB)")
    else:
        LoggerManager.warning(f"Archivo faltante: {csv}")
        archivos_faltantes.append(csv)
        diagnóstico_ok = False

# 2. Verificar tablas en MySQL
tablas_faltantes = []
try:
    with engine.connect() as conn:
        for tabla in archivos_a_procesar.values():
            try:
                query = text(f"SELECT COUNT(*) FROM {tabla}")
                conn.execute(query)
                LoggerManager.info(f"Tabla existe: {tabla}")
            except Exception:
                print(f"[ERROR] {tabla} NO existe - Necesita ser creada!")
                LoggerManager.error(f"Tabla no existe: {tabla}")
                tablas_faltantes.append(tabla)
                diagnóstico_ok = False
except Exception as e:
    LoggerManager.error(f"Error conexión MySQL: {e}")
    diagnóstico_ok = False

# Resumen del diagnóstico
if diagnóstico_ok:
    LoggerManager.info("Diagnóstico OK: Procediendo a carga")
else:
    LoggerManager.warning("Diagnóstico detectó problemas - revisar antes de proceder")
    if archivos_faltantes:
        LoggerManager.warning(
            f"  - Archivos faltantes: {', '.join(archivos_faltantes)}"
        )
    if tablas_faltantes:
        LoggerManager.warning(f"  - Tablas faltantes: {', '.join(tablas_faltantes)}")


# In[5]:


# ============================================
# EJECUCIÓN DE CARGA IDEMPOTENTE
# ============================================
LoggerManager.info("Iniciando proceso de carga")

resultados = {}
for csv, tabla in archivos_a_procesar.items():
    LoggerManager.info(f"Procesando {csv} -> {tabla}")
    resultados[csv] = cargar_csv_a_staging(csv, tabla)

exitosos = sum(1 for v in resultados.values() if v)
fallidos = len(resultados) - exitosos

LoggerManager.info(f"Carga finalizada: {exitosos} exitosos, {fallidos} fallidos")
LoggerManager.info(f"Exitosos: {exitosos}")
LoggerManager.info(f"Fallidos: {fallidos}")

if fallidos > 0:
    LoggerManager.warning("\nArchivos con fallo:")
    for csv, resultado in resultados.items():
        if not resultado:
            LoggerManager.error(f"Fallo en carga de {csv}")
else:
    LoggerManager.info("Carga completada exitosamente")
