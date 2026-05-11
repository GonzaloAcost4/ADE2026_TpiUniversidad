#!/usr/bin/env python
# coding: utf-8

# In[1]:

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Agregar ruta del proyecto al path para importar módulos
sys.path.append(os.path.join(os.getcwd(), ".."))

# Importar el LoggerManager
from logging_config import LoggerManager

# Configuración de credenciales
load_dotenv(override=True)

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
DATABASE = os.getenv("STG_DATABASE")

# Crear motor de conexión
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# Configurar logger para este proceso (logs en carpeta actual: 3-ETL_CargaInicial/logs)
logger = LoggerManager.configurar(
    "carga_staging", ruta_raiz=os.getcwd(), carpeta_logs="logs"
)

# Especificar ruta a la carpeta Sources
RUTA_SOURCES = os.path.join(os.getcwd(), "..", "Sources")
RUTA_SOURCES = os.path.abspath(RUTA_SOURCES)

# Obtener ruta de logs (creada automáticamente por LoggerManager)
RUTA_LOGS = LoggerManager.obtener_ruta_logs()


# In[2]:


def enriquecer_evaluacion_curso(df):
    """
    Completa evaluacion_curso.csv con los datos mínimos que requiere el DWH.

    El CSV original tiene evaluación a nivel dictado. Como la granularidad del
    hecho es dictado + fecha, solo se enriquece `fecha_evaluacion`.

    Regla:
    - si existe calendario del dictado: C1 -> 15/07 del año académico, C2 -> 15/12
    - si no, se usa la primera fecha_inscripcion del dictado + 90 días
    """
    if df.empty:
        return df

    if "fecha_evaluacion" in df.columns:
        return df

    ruta_inscripciones = os.path.join(RUTA_SOURCES, "inscripcion.csv")
    ruta_dictados = os.path.join(RUTA_SOURCES, "dictado.csv")

    evaluaciones = df.copy()

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

    fecha_fallback_por_dictado = {}
    if os.path.exists(ruta_inscripciones):
        inscripciones = pd.read_csv(ruta_inscripciones, sep=",", dtype=str)
        if not inscripciones.empty and {"id_dictado", "fecha_inscripcion"}.issubset(
            inscripciones.columns
        ):
            inscripciones["fecha_inscripcion"] = pd.to_datetime(
                inscripciones["fecha_inscripcion"], errors="coerce"
            )
            primeras = (
                inscripciones.dropna(subset=["fecha_inscripcion"])
                .groupby("id_dictado", as_index=False)["fecha_inscripcion"]
                .min()
            )
            for _, row in primeras.iterrows():
                fecha_fallback_por_dictado[str(row["id_dictado"])] = (
                    (row["fecha_inscripcion"] + timedelta(days=90)).date().isoformat()
                )

    def calcular_fecha_evaluacion(row):
        id_dictado = str(row.get("id_dictado", ""))
        return fecha_por_dictado.get(id_dictado) or fecha_fallback_por_dictado.get(
            id_dictado
        )

    evaluaciones["fecha_evaluacion"] = evaluaciones.apply(
        calcular_fecha_evaluacion, axis=1
    )

    sin_fecha = evaluaciones["fecha_evaluacion"].isna().sum()
    if sin_fecha > 0:
        LoggerManager.warning(
            f"Evaluaciones sin fecha asignable por falta de calendario o inscripciones: {sin_fecha}"
        )

    LoggerManager.info("evaluacion_curso enriquecido con fecha_evaluacion")
    return evaluaciones


def cargar_csv_a_staging(archivo_csv, nombre_tabla_stg):
    """
    Lee un CSV desde Sources, lo carga con TRUNCATE (idempotente).

    Estrategia: TRUNCATE + Full Load
    - Borra datos previos de la tabla
    - Carga datos completos y frescos
    - Garantiza NO hay duplicados
    - Seguro ejecutar múltiples veces

    Args:
        archivo_csv: nombre del archivo CSV (ej: 'estudiante.csv')
        nombre_tabla_stg: nombre de la tabla staging en MySQL
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
