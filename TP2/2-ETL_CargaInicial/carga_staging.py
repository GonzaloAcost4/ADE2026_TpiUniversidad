#!/usr/bin/env python
# coding: utf-8

"""
ETL Carga Staging - Carga CSV → stg_universidad

Lee los archivos CSV desde Sources y los carga en las tablas staging
con estrategia TRUNCATE + Full Load (idempotente).
"""

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

# Configurar logger para este proceso (logs en carpeta actual: 2-ETL_CargaInicial/logs)
logger = LoggerManager.configurar(
    "carga_staging", ruta_raiz=os.getcwd(), carpeta_logs="logs"
)

# Especificar ruta a la carpeta Sources
RUTA_SOURCES = os.path.join(os.getcwd(), "..", "Sources")
RUTA_SOURCES = os.path.abspath(RUTA_SOURCES)

# Obtener ruta de logs (creada automáticamente por LoggerManager)
RUTA_LOGS = LoggerManager.obtener_ruta_logs()

# ============================================
# MAPEO DE ARCHIVOS CSV A TABLAS STAGING
# ============================================
# Se define aquí una sola vez para reutilizar en diagnóstico, carga y validación
ARCHIVOS_A_PROCESAR = {
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


def enriquecer_evaluacion_curso(df):
    """
    Completa evaluacion_curso.csv con la fecha de evaluación.

    La evaluación es ANÓNIMA, por lo que NO se asigna id_estudiante.
    
    Solo se enriquece fecha_evaluacion usando el período y año académico del dictado:
    - C1 -> 15/07 del año académico
    - C2 -> 15/12 del año académico
    
    Si no se encuentra el dictado, usa la fecha actual como fallback para garantizar
    que NO sea NULL.
    """
    if df.empty:
        return df

    if "fecha_evaluacion" in df.columns:
        return df

    ruta_dictados = os.path.join(RUTA_SOURCES, "dictado.csv")
    
    # Mapear fecha por dictado basado en período y año académico
    fecha_por_dictado = {}
    if os.path.exists(ruta_dictados):
        try:
            dictados = pd.read_csv(ruta_dictados, sep=",", dtype=str)
            for _, row in dictados.iterrows():
                id_dictado = str(row.get("id_dictado", "")).strip()
                anio = str(row.get("anio_academico", "")).strip()
                periodo = str(row.get("periodo", "")).strip().upper()
                
                if id_dictado and anio.isdigit():
                    if periodo in {"C1", "1", "P1", "PRIMER", "PRIMERO"}:
                        fecha_por_dictado[id_dictado] = f"{anio}-07-15"
                    elif periodo in {"C2", "2", "P2", "SEGUNDO"}:
                        fecha_por_dictado[id_dictado] = f"{anio}-12-15"
        except Exception as e:
            LoggerManager.warning(f"No se pudo leer dictado.csv: {e}")
    else:
        LoggerManager.warning("Archivo dictado.csv no encontrado para calcular fechas")

    def calcular_fecha_evaluacion(row):
        """Calcula fecha para evaluación anónima."""
        id_dictado = str(row.get("id_dictado", "")).strip()
        
        # Intentar usar fecha del calendario académico
        if id_dictado in fecha_por_dictado:
            return fecha_por_dictado[id_dictado]
        
        # Fallback: usar fecha actual si no se puede determinar
        return datetime.now().date().isoformat()

    evaluaciones = df.copy()
    evaluaciones["fecha_evaluacion"] = evaluaciones.apply(
        calcular_fecha_evaluacion, axis=1
    )
    
    # Verificar que no haya nulls
    nulos = evaluaciones["fecha_evaluacion"].isna().sum()
    if nulos > 0:
        LoggerManager.warning(f"Se encontraron {nulos} fechas NULL (reemplazadas con hoy)")
        evaluaciones.loc[evaluaciones["fecha_evaluacion"].isna(), "fecha_evaluacion"] = (
            datetime.now().date().isoformat()
        )

    LoggerManager.info(
        f"evaluacion_curso enriquecido: {len(evaluaciones)} registros con fecha_evaluacion (anónimo)"
    )
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


def diagnostico_pre_carga():
    """
    Verifica que todos los archivos CSV existen y las tablas staging están creadas.
    Retorna True si todo está OK, False si hay problemas.
    """
    diagnostico_ok = True

    # 1. Verificar archivos CSV
    LoggerManager.info("\nVerificando archivos en Sources:")
    archivos_faltantes = []
    for csv in ARCHIVOS_A_PROCESAR.keys():
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
            diagnostico_ok = False

    # 2. Verificar tablas en MySQL
    tablas_faltantes = []
    try:
        with engine.connect() as conn:
            for tabla in ARCHIVOS_A_PROCESAR.values():
                try:
                    query = text(f"SELECT COUNT(*) FROM {tabla}")
                    conn.execute(query)
                    LoggerManager.info(f"Tabla existe: {tabla}")
                except Exception:
                    print(f"[ERROR] {tabla} NO existe - Necesita ser creada!")
                    LoggerManager.error(f"Tabla no existe: {tabla}")
                    tablas_faltantes.append(tabla)
                    diagnostico_ok = False
    except Exception as e:
        LoggerManager.error(f"Error conexión MySQL: {e}")
        diagnostico_ok = False

    # Resumen del diagnóstico
    if diagnostico_ok:
        LoggerManager.info("Diagnóstico OK: Procediendo a carga")
    else:
        LoggerManager.warning("Diagnóstico detectó problemas - revisar antes de proceder")
        if archivos_faltantes:
            LoggerManager.warning(
                f"  - Archivos faltantes: {', '.join(archivos_faltantes)}"
            )
        if tablas_faltantes:
            LoggerManager.warning(f"  - Tablas faltantes: {', '.join(tablas_faltantes)}")

    return diagnostico_ok


def ejecutar_carga_staging():
    """
    Punto de entrada principal: diagnóstico + carga completa de CSVs a staging.
    Retorna un dict con los resultados de cada archivo.
    """
    print("\n=== Carga Staging: CSV -> stg_universidad ===", flush=True)

    diagnostico_pre_carga()

    LoggerManager.info("Iniciando proceso de carga")

    resultados = {}
    for csv, tabla in ARCHIVOS_A_PROCESAR.items():
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

    return resultados


if __name__ == "__main__":
    ejecutar_carga_staging()