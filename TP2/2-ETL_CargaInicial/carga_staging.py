import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
import sys

# Agregar ruta del proyecto al path para importar módulos
sys.path.append(os.path.join(os.getcwd(), '..'))

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
logger = LoggerManager.configurar("carga_staging", ruta_raiz=os.getcwd(), carpeta_logs='logs')

# Especificar ruta a la carpeta Sources
RUTA_SOURCES = os.path.join(os.getcwd(), '..', 'Sources')
RUTA_SOURCES = os.path.abspath(RUTA_SOURCES)

# Obtener ruta de logs (creada automáticamente por LoggerManager)
RUTA_LOGS = LoggerManager.obtener_ruta_logs()

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
        df = pd.read_csv(ruta_completa, sep=',', dtype=str)
        
        if df.empty:
            LoggerManager.warning(f"Archivo vacío: {archivo_csv}")
            return True

        # 4. Renombrar columnas con sufijo '_raw'
        df = df.add_suffix('_raw')

        # 5. Inyectar metadatos
        df['archivo_origen'] = os.path.basename(archivo_csv)
        df['fecha_carga'] = datetime.now()

        # 6. Carga masiva
        df.to_sql(name=nombre_tabla_stg, con=engine, if_exists='append', index=False)
        
        LoggerManager.info(f"Cargados {len(df)} registros en {nombre_tabla_stg}")
        return True
        
    except Exception as e:
        LoggerManager.error(f"Error carga en {nombre_tabla_stg}: {str(e)}")
        return False
    
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
    "curso_programa.csv": "stg_curso_programa"
}

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
            except:
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
        LoggerManager.warning(f"  - Archivos faltantes: {', '.join(archivos_faltantes)}")
    if tablas_faltantes:
        LoggerManager.warning(f"  - Tablas faltantes: {', '.join(tablas_faltantes)}")

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
