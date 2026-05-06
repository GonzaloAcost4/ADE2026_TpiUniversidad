# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
import logging

# Configuración de tus credenciales locales
load_dotenv(override=True)  # Fuerza recarga

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
DATABASE = os.getenv("DB_DATABASE")

# Creamos el motor de conexión
# Formato: dialect+driver://username:password@host:port/database
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# Especificamos la Ruta a la carpeta Sources (desde el directorio actual del notebook)
RUTA_SOURCES = os.path.join(os.getcwd(), '..', 'Sources')
RUTA_SOURCES = os.path.abspath(RUTA_SOURCES)

# ============================================
# CONFIGURACIÓN DE LOGGING
# ============================================

# Crear carpeta logs si no existe
RUTA_LOGS =  os.path.join(os.getcwd(), 'logs')
RUTA_LOGS = os.path.abspath(RUTA_LOGS)
os.makedirs(RUTA_LOGS, exist_ok=True)

log_filename = os.path.join(RUTA_LOGS, f"carga_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"Iniciando carga. Log: {log_filename}")


# %%
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
            logger.error(f"Archivo no encontrado: {ruta_completa}")
            return False

        # 2. TRUNCATE - Limpia tabla para idempotencia
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {nombre_tabla_stg}"))
                conn.commit()
            print(f"  -> Tabla {nombre_tabla_stg} limpiada (TRUNCATE)")
            logger.info(f"TRUNCATE ejecutado en {nombre_tabla_stg}")
        except Exception as e:
            print(f"[ERROR] TRUNCATE {nombre_tabla_stg}: {str(e)}")
            logger.error(f"Fallo TRUNCATE: {str(e)}")
            return False

        # 3. Leer CSV como strings (criterio: VARCHAR en Staging)
        df = pd.read_csv(ruta_completa, sep=',', dtype=str)
        
        if df.empty:
            print(f"[WARN] {archivo_csv} está vacío")
            logger.warning(f"Archivo vacío: {archivo_csv}")
            return True

        # 4. Renombrar columnas con sufijo '_raw'
        df = df.add_suffix('_raw')

        # 5. Inyectar metadatos
        df['archivo_origen'] = os.path.basename(archivo_csv)
        df['fecha_carga'] = datetime.now()

        # 6. Carga masiva
        df.to_sql(name=nombre_tabla_stg, con=engine, if_exists='append', index=False)
        
        print(f"[OK] {len(df)} registros cargados en {nombre_tabla_stg}")
        logger.info(f"Cargados {len(df)} registros en {nombre_tabla_stg}")
        return True
        
    except Exception as e:
        print(f"[ERROR] {nombre_tabla_stg}: {str(e)}")
        logger.error(f"Error carga en {nombre_tabla_stg}: {str(e)}")
        return False


# %%
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

# %%
# ============================================
# DIAGNÓSTICO PRE-CARGA
# ============================================
# Verificar que TODO está listo ANTES de intentar cargar
print("\n" + "=" * 70)
print("DIAGNÓSTICO PRE-CARGA")
print("=" * 70)

diagnóstico_ok = True

# 1. Verificar archivos CSV
print("\n[FILES] Verificando archivos en Sources:")
archivos_faltantes = []
for csv in archivos_a_procesar.keys():
    ruta = os.path.join(RUTA_SOURCES, csv)
    existe = os.path.exists(ruta)
    status = "[OK]" if existe else "[ERROR]"
    print(f"{status} {csv}")
    if existe:
        size = os.path.getsize(ruta) / 1024  # KB
        print(f"   Tamaño: {size:.2f} KB")
    else:
        logger.warning(f"Archivo faltante: {csv}")
        archivos_faltantes.append(csv)
        diagnóstico_ok = False

# 2. Verificar tablas en MySQL
print("\n[DATABASE] Verificando tablas en MySQL:")
tablas_faltantes = []
try:
    with engine.connect() as conn:
        for tabla in archivos_a_procesar.values():
            try:
                query = text(f"SELECT COUNT(*) FROM {tabla}")
                conn.execute(query)
                print(f"[OK] {tabla} existe")
            except:
                print(f"[ERROR] {tabla} NO existe - Necesita ser creada!")
                logger.error(f"Tabla no existe: {tabla}")
                tablas_faltantes.append(tabla)
                diagnóstico_ok = False
except Exception as e:
    print(f"[ERROR] No se pudo conectar a MySQL: {e}")
    logger.error(f"Error conexión MySQL: {e}")
    diagnóstico_ok = False

# Resumen del diagnóstico
print("\n" + "-" * 70)
if diagnóstico_ok:
    print("[OK] DIAGNÓSTICO COMPLETADO: Todo listo para la carga ✓")
    logger.info("Diagnóstico OK: Procediendo a carga")
else:
    print("[WARN] DIAGNÓSTICO CON PROBLEMAS:")
    if archivos_faltantes:
        print(f"  - Archivos faltantes: {', '.join(archivos_faltantes)}")
    if tablas_faltantes:
        print(f"  - Tablas faltantes: {', '.join(tablas_faltantes)}")
    logger.warning("Diagnóstico detectó problemas - revisar antes de proceder")
print("-" * 70)

# %%
# ============================================
# EJECUCIÓN DE CARGA IDEMPOTENTE
# ============================================
print("\n" + "=" * 70)
print("INICIANDO CARGA IDEMPOTENTE CON TRUNCATE + FULL LOAD")
print("=" * 70)
logger.info("Iniciando proceso de carga")

resultados = {}
for csv, tabla in archivos_a_procesar.items():
    logger.info(f"Procesando {csv} -> {tabla}")
    resultados[csv] = cargar_csv_a_staging(csv, tabla)

print("\n" + "=" * 70)
print("RESUMEN DE CARGA")
print("=" * 70)

exitosos = sum(1 for v in resultados.values() if v)
fallidos = len(resultados) - exitosos

print(f"Total archivos procesados: {len(resultados)}")
print(f"[OK] Exitosos: {exitosos}")
print(f"[ERROR] Fallidos: {fallidos}")

if fallidos > 0:
    print("\n[WARN] Archivos con fallo:")
    for csv, resultado in resultados.items():
        if not resultado:
            print(f"  - {csv}")
            logger.error(f"Fallo en carga de {csv}")
else:
    print("\n[OK] CARGA COMPLETADA SIN ERRORES (idempotente)")
    logger.info("Carga completada exitosamente")


# %%
# ============================================
# VALIDACIONES POST-CARGA
# ============================================
def validar_integridad_staging():
    """
    Verifica que las tablas staging fueron cargadas correctamente.
    Usa esta función para confirmar que la carga fue idempotente.
    """
    print("\n" + "=" * 70)
    print("VALIDACIÓN DE INTEGRIDAD - POST CARGA")
    print("=" * 70)
    
    try:
        with engine.connect() as conn:
            print("\nVerificando registros por tabla:\n")
            
            for csv, tabla in archivos_a_procesar.items():
                try:
                    # Query 1: Contar registros
                    query = text(f"SELECT COUNT(*) as total FROM {tabla}")
                    result = conn.execute(query).fetchone()
                    total = result[0] if result else 0
                    
                    # Query 2: Contar NULLs en metadatos
                    query_null = text(f"SELECT COUNT(*) FROM {tabla} WHERE archivo_origen IS NULL OR fecha_carga IS NULL")
                    nulls = conn.execute(query_null).fetchone()[0]
                    
                    status = "[OK]" if nulls == 0 else "[WARN]"
                    print(f"{status} {tabla:25} | Registros: {total:6} | NULLs metadatos: {nulls}")
                    
                    if nulls > 0:
                        logger.warning(f"Metadatos incompletos en {tabla}")
                    
                except Exception as e:
                    print(f"[ERROR] {tabla}: {str(e)}")
                    logger.error(f"Error al verificar {tabla}: {str(e)}")
        
        print("\n[OK] Validación completada")
        logger.info("Validación post-carga completada")
        
    except Exception as e:
        print(f"\n[ERROR] Error en validación: {e}")
        logger.error(f"Error en validación: {e}")

# Ejecutar validación
validar_integridad_staging()
