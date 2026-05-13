import os
import pymysql
from dotenv import load_dotenv
from pathlib import Path

# Rutas
current_dir = Path(__file__).resolve().parent
env_path = current_dir.parent / '.env'

# Cargar variables de entorno desde el archivo .env
load_dotenv(dotenv_path=env_path)

# Obtener variables de entorno (los nombres coinciden con lo propuesto en .env.ex)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
STG_DATABASE = os.getenv("STG_DATABASE", "stg_universidad")
DWH_DATABASE = os.getenv("DWH_DATABASE", "dw_universidad")

def ejecutar_script_sql(cursor, ruta_script, old_db_name, new_db_name):
    """
    Lee un archivo SQL, reemplaza el nombre de la base de datos por el configurado en .env
    y ejecuta cada comando individualmente.
    """
    with open(ruta_script, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # Reemplazar nombre de BD original del script por el configurado en el .env
    sql_content = sql_content.replace(old_db_name, new_db_name)
    
    # Separar comandos por ; y ejecutar
    comandos = sql_content.split(';')
    for comando in comandos:
        comando_limpio = comando.strip()
        if comando_limpio:
            try:
                cursor.execute(comando_limpio)
            except Exception as e:
                print(f"Error al ejecutar: {comando_limpio[:50]}...")
                print(f"Error: {e}")
                raise

def main():
    if not all([DB_USER, DB_PASSWORD, DB_HOST]):
        print("Error: Faltan variables de entorno necesarias (DB_USER, DB_PASSWORD, DB_HOST). Verifique su archivo .env en la carpeta TP2.")
        return

    print(f"Conectando a MySQL en {DB_HOST}:{DB_PORT} con usuario '{DB_USER}'...")
    
    try:
        # Nos conectamos sin especificar base de datos porque recién las vamos a crear
        conexion = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=int(DB_PORT),
            autocommit=True
        )
        
        with conexion.cursor() as cursor:
            # 1. Ejecutar script STG
            ruta_stg = current_dir / 'CreacionSTG_Universidad.sql'
            print(f"Ejecutando script de Staging: {ruta_stg.name}...")
            ejecutar_script_sql(cursor, ruta_stg, "stg_universidad", STG_DATABASE)
            print(f"[*] Base de datos y tablas de {STG_DATABASE} creadas exitosamente.")

            # 2. Ejecutar script DWH
            ruta_dwh = current_dir / 'CreacionDWH_Universidad.sql'
            print(f"Ejecutando script de Data Warehouse: {ruta_dwh.name}...")
            ejecutar_script_sql(cursor, ruta_dwh, "dw_universidad", DWH_DATABASE)
            print(f"[*] Base de datos y tablas de {DWH_DATABASE} creadas exitosamente.")

        conexion.close()
        print("\n¡Proceso de creación completado con éxito!")
        
    except pymysql.MySQLError as e:
        print(f"Error de conexión o de base de datos MySQL: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

if __name__ == '__main__':
    main()
