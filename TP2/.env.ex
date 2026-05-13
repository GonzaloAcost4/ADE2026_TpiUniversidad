Configuración necesaria del .env para conectar con MySQL
Para Correrlo en Local
DB_USER="root"
DB_PASSWORD="tu_contraseña" # El que configuraste al instalar MySQL
DB_HOST="mysql_db"
DB_PORT="3306"
STG_DATABASE="stg_universidad"
DWH_DATABASE="dw_universidad"

Para Correrlo en el Docker 
USER = "root"
DB_PASSWORD = "root123"  
DB_HOST = "127.0.0.1"
DB_PORT = "3306"
STG_DATABASE = "stg_universidad"
DWH_DATABASE = "dw_universidad"