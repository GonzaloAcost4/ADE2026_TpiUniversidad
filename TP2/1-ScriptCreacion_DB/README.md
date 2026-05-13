# 🗄️ Fase 1: Inicialización de Bases de Datos

Este directorio contiene los scripts y herramientas necesarios para inicializar la estructura de las bases de datos del proyecto ETL.

## 📁 Archivos en este directorio

- `CreacionSTG_Universidad.sql`: Script SQL con sentencias `CREATE TABLE` para construir la base de datos de Staging (`stg_universidad`). Esta base de datos actúa como capa intermedia de almacenamiento para los datos crudos extraídos de los CSV originales antes de su limpieza.
- `CreacionDWH_Universidad.sql`: Script SQL con sentencias `CREATE TABLE` para construir la base de datos del Data Warehouse dimensional (`dw_universidad`), incluyendo las tablas de dimensiones, hechos y las claves foráneas necesarias.
- `crear_bd_y_tablas.py`: **Script automatizado** desarrollado en Python para leer y ejecutar los archivos SQL de forma autónoma, utilizando las credenciales y la configuración presentes en tu archivo `.env`.

## 🚀 Cómo inicializar las Bases de Datos

La forma más rápida y segura de crear las estructuras iniciales es utilizando el script de Python, lo cual evita tener que hacerlo manualmente desde el cliente de MySQL.

### 1. Configurar tu `.env`

Asegúrate de tener un archivo `.env` en la raíz de la carpeta `TP2/` con los datos de acceso correctos a tu servidor local de MySQL:

```env
DB_USER="root"
DB_PASSWORD="tu_contraseña"
DB_HOST="127.0.0.1"
DB_PORT="3306"
STG_DATABASE="stg_universidad"
DWH_DATABASE="dw_universidad"
```

### 2. Ejecutar la creación automatizada

Abre la consola en este directorio y ejecuta el script:

```bash
cd TP2/1-ScriptCreacion_DB
python crear_bd_y_tablas.py
```

El script se conectará con las credenciales, reemplazará automáticamente los nombres de base de datos por los que hayas definido en tu archivo `.env` y ejecutará los comandos SQL necesarios para dejar todo listo.

> **Importante:** Si en el futuro necesitas realizar modificaciones estructurales y quieres volver a correr el script para recrear las tablas, deberás borrar las tablas/bases de datos en MySQL previamente. Los scripts intentarán crear las tablas de nuevo y fallarán intencionalmente si ya existen para prevenir cualquier pérdida de información por accidente.
