# ADE 2026 — TPI Universidad 🎓

Trabajo Práctico Integrador de **Arquitectura de Datos y ETL (ADE) — 2026**.

Este repositorio contiene un pipeline ETL completo para construir un **Data Warehouse universitario** a partir de archivos CSV, usando una arquitectura de tres capas:

- **Sources**: archivos de entrada
- **Staging**: carga cruda y trazabilidad
- **DWH**: dimensiones y hechos listos para análisis

El proyecto incluye:

- creación automatizada de bases MySQL con Docker
- carga inicial a staging
- transformación y carga al DWH
- tratamiento de alumnos duplicados
- carga incremental simulada
- metabase para visualizacion de datos

---

## Estructura del repositorio

```text
ADE2026_TpiUniversidad/
├── README.md
├── docker-compose.yml
└── TP2/
    ├── logging_config.py
    ├── requirements.txt
    ├── requirements-dev.txt
    ├── .env.ex
    ├── 1-ScriptCreacion_DB/
    │   ├── CreacionSTG_Universidad.sql
    │   └── CreacionDWH_Universidad.sql
    ├── 2-ETL_CargaInicial/
    │   ├── carga_staging.py
    │   ├── transformacion.py
    │   └── README.md
    ├── 3-ETL_Incremental/
    │   ├── carga_incremental.py
    │   └── README.md
    └── Sources/
        ├── *.csv
        └── oltp_universidad_erd.html
```

---

## Flujo actual del proyecto

1. **Creación de bases**
   - `CreacionSTG_Universidad.sql`
   - `CreacionDWH_Universidad.sql`

2. **Carga inicial a staging**
   - `TP2/2-ETL_CargaInicial/carga_staging.py`
   - carga los CSV como datos crudos
   - agrega metadatos de carga
   - enriquece `stg_evaluacion_curso` con `fecha_evaluacion_raw`

3. **Transformación al DWH**
   - `TP2/2-ETL_CargaInicial/transformacion.py`
   - limpia, valida, normaliza y deduplica
   - consolida alumnos duplicados
   - registra duplicados en `stg_reg_repetidos`
   - construye dimensiones y hechos

4. **Carga incremental simulada**
   - `TP2/3-ETL_Incremental/carga_incremental.py`
   - reutiliza la lógica base de transformación

5. **Visualización web**
---

## Modelo de datos actual

### Staging
Las tablas staging almacenan datos crudos con columnas `_raw`, metadatos de archivo y fecha de carga.

Incluyen una tabla auxiliar:

- `stg_reg_repetidos`: registra qué `id_estudiante_raw` fue descartado y cuál es el `id_estudiante_raw` canónico que se usa en la transformación.

### Data Warehouse
El DWH se compone de:

#### Dimensiones
- `dim_tiempo`
- `dim_estudiante`
- `dim_dictado`

#### Hechos
- `fact_inscripcion`
- `fact_examen_estudiante`
- `fact_evaluacion_dictado`

### Granularidad de los hechos
- `fact_inscripcion`: estudiante + dictado + fecha de inscripción
- `fact_examen_estudiante`: estudiante + dictado + intento
- `fact_evaluacion_dictado`: dictado + fecha de evaluación

La evaluación de dictado **no** se modela por estudiante. Solo se conserva el dictado, la fecha y los puntajes.

---

## Reglas de negocio principales

### Consolidación de alumnos duplicados
Los alumnos duplicados se detectan y consolidan en la transformación.

La lógica actual:

- conserva un único `id_estudiante` canónico por alumno
- registra la relación duplicado → canónico en `stg_reg_repetidos`
- usa `stg_reg_repetidos` como fuente de verdad para remapear inscripciones

### Inscripciones repetidas
La carga al hecho `fact_inscripcion` consolida inscripciones repetidas del mismo alumno cuando corresponden a:

- el mismo alumno canónico
- el mismo curso
- el mismo año académico

Si la materia reaparece en otro año académico, se conserva como una recursada válida.

### Evaluaciones de curso
`evaluacion_curso.csv` se enriquece solo con `fecha_evaluacion`.

La fecha se estima así:

- si el dictado tiene calendario académico:
  - `C1` → `YYYY-07-15`
  - `C2` → `YYYY-12-15`
- si no, se usa la primera `fecha_inscripcion` del dictado + 90 días

---

## Ejecución del proyecto


## 1. Requisitos
- Python 3.10+
- Docker Desktop o una instalación local de MySQL 8
- PowerShell, bash o terminal equivalente

## 2. Crear entorno virtual
### PowerShell
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```
### Linux
```bash
python -m venv venv
source venv/bin/activate
```

## 3. Instalar dependencias
### Runtime mínimo
```bash
pip install -r TP2/requirements.txt
```

## 4. Configurar variables de entorno
Crear `TP2/.env` a partir de `TP2/.env.ex`.

Contenido esperado:

```env
DB_USER=root
DB_PASSWORD=root123 # modificar si no usa docker compose
DB_HOST=localhost
DB_PORT=3306
STG_DATABASE=stg_universidad
DWH_DATABASE=dw_universidad
```

---

## Ejecución con Docker

`docker-compose.yml` monta directamente los scripts SQL dentro de `/docker-entrypoint-initdb.d`, por lo que MySQL crea las bases automáticamente al inicializarse por primera vez.

### Levantar MySQL
```bash
docker compose up --build -d
```

### Reinicializar desde cero
```bash
docker compose down -v
docker compose up --build -d
```

### Verificar bases creadas
```bash
docker exec mysql_container mysql -uroot -proot123 -e "SHOW DATABASES;"
```

---

## Ejecutar la carga incremental dentro de Docker usando cron

A continuación se explica cómo ejecutar `TP2/3-ETL_Incremental/carga_incremental.py` dentro de un contenedor Docker y programarlo para que se ejecute diariamente a las 22:00 con `cron`.

### 1) Archivos que necesitas en la raíz del proyecto
- `run_incremental.sh` (ya creado por el setup script). Si no lo tenés, generalo con el contenido del runner.
- `TP2/3-ETL_Incremental/carga_incremental.py` (script de carga incremental).
- `.env` con variables DB en la raíz del proyecto (las mismas que usan los scripts): DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, STG_DATABASE, DWH_DATABASE.

### 2) Dockerfile de ejemplo (crear `Dockerfile.cron` en la raíz)

```dockerfile
FROM python:3.10-slim

# Instalar cron y dependencias del sistema
RUN apt-get update && apt-get install -y cron gcc default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /app

# Copiar el proyecto al contenedor
COPY . /app

# Instalar dependencias Python (ajustar si usás requirements)
RUN pip install --no-cache-dir -r TP2/requirements.txt

# Copiar y dar permisos al runner
RUN chmod +x /app/run_incremental.sh

# Añadir job de cron: ejecutar run_incremental.sh todos los días a las 22:00
RUN echo "0 22 * * * /app/run_incremental.sh >> /app/TP2/3-ETL_Incremental/logs/incremental_cron.log 2>&1" > /etc/cron.d/etl_incremental
RUN chmod 0644 /etc/cron.d/etl_incremental
RUN crontab /etc/cron.d/etl_incremental

# Ejecutar cron en primer plano
CMD ["/usr/sbin/cron", "-f"]
```

### 3) Construir la imagen y levantar el contenedor
```bash
# Construir
docker build -f Dockerfile.cron -t etl-cron:latest .

# Ejecutar (montar volumenes si querés persistir logs fuera del contenedor)
docker run -d --name etl-cron -v "$PWD/TP2/3-ETL_Incremental/logs":/app/TP2/3-ETL_Incremental/logs --env-file .env etl-cron:latest
```

- El contenedor correrá cron en primer plano y ejecutará `/app/run_incremental.sh` a las 22:00 todos los días.
- Los logs se escribirán en la carpeta montada `TP2/3-ETL_Incremental/logs` (si usás `-v` como en el ejemplo).

### 4) Verificar que el job se ejecutó
- Ver archivos de log en la carpeta montada del host, por ejemplo:
```bash
ls -ltr TP2/3-ETL_Incremental/logs
cat TP2/3-ETL_Incremental/logs/incremental_20230517_220000.log
```
- También podés ver el log 'incremental_cron.log' dentro del contenedor:
```bash
docker logs etl-cron | tail -n 200
```

### 5) Parar / eliminar contenedor
```bash
docker stop etl-cron
docker rm etl-cron
```

---

## Ejecución del ETL

### Carga inicial a staging
Ingresar al directorio `TP2/2-ETL_CargaInicial/` y ejecutar:
```bash
python carga_staging.py
```

### Transformación al DWH
Ingresar al directorio `TP2/2-ETL_CargaInicial/` y ejecutar:
```bash
python transformacion.py
```

### Carga incremental simulada
Ingresar al directorio `TP2/3-ETL_Incremental/` y ejecutar:
```bash
python carga_incremental.py
```

### Aplicación web
```bash
docker run -d -p 3000:3000 --name metabase metabase/metabase
```

Luego abrir:

```text
http://localhost:3000
```
Le das click a Comenzar y debes configurar la conexión a la base de datos.  
Si se usa Docker se debe colocar en Host la ip del host donde se está ejecutando el contenedor.
```
hostname -I # en linux
ipconfig # en windows
```

---

## Logs

El proyecto usa un sistema centralizado de logging definido en:

- `TP2/logging_config.py`

Los logs de ejecución se generan dentro de la carpeta `logs/` de cada proceso ETL.

---

## Documentación adicional

- `TP2/README.md`: documentación general del TP2
- `TP2/2-ETL_CargaInicial/README.md`: detalle de la carga inicial y transformación
