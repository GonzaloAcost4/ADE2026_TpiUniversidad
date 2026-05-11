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
- aplicación web simple para exploración de datos

---

## Estructura del repositorio

```text
ADE2026_TpiUniversidad/
├── README.md
├── docker-compose.yml
└── TP2/
    ├── README.md
    ├── LOGGING_README.md
    ├── logging_config.py
    ├── requirements.txt
    ├── requirements-dev.txt
    ├── .env.ex
    ├── 1-ScriptCreacion_DB/
    │   ├── CreacionSTG_Universidad.sql
    │   └── CreacionDWH_Universidad.sql
    ├── 2-ETL_CargaInicial/
    │   ├── carga_staging.py
    │   ├── carga_staging.ipynb
    │   ├── transformacion.py
    │   ├── transformacion.ipynb
    │   └── README.md
    ├── 3-ETL_Incremental/
    │   └── carga_incremental.py
    ├── 4-Web_App/
    │   └── app.py
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

5. **Exploración web**
   - `TP2/4-Web_App/app.py`

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

### Entorno de desarrollo con notebooks
```bash
pip install -r TP2/requirements-dev.txt
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
docker compose up -d
```

### Reinicializar desde cero
```bash
docker compose down -v
docker compose up -d
```

### Verificar bases creadas
```bash
docker exec mysql_container mysql -uroot -proot123 -e "SHOW DATABASES;"
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
Ingresar al directorio `TP2/4-Web_App/` y ejecutar:
```bash
pip install flask sqlalchemy pymysql python-dotenv pandas plotly
python app.py
```

Luego abrir:

```text
http://localhost:5000
```

---

## Uso de notebooks

Los notebooks de `2-ETL_CargaInicial` están sincronizados con los scripts `.py` y ejecutan directamente la lógica vigente del proyecto.

- `carga_staging.ipynb`
- `transformacion.ipynb`

---

## Logs

El proyecto usa un sistema centralizado de logging definido en:

- `TP2/logging_config.py`

Los logs de ejecución se generan dentro de la carpeta `logs/` de cada proceso ETL.

---

## Documentación adicional

- `TP2/README.md`: documentación general del TP2
- `TP2/2-ETL_CargaInicial/README.md`: detalle de la carga inicial y transformación
- `TP2/LOGGING_README.md`: configuración y uso del sistema de logs
