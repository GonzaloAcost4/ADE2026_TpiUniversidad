# ADE 2026 — TPI Universidad 🎓

Trabajo Práctico Integrador de la materia **Arquitectura de Datos y ETL (ADE) — 2026**.

El proyecto implementa un pipeline **ETL completo** (Extracción, Transformación y Carga) para construir un **Data Warehouse** de una universidad, partiendo de datos operativos en archivos CSV y cargándolos en una base MySQL con arquitectura de tres capas: **Fuentes → Staging → DWH**.

---

## 📁 Estructura del repositorio
```
ADE2026_TpiUniversidad/
└── TP2/
    ├── README.md                              # Documentación detallada del ETL
    ├── LOGGING_README.md                      # Documentación del sistema de logging
    ├── logging_config.py                      # Módulo centralizado de logging
    ├── requirements.txt                       # Dependencias Python
    ├── .env.ex                                # Template de variables de entorno
    │
    ├── 1-ScriptCreación_DB/
    │   └── CreacionDWH_Universidad.sql        # DDL del Data Warehouse (esquema estrella)
    │   └── CreacionSTG_Universidad.sql        # DDL de la base Staging
    │
    ├── 2-ETL_CargaInicial/
    │   ├── carga_staging.ipynb                # ✅ Carga CSV → Staging
    │   ├── carga_staging.py                   
    │   ├── transformacion.ipynb               # ✅ Transforma Staging → DWH
    │   ├── transformacion.py                  
    │   ├── PLANES_FUTUROS.md                  # Especificación de notebooks
    │   └── README.md                          # Especificación de notebooks
    │
    ├── 3-ETL_Incremental/
    │   ├── carga_incremental.py              
    │   └── README.md
    │
    └── Sources/
        ├── oltp_universidad_erd.html             # Diagrama ERD del modelo OLTP
        └── *.csv                                 # Datos de entrada (11 archivos)
```
---
## 🔄 Flujo ETL
```
[CSVs en Sources/]
       │
       ▼  carga_staging.ipynb
[STG_Universidad]   ← datos crudos (VARCHAR), con metadatos de carga
       │
       ▼  transformacion.ipynb
[dw_universidad]    ← dimensiones y hechos limpios, tipados y deduplicados
       │
       ▼  (pendiente) carga_incremental.py + orquestador.py
[DWH listo para análisis]
       │
       ▼  (futuro) 3-ETL_Incremental/
[Cargas periódicas con SCD]
```
El DWH sigue un **esquema estrella** con:
| Tabla | Tipo |
|---|---|
| `dim_estudiante` | Dimensión SCD Tipo 2 |
| `dim_dictado` | Dimensión SCD Tipo 2 (desnormalizada con Curso, Docente, Facultad)
| `dim_tiempo` | Dimensión calendario |
| `fact_examen_alumno` | Tabla de hechos |
| `fact_evaluacion_dictado` | Tabla de hechos |
| `fact_inscripcion` | Tabla de hechos |
---
## 🚀 Cómo ejecutar
### 1. Requisitos previos
- Python 3.9+
- MySQL 8.0+ corriendo localmente
- Jupyter Notebook o VS Code con extensión Jupyter
- Docker y docker-comppse (opcional, para ejecutar MySQL en un contenedor)
### 2. Instalar dependencias
```bash
cd TP2
pip install -r requirements.txt
```
### 3. Configurar variables de entorno
```bash
cp TP2/.env.ex TP2/.env
# Editar TP2/.env con tus credenciales reales si no usas el contenedor de docker
```
Contenido de `.env`:
```
DB_USER=root
DB_PASSWORD=root123
DB_HOST=localhost
DB_PORT=3306
STG_DATABASE=stg_universidad
DWH_DATABASE=dw_universidad
```
### 4. Crear el contenedor MySQL (opcional)
```bash
docker-compose up -d
```
- Ver estado del contenedor
```bash
docker-compose ps
```
- Detener el contenedor
```bash
docker-compose down
```
### 5. Crear las bases de datos en MySQL
```bash
mysql -u root -p < TP2/1-ScriptCreación_DB/CreacionSTG_Universidad.sql
mysql -u root -p < TP2/1-ScriptCreación_DB/CreacionDWH_Universidad.sql
```
### 5. Ejecutar el ETL (en orden)
```bash
# Paso 1: cargar CSV a Staging
jupyter notebook TP2/2-ETL_CargaInicial/carga_staging.ipynb
# Paso 2: transformar Staging al DWH
jupyter notebook TP2/2-ETL_CargaInicial/transformacion.ipynb
```
- O con python
```bash
# Paso 1: cargar CSV a Staging
python3 TP2/2-ETL_CargaInicial/carga_staging.py
# Paso 2: transformar Staging al DWH
python3 TP2/2-ETL_CargaInicial/transformacion.py
```
### 6. Ejecutar dashboard (opcional)
```bash
cd TP2/4-Web_App
python3 app.py
```
- Dentro de un navegador `localhost:5000`

### Resetear el contenedor
```bash
docker-compose down
docker-compose up --build
```

---
## 📊 Datos de entrada
| Archivo | Filas aprox. | Descripción |
|---|---|---|
| `facultad.csv` | 25 | Facultades de la universidad |
| `departamento.csv` | 58 | Departamentos por facultad |
| `programa.csv` | 50 | Carreras (Grado/Posgrado) |
| `curso.csv` | 45 | Materias con horas y nivel |
| `curso_programa.csv` | 694 | Relación curso ↔ carrera |
| `docente.csv` | 40 | Datos y categoría docente |
| `dictado.csv` | 2.261 | Instancias de cursado por período |
| `evaluacion_curso.csv` | 8.360 | Puntajes de evaluación por dictado |
| `estudiante.csv` | 130.000 | Datos de alumnos |
| `inscripcion.csv` | 1.003.413 | Inscripciones a dictados |
| `examen.csv` | 890.389 | Resultados de exámenes |
---
## 📦 Dependencias
```
pandas==2.2.0
sqlalchemy==2.0.23
pymysql==1.1.0
python-dotenv==1.0.0
cryptography==47.0.0
```
---
## 📝 Estado del proyecto
| Fase | Descripción | Estado |
|---|---|---|
| Scripts SQL | Creación de STG y DWH | ✅ Completado |
| Carga Staging | CSV → Staging (idempotente) | ✅ Completado |
| Transformación | Staging → DWH (limpieza, validación, deduplicación) | ✅ Completado |
| Carga DWH | `carga_dwh.ipynb` con UPSERT |  ✅ Completado |
| Orquestador | Ejecución secuencial del flujo completo | ⏳ En progreso |
| Carga Incremental | SCD, detección de cambios, cargas periódicas | 🔮 Planificado |
---
## 📚 Documentación adicional
- [`TP2/README.md`](TP2/README.md) — Guía detallada del ETL, troubleshooting y convenciones de código
- [`TP2/LOGGING_README.md`](TP2/LOGGING_README.md) — Sistema centralizado de logging
- [`TP2/2-ETL_CargaInicial/PLANES_FUTUROS.md`](TP2/2-ETL_CargaInicial/PLANES_FUTUROS.md) — Especificación de los componentes pendientes
