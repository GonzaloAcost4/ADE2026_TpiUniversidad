# ADE2026_TpiUniversidad
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
    ├── 1-ScriptCreación_UniversidadDWH/
    │   └── CreacionDWH_Universidad.sql        # DDL del Data Warehouse (esquema estrella)
    │
    ├── 2-ScriptCreación_UniversidadSTG/
    │   └── CreacionSTG_Universidad.sql        # DDL de la base Staging
    │
    ├── 3-ETL_CargaInicial/
    │   ├── carga_staging.ipynb                # ✅ Carga CSV → Staging
    │   ├── transformacion.ipynb               # ✅ Transforma Staging → DWH
    │   └── PLANES_FUTUROS.md                  # Especificación de notebooks pendientes
    │
    ├── 4-ETL_Incremental/                     # 🔮 Fase futura
    │   └── README.md
    │
    └── Sources/
        ├── ADE_TP2_Analisis_de_los_datos.ipynb  # Análisis exploratorio de datos (EDA)
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
       ▼  (pendiente) carga_dwh.ipynb + orquestador.ipynb
[DWH listo para análisis]
       │
       ▼  (futuro) 4-ETL_Incremental/
[Cargas periódicas con SCD]
```
El DWH sigue un **esquema estrella** con:
| Tabla | Tipo |
|---|---|
| `Alumno` | Dimensión SCD Tipo 2 |
| `Dictado` | Dimensión SCD Tipo 2 (desnormalizada con Curso, Docente, Facultad) |
| `Tiempo` | Dimensión calendario |
| `ExamenAlumno` | Tabla de hechos |
| `EvaluacionDictado` | Tabla de hechos |
| `Inscripcion` | Tabla de hechos |
---
## 🚀 Cómo ejecutar
### 1. Requisitos previos
- Python 3.9+
- MySQL 8.0+ corriendo localmente
- Jupyter Notebook o VS Code con extensión Jupyter
### 2. Instalar dependencias
```bash
cd TP2
pip install -r requirements.txt
```
### 3. Configurar variables de entorno
```bash
cp TP2/.env.ex TP2/.env
# Editar TP2/.env con tus credenciales reales
```
Contenido de `.env`:
```
DB_USER=root
DB_PASSWORD=tu_contraseña
DB_HOST=localhost
DB_PORT=3306
STG_DATABASE=stg_universidad
DWH_DATABASE=dw_universidad
```
### 4. Crear las bases de datos en MySQL
```bash
mysql -u root -p < TP2/2-ScriptCreación_UniversidadSTG/CreacionSTG_Universidad.sql
mysql -u root -p < TP2/1-ScriptCreación_UniversidadDWH/CreacionDWH_Universidad.sql
```
### 5. Ejecutar el ETL

**A. Carga Inicial (Full Load)**
Este proceso borra todo el staging, extrae los CSV completos y carga las dimensiones y hechos en el Data Warehouse desde cero.
```bash
cd TP2/2-ETL_CargaInicial
python orquestador.py
```

**B. Carga Incremental**
Este proceso detecta los cambios nuevos en Staging y los integra al Data Warehouse.
```bash
cd TP2/3-ETL_Incremental
python carga_incremental.py
```

**C. Simulador Automático (Testing)**
Si querés ver el incremental en acción, podés lanzar el simulador que insertará datos de prueba periódicamente y lanzará el script incremental de forma automática.
```bash
cd TP2/3-ETL_Incremental
python run_test.py
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
| Carga DWH | `carga_dwh.ipynb` con UPSERT | ⏳ En progreso |
| Orquestador | Ejecución secuencial del flujo completo | ⏳ En progreso |
| Carga Incremental | SCD, detección de cambios, cargas periódicas | 🔮 Planificado |
---
## 📚 Documentación adicional
- [`TP2/README.md`](TP2/README.md) — Guía detallada del ETL, troubleshooting y convenciones de código
- [`TP2/LOGGING_README.md`](TP2/LOGGING_README.md) — Sistema centralizado de logging
- [`TP2/3-ETL_CargaInicial/PLANES_FUTUROS.md`](TP2/3-ETL_CargaInicial/PLANES_FUTUROS.md) — Especificación de los componentes pendientes