# ETL de carga inicial y transformación al Data Warehouse

Este directorio contiene la carga inicial del proyecto, desde los CSV de `TP2/Sources` hasta las tablas finales del Data Warehouse `dw_universidad`.

La lógica vigente está implementada en:

- `carga_staging.py`
- `transformacion.py`

Los notebooks:

- `carga_staging.ipynb`
- `transformacion.ipynb`

están sincronizados con esos scripts y ejecutan directamente la misma lógica.

---

## Objetivo de esta etapa

La carga inicial hace dos cosas:

1. **cargar staging** con datos crudos y metadatos de auditoría
2. **transformar y cargar el DWH** con datos limpios, normalizados y deduplicados

No replica el modelo operacional completo en el DWH. En cambio, construye un modelo dimensional orientado a análisis.

---

## Archivos principales

- `carga_staging.py`: carga los CSV en staging
- `transformacion.py`: transforma staging y carga el DWH
- `carga_staging.ipynb`: wrapper sincronizado del script de carga
- `transformacion.ipynb`: wrapper sincronizado del script de transformación
- `logs/`: logs de ejecución

---

## Modelo destino

### Dimensiones
- `dim_tiempo`
- `dim_estudiante`
- `dim_dictado`

### Hechos
- `fact_inscripcion`
- `fact_examen_estudiante`
- `fact_evaluacion_dictado`

---

## Staging

Las tablas de staging almacenan:

- valores crudos en columnas `_raw`
- `archivo_origen`
- `fecha_carga`

Además existe una tabla de trazabilidad:

- `stg_reg_repetidos`

Esta tabla guarda:

- `id_repetido`: `id_estudiante_raw` descartado
- `id_tomado`: `id_estudiante_raw` canónico que se usa en la transformación

La transformación usa `stg_reg_repetidos` como fuente de verdad para remapear inscripciones de alumnos duplicados.

---

## Carga a staging

`carga_staging.py` realiza una carga full refresh por tabla:

- ejecuta `TRUNCATE`
- vuelve a cargar todo el CSV
- agrega metadatos
- conserva los campos crudos como texto

### Enriquecimiento de `evaluacion_curso.csv`

`evaluacion_curso.csv` no trae fecha de evaluación. Antes de insertarlo en staging, el proceso agrega `fecha_evaluacion`.

La fecha se estima con esta regla:

1. si el dictado tiene calendario académico:
   - `C1` → `YYYY-07-15`
   - `C2` → `YYYY-12-15`
2. si no hay calendario, se usa la primera `fecha_inscripcion` del dictado + 90 días
3. si no existe ninguna referencia temporal, el registro queda sin fecha y luego se rechaza en la transformación

La tabla `stg_evaluacion_curso` queda a nivel:

- `id_evaluacion_raw`
- `id_dictado_raw`
- `fecha_evaluacion_raw`
- puntajes

No se identifica al estudiante evaluador en este hecho.

---

## Limpieza y validación

La limpieza se concentra en `DataCleaner` dentro de `transformacion.py`.

### Strings
- elimina espacios sobrantes
- normaliza nulos textuales
- intenta reparar mojibake evidente

### Números
- convierte enteros y flotantes desde texto
- normaliza coma decimal
- deja `NULL` cuando la conversión falla

### Fechas
Acepta múltiples formatos comunes y devuelve `date` cuando puede resolverlos.

### Género
Normaliza a:

- `M`
- `F`
- `X`

---

## Reglas por entidad

### `stg_estudiante`
Un estudiante es válido si tiene:

- `id_estudiante`
- `dni` válido
- `apellido`
- `nombre`
- `id_programa`

La transformación:

- elimina duplicados por `id_estudiante`
- detecta duplicados por `dni`
- elige un `id_estudiante` canónico
- registra el mapeo en `stg_reg_repetidos`

### `stg_programa`, `stg_facultad`, `stg_departamento`, `stg_docente`, `stg_curso`, `stg_dictado`
Cada entidad exige sus claves mínimas y elimina duplicados por su identificador natural.

En `stg_dictado` también se conserva `anio_academico`, porque forma parte de la lógica de inscripciones repetidas.

### `stg_inscripcion`
Una inscripción es válida si tiene:

- `id_inscripcion`
- `id_estudiante`
- `id_dictado`
- `fecha_inscripcion`

El estado se normaliza a categorías consistentes como:

- `Activa`
- `Aprobada`
- `Abandonada`
- `Cancelada`

### `stg_examen`
Un examen es válido si tiene:

- `id_examen`
- `id_inscripcion`
- `fecha`
- `nota` entre 0 y 10
- `numero_intento` mayor a 0

### `stg_evaluacion_curso`
Una evaluación es válida si tiene:

- `id_evaluacion`
- `id_dictado`
- `fecha_evaluacion`
- `puntaje_dictado` entre 0 y 10
- `puntaje_contenido` entre 0 y 10
- `valoracion_general` entre 0 y 10

---

## Construcción de dimensiones

### `dim_tiempo`
Se construye a partir de las fechas usadas por los hechos:

- inscripciones
- exámenes
- evaluaciones

Incluye:

- `tiempo_skey`
- fecha
- día
- mes en español
- año
- período académico

### `dim_estudiante`
Se construye con estudiantes y programas.

Incluye:

- datos personales
- atributos del programa
- cálculo de edad de ingreso
- vigencia tipo SCD

### `dim_dictado`
Se construye denormalizando:

- dictado
- curso
- docente
- departamento
- facultad

---

## Construcción de hechos

### `fact_inscripcion`
Se construye desde `stg_inscripcion`, remapeando previamente alumnos duplicados según `stg_reg_repetidos`.

Transformaciones clave:

- `id_estudiante` → `estudiante_skey`
- `id_dictado` → `dictado_skey`
- `fecha_inscripcion` → `tiempo_skey`

Regla de consolidación:

si un alumno canónico aparece inscripto más de una vez en el mismo:

- curso
- año académico

se conserva una sola inscripción en el hecho.

Si la materia reaparece en otro año académico, se conserva como una recursada válida.

### `fact_examen_estudiante`
Se construye uniendo examen con inscripción para obtener:

- estudiante
- dictado
- fecha

Se deduplica por:

- `estudiante_skey`
- `dictado_skey`
- `n_intentos`

### `fact_evaluacion_dictado`
Se construye a nivel:

- `dictado_skey`
- `tiempo_skey`

Incluye:

- `nota_dictado`
- `nota_cont`
- `nota_general`

Se deduplica por:

- `dictado_skey`
- `tiempo_skey`

---

## Orden de ejecución recomendado

1. crear o recrear las bases con los scripts SQL
2. ejecutar `carga_staging.py`
3. ejecutar `transformacion.py`
4. validar tablas de staging, trazabilidad y DWH

---

## Salida y monitoreo

La consola muestra información resumida de:

- etapas ejecutadas
- registros rechazados
- duplicados detectados
- registros insertados
- cantidad final por tabla

El detalle completo queda en los logs del proceso.

---

## Consideraciones operativas

- la carga del DWH es full refresh
- las dimensiones se cargan antes que los hechos
- las surrogate keys se obtienen después de cargar las dimensiones
- los hechos solo se insertan cuando pueden resolverse las claves necesarias
- `stg_reg_repetidos` centraliza la trazabilidad de alumnos duplicados
- la evaluación de dictado no usa estudiante como dimensión de análisis
