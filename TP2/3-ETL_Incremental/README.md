# ETL incremental del Data Warehouse

Este directorio contiene la carga incremental del proyecto para `dw_universidad`.

La ejecución incremental reutiliza la misma lógica base de limpieza, normalización y validación definida en `TP2/2-ETL_CargaInicial/transformacion.py`, y procesa únicamente los registros nuevos detectados en staging.

El proceso está implementado en:

- `carga_incremental.py`

---

## Objetivo

La carga incremental permite incorporar nuevos datos al Data Warehouse sin reconstruir completamente todas las tablas.

El flujo actual:

- detecta deltas en staging usando `fecha_carga`
- limpia y normaliza esos deltas
- remapea alumnos duplicados según `stg_reg_repetidos`
- inserta nuevas fechas en `dim_tiempo`
- aplica actualización incremental sobre `dim_estudiante` y `dim_dictado`
- inserta hechos nuevos evitando duplicados
- registra el control de ejecución en un archivo local

---

## Archivos y estructura

```text
TP2/3-ETL_Incremental/
├── carga_incremental.py
├── logs/
└── datos_control/
    └── ultima_extraccion.json
```

### `carga_incremental.py`
Orquesta toda la carga incremental.

### `datos_control/ultima_extraccion.json`
Guarda:

- fecha de última extracción procesada
- historial resumido de ejecuciones
- métricas de dimensiones y hechos insertados

---

## Fuente de los cambios

La carga incremental lee cambios desde las tablas staging.

Las tablas monitoreadas son:

- `stg_facultad`
- `stg_departamento`
- `stg_programa`
- `stg_curso`
- `stg_docente`
- `stg_estudiante`
- `stg_dictado`
- `stg_inscripcion`
- `stg_examen`
- `stg_evaluacion_curso`

La detección se basa en el campo:

- `fecha_carga`

---

## Detección de cambios

La función `leer_delta_staging()` resuelve qué registros deben procesarse.

### Ejecución con una extracción previa registrada
Si existe una `fecha_ultima_extraccion`, el delta se obtiene así:

- se leen solo las filas con `fecha_carga > fecha_ultima_extraccion`
- los registros se ordenan por `row_id`

Conceptualmente:

```text
SELECT *
FROM tabla_staging
WHERE fecha_carga > ultima_extraccion
ORDER BY row_id
```

### Primera ejecución incremental
Si todavía no existe control previo:

- se toma una muestra reciente de cada tabla staging
- se usa `ORDER BY row_id DESC LIMIT ...`
- luego el conjunto se vuelve a ordenar ascendentemente por `row_id`

La cantidad de filas iniciales se controla con:

- `LIMITE_DELTA_INICIAL`

Esto permite simular la carga incremental sin reprocesar todo staging en la primera corrida.

---

## Limpieza y normalización

Cada delta se procesa con las funciones base de transformación ya usadas en la carga inicial.

Ejemplos:

- `transformar_estudiante_base`
- `transformar_dictado_base`
- `transformar_inscripcion_base`
- `transformar_examen_base`
- `transformar_evaluacion_base`

Esto garantiza que la carga incremental use exactamente los mismos criterios de:

- validación
- normalización
- parseo de fechas
- deduplicación técnica
- tipado

---

## Tratamiento de alumnos duplicados

Antes de construir hechos, la carga incremental consulta:

- `stg_reg_repetidos`

Esa tabla actúa como fuente de verdad para saber:

- qué `id_estudiante` fue descartado
- qué `id_estudiante` canónico se debe usar

Con ese mapa se remapean las inscripciones del delta y, cuando hace falta, también las inscripciones base usadas para resolver exámenes.

De esta forma, la carga incremental mantiene la misma lógica de consolidación de alumnos duplicados que la carga inicial.

---

## Inserción incremental de `dim_tiempo`

El proceso reúne fechas nuevas desde:

- inscripciones
- exámenes
- evaluaciones

Con esas fechas construye el subconjunto correspondiente de `dim_tiempo` usando `construir_dim_tiempo()` e inserta solo los registros nuevos con `INSERT IGNORE`.

La tabla usada es:

- `dim_tiempo`

---

## Actualización incremental de dimensiones

La carga incremental gestiona estas dimensiones:

- `dim_estudiante`
- `dim_dictado`

La estrategia combina:

- SCD Tipo 2 para atributos históricos
- SCD Tipo 1 para atributos que deben sobrescribirse en la fila vigente

---

## Reglas de `dim_estudiante`

### Clave natural
- `id_estudiante`

### SCD Tipo 2
Si cambia alguno de estos campos, se expira la fila actual y se inserta una nueva versión:

- `anio_plan_prog`

### SCD Tipo 1
Si cambia alguno de estos campos, se actualiza la fila vigente sin generar una nueva versión:

- `genero`
- `egreso_carrera`
- `anio_egreso`
- `abandono_carrera`
- `anio_abandono`

### Comportamiento
- si no existe fila actual para ese `id_estudiante`, se inserta una nueva
- si cambia un campo SCD2:
  - `valid_to` de la fila anterior se completa con la fecha actual
  - `es_actual` de la fila anterior pasa a `FALSE`
  - se inserta una nueva fila con `valid_from` actual, `valid_to = NULL` y `es_actual = TRUE`
- si cambia solo un campo SCD1:
  - se actualiza la fila actual en el mismo registro
- si no cambia nada:
  - no se inserta ni actualiza nada

---

## Reglas de `dim_dictado`

### Clave natural
- `id_dictado`

### SCD Tipo 2
Si cambia alguno de estos campos, se expira la fila actual y se inserta una nueva versión:

- `periodo`
- `turno`
- `horas_teoria`
- `horas_practica`
- `horas_lab`
- `nivel_curso`
- `nombre_docente`
- `apellido_docente`
- `titulo_docente`
- `categoria_docente`
- `dedicacion_docente`

### SCD Tipo 1
Si cambia alguno de estos campos, se actualiza la fila vigente sin generar una nueva versión:

- `aula`
- `cupo_max`
- `nombre_curso`

### Comportamiento
- si no existe fila actual para ese `id_dictado`, se inserta una nueva
- si cambia un campo SCD2:
  - se expira la versión vigente
  - se inserta una nueva versión actual
- si cambia solo un campo SCD1:
  - se actualiza la fila vigente en la misma versión
- si no cambia nada:
  - no se modifica la dimensión

---

## Cómo se detectan los cambios en dimensiones

La lógica incremental compara cada registro nuevo contra la fila vigente actual del DWH.

El proceso es:

1. buscar la fila actual por clave natural (`es_actual = TRUE`)
2. comparar campos SCD2
3. si no hubo cambios SCD2, comparar campos SCD1
4. aplicar la estrategia correspondiente

La comparación se hace campo por campo, usando los nombres reales de columnas del DWH actual.

---

## Inserción incremental de hechos

Después de actualizar dimensiones y tiempo, la carga incremental obtiene los mapas de surrogate keys actuales y construye los hechos.

Se insertan estas tablas:

- `fact_inscripcion`
- `fact_examen_estudiante`
- `fact_evaluacion_dictado`

La inserción usa `INSERT IGNORE`, por lo que si una fila ya existe según las restricciones únicas del DWH, no se duplica.

### `fact_inscripcion`
Se construye con `construir_fact_inscripcion()`.

Reglas relevantes:

- remapea alumnos duplicados antes de construir el hecho
- resuelve `estudiante_skey`, `dictado_skey` y `tiempo_skey`
- consolida inscripciones repetidas del mismo alumno canónico en el mismo:
  - curso
  - año académico

### `fact_examen_estudiante`
Se construye con `construir_fact_examen_estudiante()`.

Reglas relevantes:

- usa inscripciones para resolver alumno y dictado del examen
- remapea alumnos duplicados en las inscripciones base cuando corresponde
- inserta exámenes nuevos sin duplicar intentos ya existentes

### `fact_evaluacion_dictado`
Se construye con `construir_fact_evaluacion_dictado()`.

Reglas relevantes:

- trabaja a nivel `dictado + fecha`
- no usa estudiante como dimensión del hecho
- inserta nuevas evaluaciones sin duplicar por `dictado_skey` y `tiempo_skey`

---

## Control de ejecución

Al finalizar, la corrida actualiza `datos_control/ultima_extraccion.json` con:

- fecha de ejecución
- fecha previa procesada
- cantidad total de registros delta
- fechas insertadas en `dim_tiempo`
- métricas de `dim_estudiante`
- métricas de `dim_dictado`
- hechos insertados

Ese archivo permite que la siguiente corrida sepa desde qué instante continuar.

---

## Salida de consola

La ejecución informa:

- última extracción registrada
- cantidad de registros delta por tabla
- alertas de rechazados o duplicados en staging
- inserciones en tiempo
- inserciones y actualizaciones SCD1/SCD2 en dimensiones
- hechos insertados
- actualización del archivo de control

---

## Ejecución

```bash
python TP2/3-ETL_Incremental/carga_incremental.py
```

---

## Consideraciones operativas

- no ejecuta `TRUNCATE` del DWH
- reutiliza la lógica base de la carga inicial
- depende de que staging tenga `fecha_carga` correctamente poblada
- usa `stg_reg_repetidos` para consolidación de alumnos duplicados
- gestiona histórico en dimensiones mediante `valid_from`, `valid_to` y `es_actual`
- inserta hechos con estrategia incremental y sin duplicación lógica
