# ETL de transformación y carga al Data Warehouse

Este directorio contiene la etapa que transforma los datos cargados en `STG_Universidad` y los carga en el modelo dimensional de `dw_universidad`.

El objetivo de esta etapa no es copiar las tablas staging tal como vienen, sino convertirlas en dimensiones y hechos consistentes con el esquema del Data Warehouse.

## Modelo destino

El script `transformacion.py` carga las tablas reales del DWH:

- Dimensiones:
  - `Tiempo`
  - `Alumno`
  - `Dictado`
- Hechos:
  - `Inscripcion`
  - `ExamenAlumno`
  - `EvaluacionDictado`

No se cargan tablas operacionales como `Facultad`, `Departamento`, `Programa`, `Curso`, `Docente` o `Estudiante` porque no existen como tablas independientes en el DWH. Sus datos se integran en las dimensiones `Alumno` y `Dictado`.

## Archivos principales

- `carga_staging.py`: carga los CSV desde `TP2/Sources` hacia las tablas staging.
- `transformacion.py`: limpia, normaliza, transforma y carga el DWH dimensional.
- `logs/`: carpeta donde se guardan los logs de ejecución.
- `transformacion.py.bak`: copia de seguridad del script anterior.

## Cambio realizado sobre `stg_evaluacion_curso`

La tabla de hecho `EvaluacionDictado` del DWH requiere:

- `dictadoSKey`
- `alumnoSKey`
- `tiempoSKey`
- `notaDictado`
- `notaCont`
- `notaGeneral`

La fuente original `evaluacion_curso.csv` solo tenía:

- `id_evaluacion`
- `id_dictado`
- `puntaje_dictado`
- `puntaje_contenido`
- `valoracion_general`

Por eso se agregaron en staging:

- `id_estudiante_raw`
- `fecha_evaluacion_raw`

Estos campos permiten resolver `alumnoSKey` y `tiempoSKey`, que son obligatorios en el DWH.

Estos campos ya quedaron incorporados directamente en:

- `TP2/1-ScriptCreacion_DB/CreacionSTG_Universidad.sql`

Si la tabla `stg_evaluacion_curso` ya fue creada con la estructura anterior, hay que recrearla o actualizarla antes de volver a ejecutar `carga_staging.py`. En este proyecto se dejó el cambio dentro del script principal de creación para que la estructura correcta nazca desde la creación del staging.

## Enriquecimiento de `evaluacion_curso.csv`

El CSV original no trae estudiante ni fecha de evaluación. Para poder cargar la tabla de hecho sin inventar surrogate keys, `carga_staging.py` enriquece `evaluacion_curso.csv` antes de insertarlo en staging.

La regla aplicada es:

1. Se toma cada evaluación por `id_dictado`.
2. Se buscan estudiantes inscriptos en ese mismo `id_dictado` usando `inscripcion.csv`.
3. Se asigna cada evaluación a un estudiante inscripto de manera determinística, ordenando por `id_inscripcion`.
4. Si hay más evaluaciones que inscripciones para un dictado, se reutiliza el orden de inscriptos de forma cíclica.
5. La fecha de evaluación se estima con el calendario académico del dictado:
   - `C1` se carga como `YYYY-07-15`.
   - `C2` se carga como `YYYY-12-15`.
6. Si no se puede obtener el calendario del dictado, se usa como alternativa `fecha_inscripcion + 90 días`.
7. Si no existe inscripción para ese dictado, la evaluación queda sin estudiante y luego se rechaza en transformación.

Esta regla deja explícita la decisión tomada y evita cargar claves inexistentes o valores falsos en el DWH.

## Limpieza general de datos

La limpieza se concentra en la clase `DataCleaner` de `transformacion.py`.

### Strings

Se consideran nulos los valores:

- vacío
- `null`
- `none`
- `n/a`
- `na`
- `sin dato`
- `s/d`

Además:

- Se eliminan espacios al inicio y final.
- Se colapsan espacios internos repetidos.
- Se intenta reparar encoding solo cuando aparecen caracteres típicos de mojibake, como `Ã`, `Â` o `�`.

### Números

Se convierten valores numéricos desde texto.

Reglas:

- La coma decimal se reemplaza por punto.
- Los espacios se eliminan.
- Se aceptan enteros y decimales.
- Si se pide entero y el valor trae decimales, se convierte a entero y se registra una advertencia.
- Si no se puede convertir, el valor queda como nulo.

### Fechas

Se aceptan múltiples formatos:

- `YYYY-MM-DD`
- `DD/MM/YYYY`
- `YYYYMMDD`
- `DD-MM-YYYY`
- `MM-DD-YYYY`
- `YYYY`
- `DD/MM/YY`
- `YYYY/MM/DD`

Si solo viene un año, se interpreta como `YYYY-01-01` cuando corresponde a año de ingreso.

### Género

Se normaliza a:

- `M`
- `F`
- `X`

Ejemplos aceptados:

- `M`, `Masculino`, `Male`, `Hombre`, `1`
- `F`, `Femenino`, `Female`, `Mujer`, `2`
- `X`, `Otro`, `No binario`, `NB`

Valores desconocidos quedan como nulos.

## Criterios de validez por entidad staging

Un registro se considera válido solo si cumple los campos mínimos necesarios para construir dimensiones o hechos. Si no cumple, se rechaza y no pasa a la siguiente etapa.

### `stg_estudiante`

Válido si tiene:

- `id_estudiante` no nulo
- `dni` no nulo y dentro del rango argentino esperado, entre 1.000.000 y 99.999.999
- `apellido` no nulo
- `nombre` no nulo
- `id_programa` no nulo

Se eliminan duplicados por:

1. `id_estudiante`, conservando el primero.
2. `dni`, conservando el primero.

Un estudiante sin programa no se carga porque no se puede construir correctamente la dimensión `Alumno` con atributos de programa.

### `stg_programa`

Válido si tiene:

- `id_programa` no nulo
- `nombre_programa` no nulo

Se eliminan duplicados por `id_programa`, conservando el primero.

### `stg_facultad`

Válido si tiene:

- `id_facultad` no nulo
- `nombre_facultad` no nulo

Se eliminan duplicados por `id_facultad`, conservando el primero.

### `stg_departamento`

Válido si tiene:

- `id_departamento` no nulo
- `nombre_departamento` no nulo

Se eliminan duplicados por `id_departamento`, conservando el primero.

### `stg_docente`

Válido si tiene:

- `id_docente` no nulo
- `apellido_docente` no nulo
- `nombre_docente` no nulo

Se eliminan duplicados por `id_docente`, conservando el primero.

### `stg_curso`

Válido si tiene:

- `id_curso` no nulo
- `nombre_curso` no nulo

Además:

- Horas teóricas negativas pasan a nulo.
- Horas prácticas negativas pasan a nulo.
- Horas de laboratorio negativas pasan a nulo.
- Nivel negativo pasa a nulo.

Se eliminan duplicados por `id_curso`, conservando el primero.

### `stg_dictado`

Válido si tiene:

- `id_dictado` no nulo
- `id_curso` no nulo
- `id_docente` no nulo

Además:

- `cupo_maximo` negativo pasa a nulo.

Se eliminan duplicados por `id_dictado`, conservando el primero.

### `stg_inscripcion`

Válido si tiene:

- `id_inscripcion` no nulo
- `id_estudiante` no nulo
- `id_dictado` no nulo
- `fecha_inscripcion` válida

La fecha es obligatoria porque se usa para resolver `tiempoSKey`.

Se eliminan duplicados por `id_inscripcion`, conservando el primero.

El estado se normaliza, por ejemplo:

- `Activa`, `Activo`, `Inscripto`, `Cursando`, `Regular` pasan a `Activa`.
- `Aprobada`, `Finalizada` pasan a `Aprobada`.
- `Abandonada`, `Baja` pasan a `Abandonada`.
- `Cancelada`, `Anulada`, `Rechazada` pasan a `Cancelada`.

### `stg_examen`

Válido si tiene:

- `id_examen` no nulo
- `id_inscripcion` no nulo
- `fecha` válida
- `nota` entre 0 y 10
- `numero_intento` no nulo y mayor a 0

Se eliminan duplicados por `id_examen`, conservando el primero.

El resultado se convierte a booleano:

- Aprobado, sí, true, 1 pasan a `True`.
- Desaprobado, no, false, 0, ausente o pendiente pasan a `False`.
- Si no hay texto de resultado, se infiere aprobado cuando `nota >= 4`.

### `stg_evaluacion_curso`

Válido si tiene:

- `id_evaluacion` no nulo
- `id_dictado` no nulo
- `id_estudiante` no nulo
- `fecha_evaluacion` válida
- `puntaje_dictado` entre 0 y 10
- `puntaje_contenido` entre 0 y 10
- `valoracion_general` entre 0 y 10

Se eliminan duplicados por `id_evaluacion`, conservando el primero.

Si `id_estudiante_raw` o `fecha_evaluacion_raw` no existen en staging, los registros quedan inválidos para `EvaluacionDictado`, porque no se pueden resolver `alumnoSKey` ni `tiempoSKey`.

## Construcción de dimensiones

### Dimensión `Tiempo`

Se construye desde las fechas usadas por hechos:

- `fecha_inscripcion`
- `fecha` de examen
- `fecha_evaluacion`

El `tiempoSKey` se calcula como `YYYYMMDD`.

Ejemplo:

- `2024-07-15` produce `20240715`.

También se cargan:

- día
- mes en español
- año
- período académico
- marca de feriado, actualmente `False`

### Dimensión `Alumno`

Se construye con datos de estudiantes y programas.

Origen:

- `stg_estudiante`
- `stg_programa`

Se cargan datos personales del alumno y atributos del programa:

- nombre del programa
- tipo de programa
- duración del programa

También se calculan:

- `edadIngreso`
- `valid_from`
- `valid_to`
- `es_actual`

### Dimensión `Dictado`

Se construye denormalizando datos de:

- `stg_dictado`
- `stg_curso`
- `stg_docente`
- `stg_departamento`
- `stg_facultad`

La tabla resultante contiene datos completos del dictado, curso, docente, departamento y facultad.

## Construcción de hechos

### Hecho `Inscripcion`

Se construye desde `stg_inscripcion`.

Transformaciones clave:

- `id_estudiante` se convierte a `alumnoSKey`.
- `id_dictado` se convierte a `dictadoSKey`.
- `fecha_inscripcion` se convierte a `tiempoSKey`.

Solo se carga si las tres surrogate keys existen.

Duplicados eliminados por:

- `alumnoSKey`
- `dictadoSKey`

Se conserva el último registro.

### Hecho `ExamenAlumno`

Se construye desde `stg_examen` más `stg_inscripcion`.

Primero se une examen con inscripción por `id_inscripcion`, para saber qué alumno y dictado corresponden al examen.

Luego se resuelven:

- `alumnoSKey`
- `dictadoSKey`
- `tiempoSKey`

Solo se carga si las tres surrogate keys existen.

Duplicados eliminados por:

- `alumnoSKey`
- `dictadoSKey`
- `nroIntentos`

Se conserva el último registro, ordenado por fecha e `id_examen`.

### Hecho `EvaluacionDictado`

Se construye desde `stg_evaluacion_curso` enriquecida.

Transformaciones clave:

- `id_dictado` se convierte a `dictadoSKey`.
- `id_estudiante` se convierte a `alumnoSKey`.
- `fecha_evaluacion` se convierte a `tiempoSKey`.
- `puntaje_dictado` se carga como `notaDictado`.
- `puntaje_contenido` se carga como `notaCont`.
- `valoracion_general` se carga como `notaGeneral`.

Solo se carga si las tres surrogate keys existen.

Duplicados eliminados por:

- `dictadoSKey`
- `alumnoSKey`
- `tiempoSKey`

Se conserva el último registro, ordenado por fecha e `id_evaluacion`.

## Cuándo se elimina un registro

Un registro puede no llegar al DWH por tres motivos principales:

1. Rechazo en limpieza staging.
   - Falta una clave obligatoria.
   - Una fecha requerida no se puede parsear.
   - Una nota o puntaje está fuera de rango.
   - Un DNI no cumple rango válido.

2. Eliminación por duplicado.
   - El registro es repetido según la clave natural definida para esa entidad.
   - Se conserva el primero o el último según el caso.

3. Rechazo en construcción de hechos.
   - El registro era limpio, pero no se puede resolver una surrogate key.
   - Ejemplo: una inscripción con `id_estudiante` que no existe en `Alumno`.
   - Ejemplo: una evaluación con `id_dictado` que no existe en `Dictado`.
   - Ejemplo: una fecha que no existe en `Tiempo`.

## Orden de ejecución recomendado

1. Crear o recrear staging con `CreacionSTG_Universidad.sql` para asegurar que `stg_evaluacion_curso` tenga `id_estudiante_raw` y `fecha_evaluacion_raw`.
2. Crear el DWH con `CreacionDWH_Universidad.sql`.
3. Ejecutar `carga_staging.py`.
4. Ejecutar `transformacion.py`.

## Salida de consola

La salida de `transformacion.py` fue reducida para mostrar solo información importante:

- etapa actual
- tablas DWH cargadas
- cantidad de registros transformados
- cantidad insertada
- errores
- cantidad final en DWH
- alertas cuando hay rechazados o duplicados

El detalle completo queda en los archivos de log dentro de `logs/`.

## Consideraciones importantes

- La carga del DWH es full refresh: se vacían las tablas antes de cargar.
- Se desactivan temporalmente los checks de foreign keys durante el truncate controlado.
- Las dimensiones se cargan antes que los hechos.
- Las surrogate keys se obtienen después de cargar dimensiones.
- Los hechos no usan IDs naturales; usan surrogate keys del DWH.
- No se inventan surrogate keys inexistentes.
- Si un dato no puede relacionarse con una dimensión, se rechaza en el hecho.
