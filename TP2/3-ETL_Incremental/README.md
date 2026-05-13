# 🔄 ETL Incremental - Fase 3

## 📋 Descripción General

La **Fase 3: Carga Incremental** se ejecutará periódicamente (diaria, semanal, etc.) para incorporar cambios en los datos sin reconstruir el Data Warehouse completo. Esta fase detecta, procesa y carga solo los registros nuevos o modificados.

---

## 🎯 Objetivo General

```
┌─────────────────────────────────────┐
│  Datos Nuevos/Modificados en OLTP   │
│  (Desde última extracción)          │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  DETECTAR CAMBIOS                   │
│  ✓ Nuevos registros                 │
│  ✓ Registros modificados            │
│  ✓ Registros eliminados (soft del)  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  CARGA INCREMENTAL                  │
│  ✓ Procesar delta solamente         │
│  ✓ Aplicar SCD strategies           │
│  ✓ Actualizar dimensiones/hechos    │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  DWH Actualizado                    │
│  ✓ Datos frescos y sincronizados    │
│  ✓ Histórico mantenido              │
│  ✓ Listo para análisis              │
└─────────────────────────────────────┘
```

---

## 📁 Estructura de 3-ETL_Incremental

```text
3-ETL_Incremental/
├── carga_incremental.py          # Script principal de carga y actualización SCD
├── run_test.py                   # Simulador de entorno (inserts + schedule)
├── test_data_incremental.sql     # Archivo SQL con datos de prueba
├── logs/                         # Logs de ejecución (creado dinámicamente)
└── README.md                     # Este archivo
```

---

## 1️⃣ Detectar Cambios: `detectar_cambios.ipynb`

### 📌 Objetivo

Identificar qué registros son nuevos o han sido modificados desde la última ejecución.

### 🔍 Métodos de Detección

#### **Método 1: Timestamp** (Recomendado)

- Usar columna `fecha_modificacion` o `updated_at` en OLTP
- Comparar contra `última_extracción` guardada
- Más eficiente, menos overhead

```python
def detectar_cambios_timestamp(tabla_oltp, fecha_ultima_extraccion):
    """
    Detectar cambios usando timestamp

    Query SQL:
    SELECT * FROM <tabla_oltp>
    WHERE fecha_modificacion > fecha_ultima_extraccion
    """
    query = f"""
    SELECT * FROM {tabla_oltp}
    WHERE fecha_modificacion > '{fecha_ultima_extraccion}'
    """
    df = pd.read_sql(query, engine_oltp)
    return df
```

#### **Método 2: Hash/Checksum** (Backup)

- Calcular SHA256 de cada registro
- Comparar contra checksum anterior
- Útil si no hay timestamp
- Más overhead computacional

```python
def calcular_checksum(registro):
    """Calcular hash SHA256 de un registro"""
    import hashlib

    # Convertir registro a string JSON para hash consistente
    json_str = json.dumps(registro, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode()).hexdigest()

def detectar_cambios_checksum(df_anterior, df_actual):
    """Comparar checksums entre dos snapshots"""
    df_anterior['checksum'] = df_anterior.apply(calcular_checksum, axis=1)
    df_actual['checksum'] = df_actual.apply(calcular_checksum, axis=1)

    # Registros nuevos
    nuevos = df_actual[~df_actual['id'].isin(df_anterior['id'])]

    # Registros modificados (checksum diferente)
    modificados = df_actual[
        (df_actual['id'].isin(df_anterior['id'])) &
        (~df_actual['checksum'].isin(df_anterior['checksum']))
    ]

    return nuevos, modificados
```

#### **Método 3: CDC (Change Data Capture)** (Avanzado)

- Usar features nativas de BD (MySQL Binlog, SQL Server CDC)
- Capturar cambios a nivel de BD
- Más preciso y eficiente para volúmenes grandes

### 📋 Especificación Técnica

#### Inputs:

- Conexión a OLTP (base de datos transaccional)
- Tabla de auditoria en STG: `etl_auditoria_incremental`
- Archivos CSV delta opcionales

#### Outputs:

- DataFrame de registros **nuevos**
- DataFrame de registros **modificados**
- Archivo de log: qué registros fueron detectados
- Actualizar tabla de auditoria con nueva marca de agua

#### Pseudocódigo:

```python
# ============================================
# DETECTAR CAMBIOS
# ============================================

import json
from datetime import datetime

logger = LoggerManager.configurar("detectar_cambios", os.getcwd(), 'logs')

# 1. Cargar timestamp de última extracción desde auditoria
query = "SELECT nueva_extraccion FROM etl_auditoria_incremental WHERE estado='OK' ORDER BY id DESC LIMIT 1"
ultima_fecha = pd.read_sql(query, engine_stg).iloc[0, 0]

logger.info(f"Última extracción: {ultima_fecha}")

# 2. Detectar cambios por tabla
cambios_detectados = {}

for tabla in TABLAS_A_MONITOREAR:
    query = f"SELECT * FROM {tabla} WHERE fecha_mod > '{ultima_fecha}'"
    df_cambios = pd.read_sql(query, engine_oltp)

    if len(df_cambios) > 0:
        cambios_detectados[tabla] = df_cambios
        logger.info(f"Tabla {tabla}: {len(df_cambios)} cambios detectados")
    else:
        logger.info(f"Tabla {tabla}: Sin cambios")

# 3. Actualizar auditoria
total_cambios = sum(len(df) for df in cambios_detectados.values())
logger.info(f"Total de cambios detectados: {total_cambios}")
```

### ✅ Checklist

- [ ] Identificar fuentes de datos (OLTP, archivos, APIs)
- [ ] Implementar detección por timestamp
- [ ] Implementar detección por checksum (backup)
- [ ] Crear tabla de auditoria `etl_auditoria_incremental`
- [ ] Logging de cambios detectados
- [ ] Manejo de tablas sin cambios
- [ ] Probar detección

---

## 2️⃣ Carga Incremental: `carga_incremental.ipynb`

### 📌 Objetivo

Procesar los cambios detectados (nuevos/modificados) e integrarlos en el DWH aplicando estrategias de **Slowly Changing Dimensions (SCD)**.

---

## ✅ Decisiones de diseño en `carga_incremental.py`

### 1) No actualizar `tiempoSKey` en UPSERT de hechos

En `fact_inscripcion` y `fact_examen_estudiante`, el `tiempoSKey` representa la
fecha del evento original (inscripcion o examen). Por eso el UPSERT solo
actualiza columnas de estado/metricas (`estado`, `abandono`, `nota`, `aprobado`)
y **no** sobrescribe la fecha historica.

### 2) Recalcular `numero_intento` con offset historico

Para evitar que el delta pise intentos previos, el incremental consulta el DWH
y calcula el intento real como:

```
intento_real = intentos_previos_en_dwh + 1
```

Luego reescribe `numero_intento` antes de insertar/actualizar el hecho.

Estas dos decisiones evitan la falsificacion de fechas y la corrupcion de
historicos cuando existen reintentos o correcciones en el origen.

### 3) Auditoria en BD y deteccion de duplicados en SQL

La marca de agua se guarda en la tabla `etl_auditoria_incremental` para evitar
inconsistencias por fallos entre escritura del DWH y archivos locales. La
deteccion de duplicados se hace con `INSERT ... SELECT` en SQL para evitar
consumir memoria en pandas.

---

## 🧪 Verificacion rapida (SQL)

Consultar ultimas ejecuciones:

```sql
SELECT id, inicio, fin, ultima_extraccion, nueva_extraccion, estado, registros_delta
FROM etl_auditoria_incremental
ORDER BY id DESC
LIMIT 10;
```

Validar que la ultima ejecucion quedo OK:

```sql
SELECT estado, registros_delta, mensaje_error
FROM etl_auditoria_incremental
ORDER BY id DESC
LIMIT 1;
```

### 🔄 Estrategias de Dimensiones Lentamente Cambiantes

#### **SCD Type 1: Sobrescribir (No mantiene histórico)**

```
┌──────────────────────────────────────┐
│ DIM_ESTUDIANTE (Version Antigua)     │
├──────────────┬──────────────┬────────┤
│ id_estudiante│ nombre       │ carrera│
├──────────────┼──────────────┼────────┤
│ 1            │ Juan García  │ Comp   │
└──────────────┴──────────────┴────────┘
                     │
            Registro Modificado: carrera=Sistemas
                     │
                     ▼
┌──────────────────────────────────────┐
│ DIM_ESTUDIANTE (Version Nueva)       │
├──────────────┬──────────────┬────────┤
│ id_estudiante│ nombre       │ carrera│
├──────────────┼──────────────┼────────┤
│ 1            │ Juan García  │ Sist   │
└──────────────┴──────────────┴────────┘
```

**Uso:** Atributos que cambian pero no se necesita histórico

- Dirección
- Email
- Teléfono

**Implementación:**

```python
def scd_type1(id_registro, nuevos_datos, tabla_dwh, engine_dwh):
    """SCD Type 1: Sobrescribir valores"""

    # Generar UPDATE
    columnas_set = ', '.join([f"{col} = '{val}'" for col, val in nuevos_datos.items()])
    query = f"UPDATE {tabla_dwh} SET {columnas_set} WHERE id = {id_registro}"

    with engine_dwh.connect() as conn:
        conn.execute(text(query))
        conn.commit()
```

#### **SCD Type 2: Agregar nuevo registro (Mantiene histórico completo)**

```
┌──────────────────────────────────────────────────────────┐
│ DIM_ESTUDIANTE (SCD Type 2)                              │
├──────────────┬──────────────┬────────┬──────────┬────────┤
│ id_estudiante│ nombre       │ carrera│ activo   │ fecha_ │
│ _key (PK)    │              │        │          │vigencia│
├──────────────┼──────────────┼────────┼──────────┼────────┤
│ 1            │ Juan García  │ Comp   │ 0        │ 2026-05│
│ 2 (NEW)      │ Juan García  │ Sist   │ 1        │ 2026-05│
└──────────────┴──────────────┴────────┴──────────┴────────┘
```

**Uso:** Atributos que cambian y se necesita histórico

- Carrera
- Departamento
- Programa

**Implementación:**

```python
def scd_type2(id_original, nuevos_datos, tabla_dwh, engine_dwh):
    """SCD Type 2: Crear nueva versión + desactivar antigua"""

    # 1. Desactivar registro anterior
    query_desactivar = f"UPDATE {tabla_dwh} SET activo=0 WHERE id_original={id_original} AND activo=1"

    # 2. Insertar nuevo registro
    nuevos_datos['id_original'] = id_original
    nuevos_datos['activo'] = 1
    nuevos_datos['fecha_vigencia'] = datetime.now()

    with engine_dwh.connect() as conn:
        conn.execute(text(query_desactivar))
        conn.execute(text(insert_query))
        conn.commit()
```

#### **SCD Type 3: Agregar columna anterior (Histórico limitado)**

```
┌────────────────────────────────────────────────────────────┐
│ DIM_ESTUDIANTE (SCD Type 3)                                │
├──────────────┬──────────────┬────────────┬─────────────┬──┤
│ id_estudiante│ nombre       │ carrera    │ carrera_ant │..│
├──────────────┼──────────────┼────────────┼─────────────┼──┤
│ 1            │ Juan García  │ Sistemas   │ Computación │..│
└──────────────┴──────────────┴────────────┴─────────────┴──┘
```

**Uso:** Casos limitados donde se necesita comparación antes/después
**Implementación:** Similar a Type 2 pero con columna de valor anterior

### 📋 Procesamiento de Cambios

```python
def procesar_cambios_incrementales(tabla_dwh, df_cambios, estrategia='type2'):
    """
    Procesar cambios detectados según estrategia SCD

    Args:
        tabla_dwh: Nombre de tabla DWH
        df_cambios: DataFrame con cambios (nuevos + modificados)
        estrategia: 'type1', 'type2', 'type3'
    """

    # Separar nuevos vs modificados
    df_nuevos = df_cambios[df_cambios['es_nuevo'] == True]
    df_modificados = df_cambios[df_cambios['es_nuevo'] == False]

    # Procesar nuevos (siempre INSERT)
    if len(df_nuevos) > 0:
        df_nuevos.to_sql(tabla_dwh, engine_dwh, if_exists='append', index=False)
        logger.info(f"Insertados {len(df_nuevos)} registros nuevos en {tabla_dwh}")

    # Procesar modificados según estrategia
    if len(df_modificados) > 0:
        if estrategia == 'type1':
            # Sobrescribir
            for idx, row in df_modificados.iterrows():
                scd_type1(row['id'], row.to_dict(), tabla_dwh, engine_dwh)

        elif estrategia == 'type2':
            # Nueva versión
            for idx, row in df_modificados.iterrows():
                scd_type2(row['id'], row.to_dict(), tabla_dwh, engine_dwh)

        logger.info(f"Procesados {len(df_modificados)} registros modificados en {tabla_dwh}")
```

### 📋 Especificación Técnica

#### Inputs:

- DataFrames de cambios detectados (newos + modificados)
- Tabla de mapeo: tabla STG → tabla DWH + estrategia SCD

#### Outputs:

- DWH actualizado con cambios aplicados
- Log: qué se insertó/actualizó por tabla
- Reporte de cambios procesados

#### Mapeo de Estrategias (Ejemplo):

```python
mapeo_scd = {
    'dim_estudiante': {
        'estrategia': 'type2',  # Mantener histórico de carreras
        'atributos_type2': ['carrera', 'programa'],
        'tabla_stg': 'stg_estudiante'
    },
    'dim_docente': {
        'estrategia': 'type1',  # No interesa histórico
        'tabla_stg': 'stg_docente'
    },
    'fact_inscripcion': {
        'estrategia': 'type2',  # Registrar histórico de calificaciones
        'tabla_stg': 'stg_inscripcion'
    }
}
```

### ✅ Checklist

- [ ] Definir estrategia SCD para cada dimensión
- [ ] Implementar SCD Type 1
- [ ] Implementar SCD Type 2
- [ ] Implementar SCD Type 3 (si aplica)
- [ ] Separar nuevos vs modificados
- [ ] Procesar cada tabla según estrategia
- [ ] Generar reporte de cambios
- [ ] Logging de cada operación
- [ ] Validación de integridad referencial

---

## 3️⃣ Validación: `validar_incremental.ipynb`

### 📌 Objetivo

Verificar que la carga incremental fue exitosa y consistente.

### 📋 Validaciones a Implementar

#### 1. **Consistencia de Datos**

```python
def validar_integridad_referencial(tabla_fact, fk_columna, tabla_dim):
    """Verificar que todas las FK existen en la dimensión"""

    query = f"""
    SELECT COUNT(*) as huerfanos
    FROM {tabla_fact} f
    LEFT JOIN {tabla_dim} d ON f.{fk_columna} = d.id
    WHERE d.id IS NULL
    """
    result = pd.read_sql(query, engine_dwh)
    huerfanos = result['huerfanos'][0]

    if huerfanos > 0:
        logger.error(f"Integridad: {huerfanos} registros huérfanos en {tabla_fact}")
        return False

    logger.info(f"Integridad: OK en {tabla_fact}")
    return True
```

#### 2. **Conteo de Registros**

```python
def validar_conteos(tabla_dwh, conteo_esperado):
    """Verificar que el conteo es correcto"""

    query = f"SELECT COUNT(*) as total FROM {tabla_dwh}"
    result = pd.read_sql(query, engine_dwh)
    total_actual = result['total'][0]

    if total_actual != conteo_esperado:
        logger.warning(f"Conteo en {tabla_dwh}: esperado {conteo_esperado}, obtenido {total_actual}")
        return False

    logger.info(f"Conteo OK en {tabla_dwh}: {total_actual} registros")
    return True
```

#### 3. **Duplicados**

```python
def validar_sin_duplicados(tabla, columna_pk):
    """Verificar que no hay duplicados por PK"""

    query = f"""
    SELECT {columna_pk}, COUNT(*) as cantidad
    FROM {tabla}
    GROUP BY {columna_pk}
    HAVING cantidad > 1
    """
    result = pd.read_sql(query, engine_dwh)

    if len(result) > 0:
        logger.error(f"Duplicados detectados en {tabla}: {len(result)} registros duplicados")
        return False

    logger.info(f"Sin duplicados OK en {tabla}")
    return True
```

#### 4. **Valores NULL**

```python
def validar_columnas_requeridas(tabla, columnas_requeridas):
    """Verificar que no hay NULLs en columnas obligatorias"""

    for col in columnas_requeridas:
        query = f"SELECT COUNT(*) as nulos FROM {tabla} WHERE {col} IS NULL"
        result = pd.read_sql(query, engine_dwh)
        nulos = result['nulos'][0]

        if nulos > 0:
            logger.error(f"NULLs en {tabla}.{col}: {nulos} registros")
            return False

    logger.info(f"Columnas requeridas OK en {tabla}")
    return True
```

### ✅ Checklist

- [ ] Validar integridad referencial
- [ ] Validar conteos de registros
- [ ] Validar sin duplicados
- [ ] Validar valores requeridos no NULL
- [ ] Comparar con conteo de cambios detectados
- [ ] Generar reporte de validaciones
- [ ] Alertar si validaciones fallan

---

## 🕐 Scheduling y Orquestación

### Frecuencia de Ejecución

```
┌────────────────────────────────────────────┐
│  OPCIONES DE SCHEDULING                    │
├────────────────────────────────────────────┤
│ ✓ Diaria (PM):      23:00 UTC              │
│ ✓ Semanal (Domingo): 02:00 UTC             │
│ ✓ Mensual (1er día): 03:00 UTC             │
│ ✓ Event-driven:     Cuando hay cambios    │
└────────────────────────────────────────────┘
```

### Herramientas de Scheduling

#### **Opción 1: Apache Airflow** (Recomendado)

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'etl_team',
    'start_date': datetime(2026, 5, 6),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('etl_incremental_diario',
          default_args=default_args,
          schedule_interval='0 23 * * *')  # 23:00 UTC diariamente

def ejecutar_detectar_cambios():
    # Ejecutar detectar_cambios.ipynb
    pass

def ejecutar_carga_incremental():
    # Ejecutar carga_incremental.ipynb
    pass

task1 = PythonOperator(task_id='detectar', python_callable=ejecutar_detectar_cambios, dag=dag)
task2 = PythonOperator(task_id='cargar', python_callable=ejecutar_carga_incremental, dag=dag)

task1 >> task2  # Dependency: task1 antes de task2
```

#### **Opción 2: Cron** (Simple, Unix/Linux)

```bash
# /etc/crontab o crontab -e

# Ejecución diaria a las 23:00
0 23 * * * cd /ruta/TP2 && jupyter nbconvert --execute --to notebook 4-ETL_Incremental/detectar_cambios.ipynb && jupyter nbconvert --execute --to notebook 4-ETL_Incremental/carga_incremental.ipynb

# Ejecución semanal domingo 02:00
0 2 * * 0 cd /ruta/TP2 && python 4-ETL_Incremental/orquestador_incremental.py
```

#### **Opción 3: Windows Task Scheduler**

```powershell
# PowerShell script
$action = New-ScheduledTaskAction -Execute "python" -Argument "C:\ruta\orquestador_incremental.py"
$trigger = New-ScheduledTaskTrigger -Daily -At "23:00"
Register-ScheduledTask -TaskName "ETL_Incremental_Diario" -Action $action -Trigger $trigger
```

---

## 📊 Monitoreo y Alertas

### Métricas a Monitorear

```
┌─────────────────────────────────────┐
│  MÉTRICAS ETL INCREMENTAL           │
├─────────────────────────────────────┤
│ ✓ Cambios detectados por tabla      │
│ ✓ Tiempo de ejecución               │
│ ✓ Errores o advertencias            │
│ ✓ Integridad referencial            │
│ ✓ Duplicados detectados             │
│ ✓ Últimas actualizaciones en DWH    │
└─────────────────────────────────────┘
```

### Dashboard Recomendado (Tableau/Power BI)

```
┌──────────────────────────────────────┐
│ ETL INCREMENTAL - DASHBOARD          │
├──────────────┬───────────────────────┤
│ Cambios Hoy  │ 1,250 registros      │
│ Tiempo Eej.  │ 3min 45seg            │
│ Status       │ ✓ EXITOSO             │
│              │                       │
│ Últimas 5 Ejecuciones:               │
│ 2026-05-05 23:00 ✓ 2500 cambios      │
│ 2026-05-04 23:00 ✓ 1800 cambios      │
│ 2026-05-03 23:00 ✓ 950 cambios       │
│ 2026-05-02 23:00 ✗ ERROR             │
│ 2026-05-01 23:00 ⚠ 50 duplicados     │
└──────────────┴───────────────────────┘
```

---

## ✅ Checklist Completo - Fase 3

- [x] Crear estructura de carpeta `3-ETL_Incremental`
- [x] Implementar detección de cambios (Integrado en SQL / auditoría)
- [x] Implementar `carga_incremental.py` (Con SCD Type 1 y Type 2)
- [x] Implementar `run_test.py` con inserción dinámica
- [x] Implementar sistema de scheduling usando la librería `schedule` en `run_test.py`
- [x] Manejo de auditoria (tabla `etl_auditoria_incremental`)
- [x] Documentar en README
- [x] Pruebas end-to-end con `run_test.py`

---

## 📞 Notas Futuras

- **Considerar CDC** (Change Data Capture) si volumen crece
- **Implementar Data Vault** si complejidad aumenta
- **Agregar Machine Learning** para predicción de cambios
- **API REST** para consultar cambios recientes
- **Integración con BI Tools** para análisis en tiempo real

---

**Última actualización:** 5 de mayo de 2026  
**Estado:** Planificación - Fase 3 pendiente  
**Próximo hito:** Implementación de detectar_cambios.ipynb
