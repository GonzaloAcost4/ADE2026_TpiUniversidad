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

## 1️⃣ Detectar Cambios: `detectar_cambios.py`

### 📌 Objetivo

Identificar qué registros son nuevos o han sido modificados desde la última ejecución.

### 🔍 Método de Detección Implementado

- **Timestamp/Marcas de Agua:** Se utiliza la marca de agua (`última_extracción`) guardada en la tabla `etl_auditoria_incremental`.
- Se compara con los eventos nuevos en origen.
- Más eficiente, menos overhead.

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

---

## 2️⃣ Carga Incremental: `carga_incremental.py`

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

## 🧪 Verificacion rapida

Se puede consultar la tabla `etl_auditoria_incremental` para revisar las últimas ejecuciones y validar que el estado haya quedado en `OK` y revisar los registros delta procesados.

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

### 📋 Especificación Técnica

#### Inputs:

- DataFrames de cambios detectados (nuevos + modificados)
- Estrategia SCD a aplicar según cada tabla/dimensión.

#### Outputs:

- DWH actualizado con cambios aplicados
- Log: qué se insertó/actualizó por tabla
- Reporte de cambios procesados

---

## 3️⃣ Scheduling y Orquestación

### Herramienta Implementada

- **Schedule en Python (`run_test.py`):** Solución ligera basada en la librería `schedule` para ambientes locales o controlados, que automatiza la simulación y carga periódica del pipeline incremental de forma desatendida.

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
**Última actualización:** 13 de mayo de 2026  
**Estado:** Fase 3 Completada  
**Próximo hito:** Mantenimiento de las rutinas incrementales y optimización.
