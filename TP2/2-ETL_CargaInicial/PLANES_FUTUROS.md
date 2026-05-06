# 📋 Planes Futuros - Fase 2 (Carga Inicial)

## Descripción General

Este archivo documenta los componentes que faltan para completar la **Fase 2: Carga Inicial** del proyecto ETL Universidad.

---

## 1️⃣ Carga en DWH: `carga_dwh.ipynb`

### 📌 Objetivo

Ejecutar la carga final de datos transformados desde Staging hacia el Data Warehouse.

### 🔄 Flujo Esperado

```
┌─────────────────────────────────────┐
│  Datos en Staging (STG) - Limpio    │
│  ✓ Sin duplicados                   │
│  ✓ Tipos convertidos                │
│  ✓ Validaciones pasadas             │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  carga_dwh.ipynb                    │
│  ────────────────                   │
│  1. Leer tablas STG                 │
│  2. Validar integridad referencial  │
│  3. UPSERT en DWH                   │
│  4. Log de cambios                  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  DWH Actual (Dimensiones + Hechos)  │
│  ✓ Datos cargados                   │
│  ✓ Integridad garantizada           │
│  ✓ Listo para análisis              │
└─────────────────────────────────────┘
```

### 📋 Especificación Técnica

#### Inputs:

- Tablas Staging: `stg_*` (ya limpias y transformadas)
- Variables de entorno: `DB_*` credenciales

#### Outputs:

- Tablas DWH: `dim_*`, `fact_*` cargadas
- Log de ejecución: `carga_dwh_YYYYMMDD_HHMMSS.log`
- Reporte: Cantidad de registros insertados/actualizados

#### Procesos Clave:

**1. Inicialización**

```python
# Conectar a bases de datos
engine_stg = create_engine(f"mysql+pymysql://.../{STG_DB}")
engine_dwh = create_engine(f"mysql+pymysql://.../{DW_DB}")

# Configurar logging
logger = LoggerManager.configurar("carga_dwh",
                                 ruta_raiz=os.getcwd(),
                                 carpeta_logs='logs')
```

**2. Mapeo de Tablas**

```python
# Definir qué tablas STG van a qué tablas DWH
mapeo_tablas = {
    'stg_estudiante': 'dim_estudiante',
    'stg_docente': 'dim_docente',
    'stg_facultad': 'dim_facultad',
    'stg_departamento': 'dim_departamento',
    'stg_programa': 'dim_programa',
    'stg_curso': 'dim_curso',
    'stg_dictado': 'fact_dictado',
    'stg_inscripcion': 'fact_inscripcion',
    'stg_examen': 'fact_examen',
    'stg_evaluacion_curso': 'fact_evaluacion_curso',
    'stg_curso_programa': 'fact_curso_programa'
}
```

**3. Estrategia de Carga**

Para esta carga inicial (no incremental):

- **TRUNCATE + INSERT:** Limpiar la tabla DWH y cargar todo desde STG
- **Rationale:** Asegura sincronización exacta con Staging
- **Alternativa:** Si DWH tiene datos históricos importantes, usar INSERT con validación de duplicados

```python
def cargar_tabla_dwh(tabla_stg, tabla_dwh, engine_stg, engine_dwh):
    """
    Carga datos desde Staging a DWH

    Estrategia: TRUNCATE + INSERT (Carga Inicial)
    """
    try:
        # 1. Leer datos de STG
        df = pd.read_sql_table(tabla_stg, engine_stg)

        # 2. Aplicar transformaciones finales si es necesario
        # (ej: eliminar columnas _raw, agregar surrogate keys, etc.)

        # 3. TRUNCATE en DWH
        with engine_dwh.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {tabla_dwh}"))
            conn.commit()

        # 4. INSERT en DWH
        df.to_sql(tabla_dwh, engine_dwh, if_exists='append', index=False)

        LoggerManager.info(f"✓ Cargados {len(df)} registros: {tabla_stg} → {tabla_dwh}")
        return len(df)

    except Exception as e:
        LoggerManager.error(f"✗ Error cargando {tabla_stg}: {str(e)}")
        return 0
```

**4. Validaciones**

- Verificar que todas las tablas STG existen
- Verificar que todas las tablas DWH existen
- Contar registros antes/después
- Validar integridad referencial (FKs)

**5. Reporte Final**

- Total de registros cargados por tabla
- Tiempo de ejecución
- Errores o advertencias
- Recomendaciones (ej: regenerar índices)

### 📌 Ejemplo de Estructura

```python
# ============================================
# CARGA DE DATOS A DATA WAREHOUSE
# ============================================

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os, sys

sys.path.append(os.path.join(os.getcwd(), '..'))
from logging_config import LoggerManager

# Configuración
load_dotenv()
engine_stg = create_engine(f"mysql+pymysql://...")
engine_dwh = create_engine(f"mysql+pymysql://...")
logger = LoggerManager.configurar("carga_dwh", os.getcwd(), 'logs')

# Mapeo de tablas
mapeo = {
    'stg_estudiante': 'dim_estudiante',
    # ...
}

# Ejecución
resultados = {}
for tabla_stg, tabla_dwh in mapeo.items():
    resultados[tabla_stg] = cargar_tabla_dwh(tabla_stg, tabla_dwh, engine_stg, engine_dwh)

# Reporte
total_cargados = sum(resultados.values())
logger.info(f"Carga DWH completada: {total_cargados} registros totales")
```

### ✅ Checklist

- [ ] Crear estructura básica del notebook
- [ ] Implementar función `cargar_tabla_dwh()`
- [ ] Definir mapeo completo de tablas STG → DWH
- [ ] Implementar validaciones
- [ ] Implementar reporte de ejecución
- [ ] Probar con datos reales
- [ ] Documentar en el notebook

---

## 2️⃣ Orquestador: `orquestador.ipynb`

### 📌 Objetivo

Automatizar la ejecución completa del flujo ETL (Carga Staging → Transformación → Carga DWH).

### 🔄 Flujo Esperado

```
┌──────────────────────────────────────────────────────────┐
│         ORQUESTADOR ETL - CARGA INICIAL                  │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
   ┌───────────────────────┐
   │  1. VALIDACIÓN        │
   │  ✓ Archivos CSV       │
   │  ✓ Tablas STG/DWH     │
   │  ✓ Conexión BD        │
   └───────────┬───────────┘
               │
               ▼
   ┌───────────────────────┐
   │  2. CARGA STAGING     │
   │  (carga_staging)      │
   │  CSV → STG Tables     │
   └───────────┬───────────┘
               │
               ▼
   ┌───────────────────────┐
   │  3. TRANSFORMACIÓN    │
   │  (transformacion)     │
   │  STG → Limpio/Lógica  │
   └───────────┬───────────┘
               │
               ▼
   ┌───────────────────────┐
   │  4. CARGA DWH         │
   │  (carga_dwh)          │
   │  STG → DWH Tables     │
   └───────────┬───────────┘
               │
               ▼
   ┌───────────────────────┐
   │  5. REPORTE FINAL     │
   │  ✓ Estadísticas       │
   │  ✓ Tiempo ejecución   │
   │  ✓ Errores/Warnings   │
   └───────────────────────┘
```

### 📋 Especificación Técnica

#### Responsabilidades:

1. **Validación Pre-Ejecución**
   - Verificar conectividad a BD
   - Validar archivos CSV existen
   - Verificar tablas STG/DWH existen
   - Revisar credenciales .env

2. **Ejecución Secuencial**
   - Ejecutar `carga_staging.ipynb`
   - Si OK, ejecutar `transformacion.ipynb`
   - Si OK, ejecutar `carga_dwh.ipynb`
   - Capturar salida/errores de cada paso

3. **Manejo de Errores**
   - Detener si falla algún paso
   - Registrar error en log
   - Enviar notificación (email/mensaje)
   - Permitir reintentos

4. **Reporte de Ejecución**
   - Tiempo total de ejecución
   - Estado de cada fase (OK/ERROR)
   - Cantidad de registros procesados
   - Advertencias o anomalías detectadas

5. **Logging Consolidado**
   - Leer logs de carga_staging y transformacion
   - Generar reporte consolidado
   - Guardar resumen en archivo

#### Método de Ejecución:

**Opción A: Nbconvert** (Recomendado)

```python
import subprocess

def ejecutar_notebook(ruta_notebook):
    """Ejecutar notebook y capturar salida"""
    cmd = [
        'jupyter', 'nbconvert',
        '--to', 'notebook',
        '--execute',
        '--output', ruta_notebook.replace('.ipynb', '_executed.ipynb'),
        ruta_notebook
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr

exitoso, stdout, stderr = ejecutar_notebook('carga_staging.ipynb')
if not exitoso:
    LoggerManager.error(f"Error en carga_staging: {stderr}")
    sys.exit(1)
```

**Opción B: Papermill** (Alternativa moderna)

```python
import papermill as pm

def ejecutar_notebook_papermill(entrada, salida, parametros=None):
    """Ejecutar notebook con parámetros"""
    try:
        pm.execute_notebook(
            entrada,
            salida,
            parameters=parametros or {}
        )
        return True
    except pm.PapermillExecutionException as e:
        LoggerManager.error(f"Error: {e}")
        return False
```

### 📌 Ejemplo de Estructura

```python
# ============================================
# ORQUESTADOR ETL - CARGA INICIAL UNIVERSIDAD
# ============================================

import sys, os
import subprocess
from datetime import datetime

sys.path.append(os.path.join(os.getcwd(), '..'))
from logging_config import LoggerManager

logger = LoggerManager.configurar("orquestador", os.getcwd(), 'logs')

class OrquestadorETL:
    """Orquestador del flujo ETL completo"""

    def __init__(self):
        self.inicio = datetime.now()
        self.resultados = {}
        self.exitoso = True

    def validar_ambiente(self):
        """Validar que todo está listo"""
        logger.info("=" * 50)
        logger.info("1. VALIDANDO AMBIENTE")
        logger.info("=" * 50)

        # Verificar archivos, tablas, conexión
        # ...

    def ejecutar_carga_staging(self):
        """Ejecutar carga_staging.ipynb"""
        logger.info("=" * 50)
        logger.info("2. EJECUTANDO CARGA STAGING")
        logger.info("=" * 50)

        exitoso = self._ejecutar_notebook('carga_staging.ipynb')
        self.resultados['carga_staging'] = exitoso
        return exitoso

    def ejecutar_transformacion(self):
        """Ejecutar transformacion.ipynb"""
        logger.info("=" * 50)
        logger.info("3. EJECUTANDO TRANSFORMACIÓN")
        logger.info("=" * 50)

        exitoso = self._ejecutar_notebook('transformacion.ipynb')
        self.resultados['transformacion'] = exitoso
        return exitoso

    def ejecutar_carga_dwh(self):
        """Ejecutar carga_dwh.ipynb"""
        logger.info("=" * 50)
        logger.info("4. EJECUTANDO CARGA DWH")
        logger.info("=" * 50)

        exitoso = self._ejecutar_notebook('carga_dwh.ipynb')
        self.resultados['carga_dwh'] = exitoso
        return exitoso

    def generar_reporte(self):
        """Generar reporte final"""
        logger.info("=" * 50)
        logger.info("5. REPORTE FINAL")
        logger.info("=" * 50)

        tiempo_total = (datetime.now() - self.inicio).total_seconds()

        logger.info(f"Tiempo total: {tiempo_total:.2f} segundos")
        for proceso, estado in self.resultados.items():
            estado_str = "✓ OK" if estado else "✗ ERROR"
            logger.info(f"  {proceso}: {estado_str}")

    def ejecutar(self):
        """Ejecutar flujo ETL completo"""
        try:
            self.validar_ambiente()

            if not self.ejecutar_carga_staging():
                raise Exception("Fallo en carga_staging")

            if not self.ejecutar_transformacion():
                raise Exception("Fallo en transformacion")

            if not self.ejecutar_carga_dwh():
                raise Exception("Fallo en carga_dwh")

            self.generar_reporte()
            logger.info("✓ ETL COMPLETADO EXITOSAMENTE")

        except Exception as e:
            logger.error(f"✗ ETL FALLIDO: {str(e)}")
            self.generar_reporte()
            raise

    def _ejecutar_notebook(self, ruta):
        """Ejecutar un notebook y retornar True si tuvo éxito"""
        # Implementar usando nbconvert o papermill
        pass

# Ejecutar orquestador
if __name__ == "__main__":
    orquestador = OrquestadorETL()
    orquestador.ejecutar()
```

### ✅ Checklist

- [ ] Crear clase `OrquestadorETL`
- [ ] Implementar método `validar_ambiente()`
- [ ] Implementar método para ejecutar cada notebook
- [ ] Implementar manejo de errores
- [ ] Implementar generador de reporte
- [ ] Implementar logging consolidado
- [ ] Probar flujo completo
- [ ] Documentar en el notebook

---

## 📚 Referencias y Recursos

### Para `carga_dwh.ipynb`:

- Documentación SQLAlchemy: https://docs.sqlalchemy.org/
- Pandas to_sql: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html

### Para `orquestador.ipynb`:

- Papermill: https://papermill.readthedocs.io/
- Nbconvert: https://nbconvert.readthedocs.io/
- Subprocess: https://docs.python.org/3/library/subprocess.html

---

## 🎯 Orden de Implementación Sugerido

1. **Primero:** `carga_dwh.ipynb` (Independiente, completa flujo actual)
2. **Después:** `orquestador.ipynb` (Depende de carga_dwh)

---

## 📊 Métricas de Éxito

- [ ] Todos los notebooks ejecutables sin errores
- [ ] Datos completos en DWH
- [ ] Logs generados correctamente
- [ ] Reporte de ejecución generado
- [ ] Tiempo de ejecución razonable (<5 min para carga inicial)

---

**Última actualización:** 5 de mayo de 2026
