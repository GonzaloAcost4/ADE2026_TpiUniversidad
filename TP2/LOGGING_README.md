# Sistema Centralizado de Logging - ETL Universidad

## 📋 Descripción

Se ha creado un módulo centralizado `logging_config.py` que proporciona la clase `LoggerManager` para gestionar el logging en todos los notebooks ETL del proyecto. Esto elimina la duplicación de código y garantiza consistencia.

## 📁 Módulo: `logging_config.py`

**Ubicación:** `./logging_config.py` (raíz del proyecto)

### Clase: `LoggerManager`

Una clase con métodos estáticos que gestiona logging de forma centralizada.

#### Métodos principales

| Método                                       | Descripción                                                                           |
| -------------------------------------------- | ------------------------------------------------------------------------------------- |
| `configurar(nombre_proceso, ruta_raiz=None)` | Inicializa el logger para un proceso específico. Crea automáticamente carpeta `logs/` |
| `obtener(nombre_proceso=None)`               | Retorna la instancia del logger existente o crea una nueva                            |
| `info(mensaje)`                              | Registra mensaje de nivel INFO                                                        |
| `warning(mensaje)`                           | Registra mensaje de nivel WARNING                                                     |
| `error(mensaje)`                             | Registra mensaje de nivel ERROR                                                       |
| `debug(mensaje)`                             | Registra mensaje de nivel DEBUG                                                       |
| `obtener_ruta_logs()`                        | Retorna la ruta del directorio `logs/`                                                |
| `reiniciar()`                                | Reinicia la configuración del logger                                                  |

## 🚀 Cómo usar

### 1. En tu script ETL

```python
import sys
import os
sys.path.append(os.path.join(os.getcwd(), '..'))

# Importar el LoggerManager
from logging_config import LoggerManager

# Configurar al inicio del script
logger = LoggerManager.configurar(
    "nombre_de_tu_proceso",
    ruta_raiz=os.path.join(os.getcwd())
)
```

### 2. Usar logging en tu código

```python
# Información
LoggerManager.info("Iniciando carga de datos")

# Advertencias
LoggerManager.warning(f"Registros faltantes: {cantidad}")

# Errores
LoggerManager.error(f"Error al conectar: {str(e)}")

# Debug
LoggerManager.debug("Estado actual del proceso")
```

### 3. Acceder a la ruta de logs

```python
# Obtener ruta donde se guardan los logs
logs_path = LoggerManager.obtener_ruta_logs()
print(f"Logs disponibles en: {logs_path}")
```

## 📊 Archivos de Log

Los logs se guardan automáticamente en:

```
./logs/
├── carga_staging_20260504_101530.log
└── transformacion_20260504_102045.log
```

**Formato del nombre:** `{nombre_proceso}_{YYYYMMDD_HHMMSS}.log`

**Contenido del log:**

```
2026-05-04 10:15:30 - INFO - Iniciando carga. Log: ./logs/carga_staging_20260504_101530.log
2026-05-04 10:15:31 - INFO - TRUNCATE ejecutado en stg_estudiante
2026-05-04 10:15:32 - INFO - Cargados 150 registros en stg_estudiante
2026-05-04 10:15:33 - WARNING - Archivo vacío: curso.csv
```

## 📝 Notebooks ya configurados

- ✅ `2-ETL_CargaInicial/carga_staging.py`
- ✅ `2-ETL_CargaInicial/transformacion.py`

Ambos scripts ya importan y usan `LoggerManager`.

## 🔧 Ventajas del nuevo sistema

✅ **DRY (Don't Repeat Yourself):** Configuración única para todos los procesos  
✅ **Consistencia:** Mismo formato de logs en todos los ETLs  
✅ **Mantenibilidad:** Un único punto de cambio para actualizar logging  
✅ **Reutilizable:** Cualquier nuevo script puede importar `LoggerManager`  
✅ **Automatizado:** Directorios y archivos generados automáticamente  
✅ **Escalable:** Fácil agregar nuevas características de logging

## 📌 Ejemplo completo

```python
# Inicio del script
import sys
import os
sys.path.append(os.path.join(os.getcwd(), '..'))
from logging_config import LoggerManager

# Configurar
logger = LoggerManager.configurar("mi_etl", ruta_raiz=os.path.join(os.getcwd(), '..'))

# Usar en código
try:
    datos = cargar_datos()
    LoggerManager.info(f"Cargados {len(datos)} registros")

    datos_transformados = transformar(datos)
    LoggerManager.info(f"Transformados {len(datos_transformados)} registros")

    insertar(datos_transformados)
    LoggerManager.info("Inserción completada exitosamente")

except Exception as e:
    LoggerManager.error(f"Error en proceso: {str(e)}")
    raise
```

## ❓ Preguntas frecuentes

**P:** ¿Puedo usar `logger` directamente en funciones?  
**R:** Sí. Una vez configurado con `LoggerManager.configurar()`, puedes usar `LoggerManager.info()`, etc., desde cualquier parte del código.

**P:** ¿Qué pasa si ejecuto el script varias veces?  
**R:** Se crean múltiples archivos de log (uno por ejecución) con timestamps diferentes. Los logs anteriores se preservan.

**P:** ¿Puedo cambiar el nivel de logging (DEBUG, INFO, WARNING, ERROR)?  
**R:** Por ahora está fijo en INFO. Para modificarlo, edita `logging_config.py` y cambia `logger.setLevel(logging.INFO)`.

**P:** ¿Dónde puedo ver los logs?  
**R:** En la carpeta `logs/` dentro del directorio raíz del proyecto.
