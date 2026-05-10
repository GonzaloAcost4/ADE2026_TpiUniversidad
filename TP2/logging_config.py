"""
================================================================================
MÓDULO: logging_config.py
================================================================================
Módulo de configuración centralizado para logging en los ETL processes.

Proporciona una clase LoggerManager que centraliza la configuración de logging
para todos los notebooks del proyecto, garantizando consistencia y reutilización
de la instancia del logger en múltiples procesos ETL.

FEATURES:
- Singleton pattern: una sola instancia de logger para toda la aplicación
- Salida dual: archivos de log + consola simultáneamente
- Timestamps automáticos en nombres de archivo
- Creación automática de directorios
- Métodos de clase para facilitar acceso global

DISEÑO:
- _logger_instance: almacena la única instancia del logger (None si no se ha configurado)
- _log_dir: ruta del directorio donde se guardan los logs
- métodos @classmethod: permiten acceso estático sin necesidad de instanciar la clase
================================================================================
"""

import logging
import os
from datetime import datetime
from pathlib import Path


class LoggerManager:
    """
    ============================================================================
    Gestor centralizado de logging para ETL processes (Patrón Singleton).
    ============================================================================
    
    PROPÓSITO:
    Proporciona una interfaz unificada para logging en todos los ETL processes
    (carga_staging, transformacion, carga_incremental, análisis, etc.)
    
    FUNCIONALIDADES:
    1. Configuración única y global del logger con @classmethod
    2. Salida dual: archivos (.log) + mensajes en consola
    3. Creación automática de directorios de logs
    4. Métodos estáticos para registrar INFO, WARNING, ERROR, DEBUG
    5. Gestión de timestamps automáticos en nombres de archivo
    6. Reinicio de configuración si es necesario (para cambiar parámetros)
    
    ATRIBUTOS DE CLASE:
    - _logger_instance (logging.Logger|None): única instancia del logger
    - _log_dir (str|None): ruta absoluta del directorio de logs
    
    EJEMPLO DE USO:
        # En inicio del script/notebook
        logger = LoggerManager.configurar('carga_staging', carpeta_logs='logs')
        logger.info("Iniciando proceso")  # Usa logging directo
        
        # O bien, usando métodos estáticos de la clase
        LoggerManager.info("Iniciando proceso")
        LoggerManager.warning("Datos faltantes")
        LoggerManager.error("Error crítico")
    ============================================================================
    """
    
    # Atributos de clase para implementar Singleton
    _logger_instance = None  # Almacena la única instancia del logger
    _log_dir = None           # Almacena la ruta de los logs
    
    @classmethod
    def configurar(cls, nombre_proceso: str, ruta_raiz: str = None, carpeta_logs: str = None) -> logging.Logger:
        """
        ========================================================================
        MÉTODO: configurar()
        ========================================================================
        Configura el logger único para un proceso ETL específico (singleton).
        
        PROPÓSITO:
        Inicializa la configuración global de logging la primera vez que se 
        llama. Las llamadas posteriores retornan la instancia existente sin
        reconfigurar (idempotente).
        
        PARÁMETROS (INPUT):
        - nombre_proceso (str): Nombre descriptivo del proceso
          ej: 'carga_staging', 'transformacion', 'carga_incremental'
          USO: Se usa como prefijo en el nombre del archivo de log
          
        - ruta_raiz (str, default=None): Directorio base para los logs
          - Si es None: usa os.getcwd() (directorio de trabajo actual)
          - Si es str: usa esa ruta como base
          USO: Se combina con carpeta_logs para determinar ubicación final
          
        - carpeta_logs (str, default=None): Carpeta relativa o absoluta de logs
          - Si es None: usa 'logs' (en ruta_raiz)
          - Si es ruta relativa: se combina con ruta_raiz
            ej: 'logs', '..  /logs', 'output/logs'
          - Si es ruta absoluta: se usa directamente (ignora ruta_raiz)
            ej: 'C:/proyecto/logs' o '/home/user/logs'
          USO: Determina dónde se guardan los archivos .log
        
        PROCESO DE CONFIGURACIÓN (TRATAMIENTO DE INPUT):
        1. Verifica si ya existe _logger_instance (singleton check)
           → Si existe: retorna inmediatamente sin reconfigurar
        
        2. Determina ruta base:
           → Si ruta_raiz es None: usa getcwd()
           → Si no: usa la ruta proporcionada
        
        3. Determina directorio de logs:
           → Si carpeta_logs es abs path: usa directamente
           → Si es relative: combina con ruta_raiz
           → Si es None: usa 'logs' en ruta_raiz
        
        4. Crea directorios:
           → os.makedirs() con exist_ok=True (no falla si ya existen)
        
        5. Configura logger con dos handlers:
           a) FileHandler: guarda logs en archivo
              - Nombre: {nombre_proceso}_{YYYYMMdd_HHMMSS}.log
              - Encoding: utf-8 (para caracteres especiales)
              - Level: INFO
           
           b) StreamHandler (consola): imprime en stdout/stderr
              - Level: INFO
              - Mismo formato que archivo para consistencia
        
        6. Guarda referencias en _logger_instance y _log_dir
        
        OUTPUT (RETORNO):
        - logging.Logger: instancia configurada del logger
          USO: se usa para llamar logger.info(), logger.warning(), etc.
        
        EJEMPLO DE USO:
            # Caso 1: simple (usa directorio actual/logs)
            logger = LoggerManager.configurar('carga_staging')
            
            # Caso 2: con ruta personalizada
            logger = LoggerManager.configurar(
                'transformacion',
                ruta_raiz='/proyectos/miproyecto',
                carpeta_logs='output/logs'
            )
            
            # Resultado: logs en /proyectos/miproyecto/output/logs/
            
            # Caso 3: ruta absoluta de logs
            logger = LoggerManager.configurar(
                'analisis',
                carpeta_logs='C:/compartido/logs'  # ignora ruta_raiz
            )
        ========================================================================
        """
        # SINGLETON CHECK: si ya existe logger, retorna sin hacer nada
        if cls._logger_instance is not None:
            return cls._logger_instance
        
        # STEP 1: Determinar ruta base del proyecto
        if ruta_raiz is None:
            ruta_raiz = os.getcwd()
        
        # STEP 2: Determinar ubicación final del directorio de logs
        # (siguiendo precedencia: abs > relative > default)
        if carpeta_logs is not None:
            if os.path.isabs(carpeta_logs):
                # Ruta absoluta: se usa directamente
                cls._log_dir = carpeta_logs
            else:
                # Ruta relativa: se combina con ruta_raiz
                cls._log_dir = os.path.join(ruta_raiz, carpeta_logs)
        else:
            # Default: carpeta 'logs' en ruta_raiz
            cls._log_dir = os.path.join(ruta_raiz, 'logs')
        
        # STEP 3: Crear directorio si no existe (idempotente)
        os.makedirs(cls._log_dir, exist_ok=True)
        
        # STEP 4: Generar nombre de archivo de log con timestamp
        # Formato: {nombre_proceso}_{YYYYMMdd_HHMMSS}.log
        # Esto permite múltiples ejecuciones sin sobrescribirse
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = os.path.join(cls._log_dir, f"{nombre_proceso}_{timestamp}.log")
        
        # STEP 5: Crear logger principal
        logger = logging.getLogger(nombre_proceso)
        
        # Limpiar handlers existentes para evitar duplicación si se reconfigura
        logger.handlers.clear()
        logger.setLevel(logging.INFO)  # Nivel mínimo: INFO
        
        # STEP 6: Configurar formato de los mensajes de log
        # Formato: [YYYY-MM-DD HH:MM:SS] - [NIVEL] - [MENSAJE]
        formato = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # STEP 7: Handler 1 - Archivos
        # Guarda logs en archivos con encoding utf-8 para caracteres especiales
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formato)
        logger.addHandler(file_handler)
        
        # STEP 8: Handler 2 - Consola
        # Imprime simultáneamente en la consola para feedback en vivo
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formato)
        logger.addHandler(console_handler)
        
        # STEP 9: Guardar referencias para acceso futuro
        cls._logger_instance = logger
        
        # STEP 10: Logging inicial
        logger.info(f"Iniciando {nombre_proceso}. Log: {log_filename}")
        
        return logger
    
    @classmethod
    def obtener(cls, nombre_proceso: str = None) -> logging.Logger:
        """
        ========================================================================
        MÉTODO: obtener()
        ========================================================================
        Obtiene la instancia actual del logger o crea una nueva si no existe.
        
        PROPÓSITO:
        Proporciona acceso seguro al logger en cualquier momento del código.
        Si aún no se ha llamado a configurar(), lo hace automáticamente.
        
        PARÁMETROS (INPUT):
        - nombre_proceso (str, default=None): Nombre del proceso para 
          inicialización automática (solo si logger no existe)
          - Si es None: usa 'etl_process' como nombre por defecto
          - Si logger existe: este parámetro se ignora
        
        PROCESO (TRATAMIENTO):
        1. Verifica si _logger_instance existe
           → Si NO existe: llama a configurar() con nombre_proceso
           → Si SÍ existe: retorna la instancia existente
        
        OUTPUT (RETORNO):
        - logging.Logger: instancia del logger (puede ser existente o nueva)
        
        EJEMPLO:
            # Primera llamada (no existe logger aún)
            logger = LoggerManager.obtener('mi_proceso')
            # → Configura y retorna logger
            
            # Llamada posterior (logger ya existe)
            logger2 = LoggerManager.obtener('otro_nombre')
            # → Ignora 'otro_nombre', retorna logger existente
        ========================================================================
        """
        if cls._logger_instance is None:
            if nombre_proceso is None:
                nombre_proceso = 'etl_process'
            return cls.configurar(nombre_proceso)
        
        return cls._logger_instance
    
    @classmethod
    def info(cls, mensaje: str) -> None:
        """
        ========================================================================
        MÉTODO: info()
        ========================================================================
        Registra un mensaje de nivel INFO (información general).
        
        INPUT:
        - mensaje (str): Texto a registrar
          Ejemplos: "Iniciando carga", "Leídos 1000 registros"
        
        OUTPUT:
        - None: no retorna nada, solo efecto secundario de logging
        
        TRATAMIENTO:
        1. Obtiene instancia del logger (crea si no existe)
        2. Llama al método logger.info(mensaje)
        3. El mensaje se registra en AMBOS:
           - Archivo de log (log file)
           - Consola (stdout)
        
        NOTA: INFO es para eventos normales del proceso, no errores.
        
        EJEMPLO:
            LoggerManager.info("Iniciando transformación")
            LoggerManager.info(f"Procesados {count} registros")
        ========================================================================
        """
        logger = cls.obtener()
        logger.info(mensaje)
    
    @classmethod
    def warning(cls, mensaje: str) -> None:
        """
        ========================================================================
        MÉTODO: warning()
        ========================================================================
        Registra un mensaje de nivel WARNING (aviso de algo potencialmente erróneo).
        
        INPUT:
        - mensaje (str): Texto a registrar
          Ejemplos: "Datos faltantes en campo X", "Duplicados detectados"
        
        OUTPUT:
        - None: no retorna nada, solo efecto secundario de logging
        
        TRATAMIENTO:
        1. Obtiene instancia del logger (crea si no existe)
        2. Llama al método logger.warning(mensaje)
        3. El mensaje se registra en AMBOS:
           - Archivo de log (log file) con nivel WARNING
           - Consola (stderr) con nivel WARNING
        
        NOTA: WARNING es para situaciones inesperadas pero recuperables.
        Para errores críticos, usar error().
        
        EJEMPLO:
            LoggerManager.warning("20 registros incompletos")
            LoggerManager.warning(f"Faltaron {missing} valores en DNI")
        ========================================================================
        """
        logger = cls.obtener()
        logger.warning(mensaje)
    
    @classmethod
    def error(cls, mensaje: str) -> None:
        """
        ========================================================================
        MÉTODO: error()
        ========================================================================
        Registra un mensaje de nivel ERROR (error crítico).
        
        INPUT:
        - mensaje (str): Texto a registrar
          Ejemplos: "Conexión a BD fallida", "Tabla no existe"
        
        OUTPUT:
        - None: no retorna nada, solo efecto secundario de logging
        
        TRATAMIENTO:
        1. Obtiene instancia del logger (crea si no existe)
        2. Llama al método logger.error(mensaje)
        3. El mensaje se registra en AMBOS:
           - Archivo de log (log file) con nivel ERROR
           - Consola (stderr) con nivel ERROR (usualmente en rojo)
        
        NOTA: ERROR es para problemas graves que requieren atención inmediata.
        El proceso puede continuar o no según el manejador de excepciones.
        
        EJEMPLO:
            LoggerManager.error("No se pudo conectar a MySQL")
            LoggerManager.error(f"Error en fila {row_num}: {exception_msg}")
        ========================================================================
        """
        logger = cls.obtener()
        logger.error(mensaje)
    
    @classmethod
    def debug(cls, mensaje: str) -> None:
        """
        ========================================================================
        MÉTODO: debug()
        ========================================================================
        Registra un mensaje de nivel DEBUG (información detallada de depuración).
        
        INPUT:
        - mensaje (str): Texto detallado a registrar
          Ejemplos: "Valor transformado: X -> Y", "SQL ejecutado: SELECT ..."
        
        OUTPUT:
        - None: no retorna nada, solo efecto secundario de logging
        
        TRATAMIENTO:
        1. Obtiene instancia del logger (crea si no existe)
        2. Llama al método logger.debug(mensaje)
        3. El mensaje se registra SOLO en archivo de log (no en consola)
        
        NOTA: DEBUG es para información detallada útil en depuración.
        Por defecto está deshabilitado en consola para no saturar la salida.
        Se usa principalmente para diagnóstico del desarrollo.
        
        EJEMPLO:
            LoggerManager.debug(f"DataFrame shape: {df.shape}")
            LoggerManager.debug(f"Query ejecutada: {query}")
        ========================================================================
        """
        logger = cls.obtener()
        logger.debug(mensaje)
    
    @classmethod
    def obtener_ruta_logs(cls) -> str:
        """
        ========================================================================
        MÉTODO: obtener_ruta_logs()
        ========================================================================
        Retorna la ruta absoluta del directorio donde se guardan los logs.
        
        INPUT:
        - None: no requiere parámetros
        
        OUTPUT:
        - str: Ruta absoluta del directorio de logs
          Ejemplo: 'C:\\proyectos\\TP2\\logs' o '/home/user/TP2/logs'
        
        TRATAMIENTO:
        1. Verifica si _log_dir está definido
           → Si NO está definido (None):
             - Crea ruta default: {getcwd()}/logs
             - Crea el directorio si no existe (exist_ok=True)
           → Si SÍ está definido: retorna como está
        
        CASOS DE USO:
        - Imprimir ubicación de logs al usuario final
        - Guardar referencia de dónde está guardado el log
        - Limpiar archivos antiguos de logs programáticamente
        
        EJEMPLO:
            ruta = LoggerManager.obtener_ruta_logs()
            print(f"Logs guardados en: {ruta}")
            # Output: Logs guardados en: C:\\proyectos\\TP2\\logs
        ========================================================================
        """
        if cls._log_dir is None:
            # Default: usar directorio 'logs' en cwd
            cls._log_dir = os.path.join(os.getcwd(), 'logs')
            # Crear el directorio si no existe
            os.makedirs(cls._log_dir, exist_ok=True)
        
        return cls._log_dir
    
    @classmethod
    def reiniciar(cls) -> None:
        """
        ========================================================================
        MÉTODO: reiniciar()
        ========================================================================
        Reinicia la configuración del logger limpiando las referencias globales.
        
        INPUT:
        - None: no requiere parámetros
        
        OUTPUT:
        - None: no retorna nada, solo efecto secundario
        
        TRATAMIENTO:
        1. Pone _logger_instance a None (elimina referencia)
        2. Pone _log_dir a None (elimina referencia)
        3. De esta forma, la próxima llamada a configurar() o obtener()
           creará un nuevo logger (se puede usar distinto nombre/ruta)
        
        CASOS DE USO:
        - Cambiar parámetros de configuración en medio de ejecución
        - Limpiar estado en tests unitarios
        - Iniciar un nuevo proceso con diferentes parámetros
        - Resetear entre ejecuciones de múltiples ETL
        
        EJEMPLO:
            # Proceso 1
            logger1 = LoggerManager.configurar('etl_parte1', carpeta_logs='logs')
            
            # Quiero cambiar la ruta para el siguiente proceso
            LoggerManager.reiniciar()  # Limpia referencias
            
            # Proceso 2 (con nueva configuración)
            logger2 = LoggerManager.configurar(
                'etl_parte2',
                carpeta_logs='logs_finales'
            )
        ========================================================================
        """
        # Resetea las referencias para permitir reconfiguración
        cls._logger_instance = None
        cls._log_dir = None
