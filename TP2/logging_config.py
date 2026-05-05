"""
Módulo de configuración centralizado para logging en los ETL processes.

Proporciona una clase LoggerManager que centraliza la configuración de logging
para todos los notebooks del proyecto, garantizando consistencia y reutilización.
"""

import logging
import os
from datetime import datetime
from pathlib import Path


class LoggerManager:
    """
    Gestor centralizado de logging para ETL processes.
    
    Proporciona métodos para:
    - Configurar logging con archivos y consola
    - Crear directorio de logs automáticamente
    - Enviar mensajes de diferente nivel (info, warning, error, debug)
    - Obtener instancia del logger configurado
    """
    
    _logger_instance = None
    _log_dir = None
    
    @classmethod
    def configurar(cls, nombre_proceso: str, ruta_raiz: str = None, carpeta_logs: str = None) -> logging.Logger:
        """
        Configura el logger para un proceso específico.
        
        Args:
            nombre_proceso (str): Nombre del proceso (ej: 'carga_staging', 'transformacion')
            ruta_raiz (str): Ruta raíz del proyecto. Si es None, usa el directorio actual.
            carpeta_logs (str): Nombre o ruta relativa de la carpeta de logs. 
                               Si es None, usa 'logs' en ruta_raiz.
                               Ejemplos: 'logs', '../logs', etc.
            
        Returns:
            logging.Logger: Instancia del logger configurado.
            
        Example:
            logger = LoggerManager.configurar('carga_staging', carpeta_logs='logs')
            logger = LoggerManager.configurar('carga_staging', carpeta_logs=os.path.join(os.getcwd(), 'logs'))
            logger.info("Iniciando proceso")
        """
        if cls._logger_instance is not None:
            return cls._logger_instance
        
        # Determinar ruta de logs
        if ruta_raiz is None:
            ruta_raiz = os.getcwd()
        
        # Si se especifica carpeta_logs, usarla; si no, usar 'logs' en ruta_raiz
        if carpeta_logs is not None:
            if os.path.isabs(carpeta_logs):
                cls._log_dir = carpeta_logs
            else:
                cls._log_dir = os.path.join(ruta_raiz, carpeta_logs)
        else:
            cls._log_dir = os.path.join(ruta_raiz, 'logs')
        
        os.makedirs(cls._log_dir, exist_ok=True)
        
        # Crear nombre del archivo de log con timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = os.path.join(cls._log_dir, f"{nombre_proceso}_{timestamp}.log")
        
        # Configurar logging
        logger = logging.getLogger(nombre_proceso)
        
        # Limpiar handlers existentes para evitar duplicados
        logger.handlers.clear()
        logger.setLevel(logging.INFO)
        
        # Formato de log
        formato = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para archivo
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formato)
        logger.addHandler(file_handler)
        
        # Handler para consola (StreamHandler)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formato)
        logger.addHandler(console_handler)
        
        # Guardar referencia
        cls._logger_instance = logger
        
        # Mensaje inicial
        logger.info(f"Iniciando {nombre_proceso}. Log: {log_filename}")
        
        return logger
    
    @classmethod
    def obtener(cls, nombre_proceso: str = None) -> logging.Logger:
        """
        Obtiene la instancia del logger.
        
        Si no existe, crea una nueva configuración.
        
        Args:
            nombre_proceso (str): Nombre del proceso. Solo usado si no existe logger.
            
        Returns:
            logging.Logger: Instancia del logger configurado.
        """
        if cls._logger_instance is None:
            if nombre_proceso is None:
                nombre_proceso = 'etl_process'
            return cls.configurar(nombre_proceso)
        
        return cls._logger_instance
    
    @classmethod
    def info(cls, mensaje: str) -> None:
        """
        Registra un mensaje de nivel INFO.
        
        Args:
            mensaje (str): Mensaje a registrar.
        """
        logger = cls.obtener()
        logger.info(mensaje)
    
    @classmethod
    def warning(cls, mensaje: str) -> None:
        """
        Registra un mensaje de nivel WARNING.
        
        Args:
            mensaje (str): Mensaje a registrar.
        """
        logger = cls.obtener()
        logger.warning(mensaje)
    
    @classmethod
    def error(cls, mensaje: str) -> None:
        """
        Registra un mensaje de nivel ERROR.
        
        Args:
            mensaje (str): Mensaje a registrar.
        """
        logger = cls.obtener()
        logger.error(mensaje)
    
    @classmethod
    def debug(cls, mensaje: str) -> None:
        """
        Registra un mensaje de nivel DEBUG.
        
        Args:
            mensaje (str): Mensaje a registrar.
        """
        logger = cls.obtener()
        logger.debug(mensaje)
    
    @classmethod
    def obtener_ruta_logs(cls) -> str:
        """
        Retorna la ruta del directorio de logs.
        
        Returns:
            str: Ruta absoluta del directorio de logs.
        """
        if cls._log_dir is None:
            cls._log_dir = os.path.join(os.getcwd(), 'logs')
            os.makedirs(cls._log_dir, exist_ok=True)
        
        return cls._log_dir
    
    @classmethod
    def reiniciar(cls) -> None:
        """
        Reinicia la configuración del logger.
        
        Útil cuando se necesita cambiar el nombre del proceso o la ruta de logs.
        """
        cls._logger_instance = None
        cls._log_dir = None
