# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # ETL Transformación - De Staging a DWH
#
# Este notebook realiza la transformación de datos desde las tablas staging (STG_Universidad) hacia el Data Warehouse (dw_universidad).
#
# ## Procesos incluidos:
# - **Limpieza**: Reparación de encoding, espacios, valores nulos
# - **Validación**: Tipos de datos, rangos, integridad referencial
# - **Normalización**: Formatos de fechas, mayúsculas, decimales
# - **Deduplicación**: Eliminación de registros duplicados
# - **Optimización**: Procesamiento por lotes y transacciones

# %%
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.pool import NullPool
from datetime import datetime, date
from dotenv import load_dotenv
import os
from pathlib import Path
import logging
import warnings
import re
from typing import Tuple, Dict, List

warnings.filterwarnings('ignore')

# ============================================
# CONFIGURACIÓN DE CREDENCIALES Y CONEXIÓN
# ============================================

load_dotenv()

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
STG_DATABASE = os.getenv("DB_DATABASE")
DWH_DATABASE = os.getenv("DB_DATABASE")

load_dotenv()

# Motor para Staging (lectura)
engine_stg = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{STG_DATABASE}",
    poolclass=NullPool
)

# Motor para DWH (escritura)
engine_dwh = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DWH_DATABASE}",
    poolclass=NullPool
)

print("[INFO] Motores de conexión configurados")
print(f"  Staging: {STG_DATABASE} en {HOST}:{PORT}")
print(f"  DWH: {DWH_DATABASE} en {HOST}:{PORT}")

# ============================================
# CONFIGURACIÓN DE LOGGING
# ============================================

RUTA_LOGS = os.path.join(os.getcwd(), 'logs')
os.makedirs(RUTA_LOGS, exist_ok=True)

log_filename = os.path.join(
    RUTA_LOGS, 
    f"transformacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info(f"Iniciando transformación. Log: {log_filename}")
print(f"\n[OK] Logging configurado en: {log_filename}")


# %%
# ============================================
# FUNCIONES DE LIMPIEZA Y VALIDACIÓN
# ============================================

class DataCleaner:
    """Clase para limpieza y validación de datos."""
    
    @staticmethod
    def limpiar_string(valor: str) -> str:
        """Limpia strings: espacios, codificación, minúsculas."""
        if pd.isna(valor) or valor is None:
            return None
        
        # Convertir a string si no lo es
        valor = str(valor).strip()
        
        if valor.lower() in ['', 'null', 'none', 'n/a', 'sin dato']:
            return None
        
        # Reparar encoding (caracteres corruptos por UTF-8/Latin-1)
        try:
            # Intenta decodificar como Latin-1 y recodificar como UTF-8
            valor = valor.encode('latin-1').decode('utf-8', errors='ignore')
        except:
            pass
        
        return valor
    
    @staticmethod
    def limpiar_numero(valor, tipo='float', requerido=False) -> any:
        """Convierte y valida números."""
        if pd.isna(valor) or valor is None:
            if requerido:
                logger.warning("Valor requerido faltante")
            return None
        
        valor_str = str(valor).strip()
        
        if valor_str.lower() in ['', 'null', 'none', 'n/a']:
            return None
        
        try:
            # Remover espacios y símbolos comunes
            valor_str = valor_str.replace(',', '.').replace(' ', '')
            
            if tipo == 'int':
                return int(float(valor_str))
            elif tipo == 'float':
                return float(valor_str)
            else:
                return None
        except:
            logger.warning(f"No se pudo convertir '{valor}' a {tipo}")
            return None
    
    @staticmethod
    def limpiar_fecha(valor) -> date:
        """Convierte y valida fechas (múltiples formatos)."""
        if pd.isna(valor) or valor is None:
            return None
        
        valor_str = str(valor).strip()
        
        if valor_str.lower() in ['', 'null', 'none', 'n/a']:
            return None
        
        # Formatos posibles
        formatos = [
            '%Y-%m-%d',   # ISO 
            '%d/%m/%Y',   # Latino con barra
            '%Y%m%d',     # Compacto 
            '%d-%m-%Y',   # Latino con guion
            '%m-%d-%Y',   # Americano 
            '%Y',         # Solo año 
            '%d/%m/%y',   # Latino año corto
            '%Y/%m/%d',   # ISO con barra
        ]
        
        for fmt in formatos:
            try:
                return pd.to_datetime(valor_str, format=fmt).date()
            except:
                continue
        
        # Intento final sin formato específico
        try:
            return pd.to_datetime(valor_str).date()
        except:
            logger.warning(f"No se pudo parsear fecha: '{valor}'")
            return None
    
    @staticmethod
    def limpiar_genero(valor: str) -> str:
        """Normaliza valores de género."""
        if pd.isna(valor) or valor is None:
            return None
        
        valor = str(valor).strip().upper()
        
        if valor in ['M', 'MASCULINO', 'MALE', 'HOMBRE','1']:
            return 'M'
        elif valor in ['F', 'FEMENINO', 'FEMALE', 'MUJER','2']:
            return 'F'
        else:
            logger.warning(f"Género desconocido: '{valor}'")
            return None
    
    @staticmethod
    def validar_dni(dni) -> bool:
        """Valida rango de DNI argentino manejando formatos sucios."""
        if pd.isna(dni) or dni is None:
            return False
        
        try:
            #Lo convertimos a string para quitarle puntos y espacios
            dni_limpio = str(dni).replace('.', '').replace(' ', '').strip()
            
            #Pasamos a entero para poder comparar rangos
            dni_num = int(float(dni_limpio))
            
            # 4. RANGO: Verificamos que esté en el rango legal (7 a 8 dígitos)
            return 1000000 <= dni_num <= 99999999
            
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validar_nota(nota: float) -> bool:
        """Valida rango de nota (0-10)."""
        if pd.isna(nota) or nota is None:
            return False
        return 0 <= nota <= 10

print("[OK] Clase DataCleaner cargada")


# %%
# ============================================
# FUNCIONES DE TRANSFORMACIÓN POR TABLA
# ============================================

def transformar_estudiante(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_ESTUDIANTE a formato DWH.
    
    Limpieza:
    - Encoding de nombres
    - Validación de DNI
    - Normalización de género
    - Conversión de fechas
    - Deduplicación por DNI
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    # Limpiar columnas
    df['id_estudiante'] = df['id_estudiante_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    df['dni'] = df['dni_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['apellido'] = df['apellido_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['nombre'] = df['nombre_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['genero'] = df['genero_raw'].apply(lambda x: cleaner.limpiar_genero(x))
    df['fecha_nacimiento'] = df['fecha_nacimiento_raw'].apply(
        lambda x: cleaner.limpiar_fecha(x)
    )
    df['email'] = df['email_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['nacionalidad'] = df['nacionalidad_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['id_programa'] = df['id_programa_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    df['anio_ingreso'] = df['anio_ingreso_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    
    # Validaciones
    df['es_válido'] = df.apply(
        lambda row: (
            row['id_estudiante'] is not None and
            row['dni'] is not None and
            cleaner.validar_dni(row['dni']) and
            row['apellido'] is not None and
            row['nombre'] is not None and
            row['id_programa'] is not None
        ),
        axis=1
    )
    
    # Separar válidos de inválidos
    df_válidos = df[df['es_válido']].copy()
    df_inválidos = df[~df['es_válido']].copy()
    
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df_inválidos)
    
    if len(df_inválidos) > 0:
        logger.warning(f"Estudiantes rechazados: {len(df_inválidos)}")
    
    # Deduplicación por DNI (mantener primero)
    duplicados = df_válidos[df_válidos.duplicated(subset=['dni'], keep=False)]
    stats['duplicados'] = len(duplicados)
    
    if len(duplicados) > 0:
        logger.warning(f"Registros con DNI duplicado: {len(duplicados)}")
        df_válidos = df_válidos.drop_duplicates(subset=['dni'], keep='first')
    
    # Seleccionar columnas para DWH
    columnas = ['id_estudiante', 'dni', 'apellido', 'nombre', 'genero',
                'fecha_nacimiento', 'email', 'nacionalidad', 'id_programa', 'anio_ingreso']
    
    return df_válidos[columnas], stats


def transformar_docente(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_DOCENTE a formato DWH.
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_docente'] = df['id_docente_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['apellido'] = df['apellido_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['nombre'] = df['nombre_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['titulo'] = df['titulo_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['categoria'] = df['categoria_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['dedicacion'] = df['dedicacion_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['id_departamento'] = df['id_departamento_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    
    df['es_válido'] = df.apply(
        lambda row: (
            row['id_docente'] is not None and
            row['apellido'] is not None and
            row['nombre'] is not None
        ),
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    # Deduplicación por id_docente
    df_válidos = df_válidos.drop_duplicates(subset=['id_docente'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_docente', 'apellido', 'nombre', 'titulo', 'categoria', 'dedicacion', 'id_departamento']
    
    return df_válidos[columnas], stats


def transformar_examen(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_EXAMEN a formato DWH.
    
    Validaciones especiales:
    - Notas en rango 0-10
    - Número de intento positivo
    - Normalización de resultado
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_examen'] = df['id_examen_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['id_inscripcion'] = df['id_inscripcion_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['fecha'] = df['fecha_raw'].apply(lambda x: cleaner.limpiar_fecha(x))
    df['nota'] = df['nota_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'float'))
    df['numero_intento'] = df['numero_intento_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['resultado'] = df['resultado_raw'].apply(lambda x: cleaner.limpiar_string(x))
    
    # Normalizar resultado
    df['resultado'] = df['resultado'].apply(
        lambda x: 'Aprobado' if x and x.lower() in ['aprobado', 'aprob', 'sí', 'si', '1'] 
        else 'Desaprobado' if x else None
    )
    
    df['es_válido'] = df.apply(
        lambda row: (
            row['id_examen'] is not None and
            row['id_inscripcion'] is not None and
            row['nota'] is not None and
            cleaner.validar_nota(row['nota']) and
            row['numero_intento'] is not None and
            row['numero_intento'] > 0 and
            row['fecha'] is not None
        ),
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    # Deduplicación
    df_válidos = df_válidos.drop_duplicates(subset=['id_examen'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_examen', 'id_inscripcion', 'fecha', 'nota', 'numero_intento', 'resultado']
    
    return df_válidos[columnas], stats


def transformar_facultad(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_FACULTAD.
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_facultad'] = df['id_facultad_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['nombre'] = df['nombre_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['ciudad'] = df['ciudad_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['provincia'] = df['provincia_raw'].apply(lambda x: cleaner.limpiar_string(x))
    
    df['es_válido'] = df.apply(
        lambda row: row['id_facultad'] is not None and row['nombre'] is not None,
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    df_válidos = df_válidos.drop_duplicates(subset=['id_facultad'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_facultad', 'nombre', 'ciudad', 'provincia']
    
    return df_válidos[columnas], stats


def transformar_departamento(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_DEPARTAMENTO.
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_departamento'] = df['id_departamento_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    df['nombre'] = df['nombre_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['id_facultad'] = df['id_facultad_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    
    df['es_válido'] = df.apply(
        lambda row: (
            row['id_departamento'] is not None and
            row['nombre'] is not None and
            row['id_facultad'] is not None
        ),
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    df_válidos = df_válidos.drop_duplicates(subset=['id_departamento'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_departamento', 'nombre', 'id_facultad']
    
    return df_válidos[columnas], stats


def transformar_programa(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_PROGRAMA.
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_programa'] = df['id_programa_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['nombre'] = df['nombre_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['tipo'] = df['tipo_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['duracion_anios'] = df['duracion_anios_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    df['id_facultad'] = df['id_facultad_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    
    df['es_válido'] = df.apply(
        lambda row: (
            row['id_programa'] is not None and
            row['nombre'] is not None and
            row['id_facultad'] is not None
        ),
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    df_válidos = df_válidos.drop_duplicates(subset=['id_programa'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_programa', 'nombre', 'tipo', 'duracion_anios', 'id_facultad']
    
    return df_válidos[columnas], stats


def transformar_curso(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_CURSO.
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_curso'] = df['id_curso_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['codigo'] = df['codigo_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['nombre'] = df['nombre_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['horas_teorica'] = df['horas_teorica_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['horas_ejercicios'] = df['horas_ejercicios_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    df['horas_laboratorio'] = df['horas_laboratorio_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    df['nivel'] = df['nivel_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    
    df['es_válido'] = df.apply(
        lambda row: (
            row['id_curso'] is not None and
            row['nombre'] is not None
        ),
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    df_válidos = df_válidos.drop_duplicates(subset=['id_curso'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_curso', 'codigo', 'nombre', 'horas_teorica', 'horas_ejercicios',
                'horas_laboratorio', 'nivel']
    
    return df_válidos[columnas], stats


def transformar_dictado(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_DICTADO.
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_dictado'] = df['id_dictado_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['id_curso'] = df['id_curso_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['id_docente'] = df['id_docente_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['id_programa'] = df['id_programa_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['periodo'] = df['periodo_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['turno'] = df['turno_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['aula'] = df['aula_raw'].apply(lambda x: cleaner.limpiar_string(x))
    df['cupo_maximo'] = df['cupo_maximo_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    
    df['es_válido'] = df.apply(
        lambda row: (
            row['id_dictado'] is not None and
            row['id_curso'] is not None and
            row['id_docente'] is not None
        ),
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    df_válidos = df_válidos.drop_duplicates(subset=['id_dictado'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_dictado', 'id_curso', 'id_docente', 'id_programa', 'periodo',
                'turno', 'aula', 'cupo_maximo']
    
    return df_válidos[columnas], stats


def transformar_inscripcion(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_INSCRIPCION.
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_inscripcion'] = df['id_inscripcion_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    df['id_estudiante'] = df['id_estudiante_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    df['id_dictado'] = df['id_dictado_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['fecha_inscripcion'] = df['fecha_inscripcion_raw'].apply(
        lambda x: cleaner.limpiar_fecha(x)
    )
    df['estado'] = df['estado_raw'].apply(lambda x: cleaner.limpiar_string(x))
    
    df['es_válido'] = df.apply(
        lambda row: (
            row['id_inscripcion'] is not None and
            row['id_estudiante'] is not None and
            row['id_dictado'] is not None
        ),
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    df_válidos = df_válidos.drop_duplicates(subset=['id_inscripcion'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_inscripcion', 'id_estudiante', 'id_dictado', 'fecha_inscripcion', 'estado']
    
    return df_válidos[columnas], stats


def transformar_curso_programa(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_CURSO_PROGRAMA.
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_curso'] = df['id_curso_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['id_programa'] = df['id_programa_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    
    df['es_válido'] = df.apply(
        lambda row: row['id_curso'] is not None and row['id_programa'] is not None,
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    df_válidos = df_válidos.drop_duplicates(subset=['id_curso', 'id_programa'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_curso', 'id_programa']
    
    return df_válidos[columnas], stats


def transformar_evaluacion_curso(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma datos de STG_EVALUACION_CURSO.
    """
    cleaner = DataCleaner()
    stats = {'total': len(df), 'válidos': 0, 'rechazados': 0, 'duplicados': 0}
    
    df['id_evaluacion'] = df['id_evaluacion_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'int')
    )
    df['id_dictado'] = df['id_dictado_raw'].apply(lambda x: cleaner.limpiar_numero(x, 'int'))
    df['puntaje_dictado'] = df['puntaje_dictado_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'float')
    )
    df['puntaje_contenido'] = df['puntaje_contenido_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'float')
    )
    df['valoracion_general'] = df['valoracion_general_raw'].apply(
        lambda x: cleaner.limpiar_numero(x, 'float')
    )
    
    df['es_válido'] = df.apply(
        lambda row: row['id_evaluacion'] is not None and row['id_dictado'] is not None,
        axis=1
    )
    
    df_válidos = df[df['es_válido']].copy()
    stats['válidos'] = len(df_válidos)
    stats['rechazados'] = len(df) - len(df_válidos)
    
    df_válidos = df_válidos.drop_duplicates(subset=['id_evaluacion'], keep='first')
    stats['duplicados'] = stats['válidos'] - len(df_válidos)
    
    columnas = ['id_evaluacion', 'id_dictado', 'puntaje_dictado', 'puntaje_contenido',
                'valoracion_general']
    
    return df_válidos[columnas], stats

print("[OK] Funciones de transformación cargadas")


# %%
# ============================================
# FUNCIÓN DE INSERCIÓN CON LOTES Y TRANSACCIONES
# ============================================

def insertar_en_lotes(df: pd.DataFrame, tabla_destino: str, tamaño_lote: int = 500) -> Dict:
    """
    Inserta datos en el DWH en lotes con manejo de transacciones.
    
    Caracterísitcas:
    - Procesamiento por lotes para optimizar performance
    - Transacciones explícitas para integridad
    - Rollback automático en caso de error
    - Reporte detallado
    """
    if df.empty:
        logger.warning(f"DataFrame vacío para {tabla_destino}")
        return {'insertados': 0, 'errores': 0, 'lotes': 0}
    
    resultados = {'insertados': 0, 'errores': 0, 'lotes': 0}
    
    # Calcular número de lotes
    num_lotes = (len(df) + tamaño_lote - 1) // tamaño_lote
    
    try:
        # Truncate tabla antes de insertar (idempotencia)
        with engine_dwh.begin() as connection:
            connection.execute(text(f"TRUNCATE TABLE {tabla_destino}"))
            logger.info(f"TRUNCATE: {tabla_destino}")
        
        # Procesar lotes
        for i in range(num_lotes):
            inicio = i * tamaño_lote
            fin = min(inicio + tamaño_lote, len(df))
            lote = df.iloc[inicio:fin].copy()
            
            try:
                # Inserción con transacción
                lote.to_sql(
                    name=tabla_destino,
                    con=engine_dwh,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                registros_lote = len(lote)
                resultados['insertados'] += registros_lote
                resultados['lotes'] += 1
                
                print(f"  Lote {i+1}/{num_lotes}: {registros_lote} registros insertados")
                logger.info(f"Lote {i+1}/{num_lotes} en {tabla_destino}: {registros_lote} registros")
                
            except Exception as e:
                resultados['errores'] += len(lote)
                logger.error(f"Error en lote {i+1} de {tabla_destino}: {str(e)}")
                print(f"  [ERROR] Lote {i+1}/{num_lotes}: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error crítico en inserción de {tabla_destino}: {str(e)}")
        print(f"[ERROR CRÍTICO] {tabla_destino}: {str(e)}")
    
    return resultados

print("[OK] Función de inserción con lotes cargada")

# %%
# ============================================
# ORQUESTACIÓN DE TRANSFORMACIÓN
# ============================================

print("\n" + "="*80)
print("INICIANDO TRANSFORMACIÓN - ETL STAGING -> DWH")
print("="*80 + "\n")

# Mapeo de tablas: (tabla_staging, tabla_dwh, función_transformación)
transformaciones = [
    ('stg_facultad', 'Facultad', transformar_facultad),
    ('stg_departamento', 'Departamento', transformar_departamento),
    ('stg_programa', 'Programa', transformar_programa),
    ('stg_curso', 'Curso', transformar_curso),
    ('stg_curso_programa', 'CursoPrograma', transformar_curso_programa),
    ('stg_docente', 'Docente', transformar_docente),
    ('stg_estudiante', 'Estudiante', transformar_estudiante),
    ('stg_dictado', 'Dictado', transformar_dictado),
    ('stg_inscripcion', 'Inscripcion', transformar_inscripcion),
    ('stg_examen', 'Examen', transformar_examen),
    ('stg_evaluacion_curso', 'EvaluacionCurso', transformar_evaluacion_curso),
]

reporte_general = {}

for tabla_stg, tabla_dwh, funcion_transform in transformaciones:
    print(f"\n{'='*80}")
    print(f"Transformando: {tabla_stg} -> {tabla_dwh}")
    print(f"{'='*80}")
    
    try:
        # 1. LECTURA desde Staging
        print(f"\n[1] Leyendo datos desde {tabla_stg}...")
        df_staging = pd.read_sql(f"SELECT * FROM {tabla_stg}", con=engine_stg)
        print(f"    → {len(df_staging)} registros leídos")
        logger.info(f"Lectura {tabla_stg}: {len(df_staging)} registros")
        
        if df_staging.empty:
            print(f"    [WARN] Tabla {tabla_stg} vacía")
            logger.warning(f"Tabla vacía: {tabla_stg}")
            reporte_general[tabla_stg] = {
                'lectura': 0,
                'transformacion': {'total': 0},
                'insercion': {'insertados': 0}
            }
            continue
        
        # 2. TRANSFORMACIÓN
        print(f"\n[2] Aplicando transformaciones...")
        df_transformado, stats_transform = funcion_transform(df_staging)
        
        print(f"    → Total: {stats_transform['total']}")
        print(f"    → Válidos: {stats_transform['válidos']}")
        print(f"    → Rechazados: {stats_transform['rechazados']}")
        print(f"    → Duplicados eliminados: {stats_transform['duplicados']}")
        print(f"    → Registros finales: {len(df_transformado)}")
        
        logger.info(f"Transformación {tabla_stg}: {len(df_transformado)} registros válidos")
        
        # 3. INSERCIÓN en DWH
        print(f"\n[3] Insertando en DWH ({tabla_dwh})...")
        stats_insert = insertar_en_lotes(df_transformado, tabla_dwh, tamaño_lote=500)
        
        print(f"    → Insertados: {stats_insert['insertados']}")
        print(f"    → Errores: {stats_insert['errores']}")
        print(f"    → Lotes procesados: {stats_insert['lotes']}")
        
        logger.info(f"Inserción {tabla_dwh}: {stats_insert['insertados']} registros")
        
        # 4. VALIDACIÓN POST-INSERCIÓN
        print(f"\n[4] Validando integridad en DWH...")
        with engine_dwh.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {tabla_dwh}"))
            count_final = result.fetchone()[0]
        
        status = "[OK]" if count_final == stats_insert['insertados'] else "[WARN]"
        print(f"    {status} Registros en {tabla_dwh}: {count_final}")
        
        reporte_general[tabla_stg] = {
            'lectura': len(df_staging),
            'transformacion': stats_transform,
            'insercion': stats_insert,
            'final': count_final
        }
        
        print(f"\n[OK] Transformación completada para {tabla_stg}")
        logger.info(f"COMPLETADO: {tabla_stg} -> {tabla_dwh}")
        
    except Exception as e:
        print(f"\n[ERROR] Fallo en transformación de {tabla_stg}: {str(e)}")
        logger.error(f"Error en {tabla_stg}: {str(e)}", exc_info=True)
        reporte_general[tabla_stg] = {'error': str(e)}

print("\n" + "="*80)
print("TRANSFORMACIÓN COMPLETADA")
print("="*80)

# %%
# ============================================
# REPORTE FINAL
# ============================================

print("\n" + "="*80)
print("REPORTE DE TRANSFORMACIÓN")
print("="*80)

for tabla, stats in reporte_general.items():
    if 'error' in stats:
        print(f"\n[ERROR] {tabla}: {stats['error']}")
    else:
        print(f"\n{tabla}:")
        print(f"  Lectura: {stats['lectura']:6} registros")
        print(f"  Válidos: {stats['transformacion']['válidos']:6} | Rechazados: {stats['transformacion']['rechazados']:6}")
        print(f"  Duplicados: {stats['transformacion']['duplicados']:6}")
        print(f"  Insertados: {stats['insercion']['insertados']:6} | Errores: {stats['insercion']['errores']:6}")
        print(f"  Final en DWH: {stats['final']:6} registros")

# Resumen
print(f"\n{'='*80}")
print("RESUMEN GENERAL")
print(f"{'='*80}")

total_leidos = sum(s['lectura'] for s in reporte_general.values() if 'lectura' in s)
total_válidos = sum(s['transformacion']['válidos'] for s in reporte_general.values() if 'transformacion' in s)
total_rechazados = sum(s['transformacion']['rechazados'] for s in reporte_general.values() if 'transformacion' in s)
total_duplicados = sum(s['transformacion']['duplicados'] for s in reporte_general.values() if 'transformacion' in s)
total_insertados = sum(s['insercion']['insertados'] for s in reporte_general.values() if 'insercion' in s)
total_errores = sum(s['insercion']['errores'] for s in reporte_general.values() if 'insercion' in s)

print(f"\nTotal registros leídos: {total_leidos}")
print(f"Total registros válidos: {total_válidos}")
print(f"Total registros rechazados: {total_rechazados}")
print(f"Total duplicados eliminados: {total_duplicados}")
print(f"Total registros insertados: {total_insertados}")
print(f"Total errores en inserción: {total_errores}")

porcentaje_válidos = (total_válidos / total_leidos * 100) if total_leidos > 0 else 0
porcentaje_insertados = (total_insertados / total_válidos * 100) if total_válidos > 0 else 0

print(f"\nCalidad de datos: {porcentaje_válidos:.2f}% válidos")
print(f"Tasa de inserción: {porcentaje_insertados:.2f}%")

if total_errores == 0 and total_insertados == total_válidos:
    print(f"\n[OK] TRANSFORMACIÓN EXITOSA SIN ERRORES ✓")
    logger.info("Transformación completada exitosamente")
else:
    print(f"\n[WARN] Se encontraron problemas - revisar log")
    logger.warning("Transformación completada con problemas")

print(f"\nLog guardado en: {log_filename}")
