#!/usr/bin/env python
# coding: utf-8

"""
ETL Transformación - De STG_Universidad a dw_universidad

Este script transforma los datos crudos de staging hacia el modelo dimensional
real del Data Warehouse definido en CreacionDWH_Universidad.sql:

Dimensiones:
- Tiempo
- Alumno
- Dictado

Hechos:
- Inscripcion
- ExamenAlumno
- EvaluacionDictado

La transformación NO carga tablas operacionales intermedias como Facultad,
Departamento, Programa, Curso, Docente o Estudiante, porque esas tablas no
existen en el esquema dimensional del DWH. Sus datos se integran/denormalizan
dentro de las dimensiones Alumno y Dictado.
"""

import logging
import os
import sys
import warnings
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

warnings.filterwarnings("ignore")

# ============================================
# RUTAS E IMPORTS DEL PROYECTO
# ============================================

try:
    SCRIPT_DIR = Path(__file__).resolve().parent
except NameError:
    SCRIPT_DIR = Path.cwd().resolve()

PROJECT_TP2_DIR = SCRIPT_DIR.parent
if str(PROJECT_TP2_DIR) not in sys.path:
    sys.path.append(str(PROJECT_TP2_DIR))

from logging_config import LoggerManager

# ============================================
# CONFIGURACIÓN DE CREDENCIALES Y CONEXIÓN
# ============================================

load_dotenv()

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
STG_DATABASE = os.getenv("STG_DATABASE")
DWH_DATABASE = os.getenv("DWH_DATABASE")

VARIABLES_REQUERIDAS = {
    "DB_USER": USER,
    "DB_PASSWORD": PASSWORD,
    "DB_HOST": HOST,
    "DB_PORT": PORT,
    "STG_DATABASE": STG_DATABASE,
    "DWH_DATABASE": DWH_DATABASE,
}

faltantes = [nombre for nombre, valor in VARIABLES_REQUERIDAS.items() if not valor]
if faltantes:
    raise EnvironmentError(
        "Faltan variables de entorno requeridas para la conexión: "
        + ", ".join(faltantes)
    )

engine_stg = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{STG_DATABASE}",
    poolclass=NullPool,
)

engine_dwh = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DWH_DATABASE}",
    poolclass=NullPool,
)

logger = LoggerManager.configurar(
    "transformacion",
    ruta_raiz=str(SCRIPT_DIR),
    carpeta_logs="logs",
)

for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler) and not isinstance(
        handler, logging.FileHandler
    ):
        handler.setLevel(logging.WARNING)

print(f"[OK] Conexiones configuradas | STG={STG_DATABASE} | DWH={DWH_DATABASE}")


# ============================================
# CONSTANTES DEL DWH
# ============================================

TABLAS_DWH = [
    "Tiempo",
    "Dictado",
    "Alumno",
    "Inscripcion",
    "ExamenAlumno",
    "EvaluacionDictado",
]

# Primero hechos, luego dimensiones. Se desactivan FK checks durante el truncate.
ORDEN_TRUNCATE = [
    "EvaluacionDictado",
    "ExamenAlumno",
    "Inscripcion",
    "Alumno",
    "Dictado",
    "Tiempo",
]

MESES_ES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


# ============================================
# FUNCIONES DE LIMPIEZA Y VALIDACIÓN
# ============================================


class DataCleaner:
    """Funciones reutilizables de limpieza y validación de datos de staging."""

    @staticmethod
    def limpiar_string(valor) -> Optional[str]:
        """Limpia strings, nulos textuales, espacios y mojibake evidente."""
        if pd.isna(valor) or valor is None:
            return None

        texto = str(valor).strip()
        if texto.lower() in {"", "null", "none", "n/a", "na", "sin dato", "s/d"}:
            return None

        # Solo intenta reparar encoding si detecta caracteres típicos de mojibake.
        if any(marca in texto for marca in ["Ã", "Â", "�"]):
            for origen, destino in [("latin-1", "utf-8"), ("cp1252", "utf-8")]:
                try:
                    reparado = texto.encode(origen).decode(destino)
                    if "�" not in reparado:
                        texto = reparado
                        break
                except Exception:
                    continue

        return " ".join(texto.split())

    @staticmethod
    def limpiar_numero(valor, tipo: str = "float", requerido: bool = False):
        """Convierte números sucios a int o float."""
        if pd.isna(valor) or valor is None:
            if requerido:
                LoggerManager.warning("Valor numérico requerido faltante")
            return None

        texto = str(valor).strip()
        if texto.lower() in {"", "null", "none", "n/a", "na", "sin dato", "s/d"}:
            return None

        try:
            texto = texto.replace(".", "") if texto.count(".") > 1 else texto
            texto = texto.replace(",", ".").replace(" ", "")
            numero = float(texto)

            if tipo == "int":
                if not numero.is_integer():
                    LoggerManager.warning(
                        f"Número no entero convertido por truncamiento: '{valor}'"
                    )
                return int(numero)
            if tipo == "float":
                return float(numero)
            return None
        except Exception:
            LoggerManager.warning(f"No se pudo convertir '{valor}' a {tipo}")
            return None

    @staticmethod
    def limpiar_fecha(valor) -> Optional[date]:
        """Convierte fechas desde múltiples formatos comunes."""
        if pd.isna(valor) or valor is None:
            return None

        texto = str(valor).strip()
        if texto.lower() in {"", "null", "none", "n/a", "na", "sin dato", "s/d"}:
            return None

        formatos = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%Y%m%d",
            "%d-%m-%Y",
            "%m-%d-%Y",
            "%Y",
            "%d/%m/%y",
            "%Y/%m/%d",
        ]

        for fmt in formatos:
            try:
                return pd.to_datetime(texto, format=fmt).date()
            except Exception:
                continue

        try:
            return pd.to_datetime(texto, dayfirst=True).date()
        except Exception:
            LoggerManager.warning(f"No se pudo parsear fecha: '{valor}'")
            return None

    @staticmethod
    def limpiar_genero(valor) -> Optional[str]:
        """Normaliza valores de género al dominio usado en el DWH."""
        texto = DataCleaner.limpiar_string(valor)
        if texto is None:
            return None

        texto = texto.upper()
        if texto in {"M", "MASCULINO", "MALE", "HOMBRE", "1"}:
            return "M"
        if texto in {"F", "FEMENINO", "FEMALE", "MUJER", "2"}:
            return "F"
        if texto in {"X", "OTRO", "OTRA", "NO BINARIO", "NB"}:
            return "X"

        LoggerManager.warning(f"Género desconocido: '{valor}'")
        return None

    @staticmethod
    def validar_dni(dni) -> bool:
        """Valida DNI argentino en rango de 7 a 8 dígitos."""
        if pd.isna(dni) or dni is None:
            return False
        try:
            dni_int = int(dni)
            return 1_000_000 <= dni_int <= 99_999_999
        except Exception:
            return False

    @staticmethod
    def validar_nota(nota, minimo: float = 0, maximo: float = 10) -> bool:
        """Valida que una nota/puntaje esté dentro de un rango."""
        if pd.isna(nota) or nota is None:
            return False
        try:
            return minimo <= float(nota) <= maximo
        except Exception:
            return False

    @staticmethod
    def normalizar_estado_inscripcion(valor) -> Optional[str]:
        texto = DataCleaner.limpiar_string(valor)
        if texto is None:
            return None

        normalizado = texto.strip().lower()
        if normalizado in {
            "activa",
            "activo",
            "inscripto",
            "inscripta",
            "cursando",
            "regular",
        }:
            return "Activa"
        if normalizado in {"aprobada", "aprobado", "finalizada", "finalizado"}:
            return "Aprobada"
        if normalizado in {"abandonada", "abandonado", "abandono", "baja"}:
            return "Abandonada"
        if normalizado in {
            "rechazada",
            "rechazado",
            "cancelada",
            "cancelado",
            "anulada",
            "anulado",
        }:
            return "Cancelada"
        return texto.title()

    @staticmethod
    def normalizar_periodo(valor) -> Optional[int]:
        """Normaliza período académico a entero compatible con Dictado.periodo."""
        texto = DataCleaner.limpiar_string(valor)
        if texto is None:
            return None

        normalizado = texto.strip().upper()
        if normalizado in {"C1", "1", "P1", "PRIMER", "PRIMERO", "PRIMER CUATRIMESTRE"}:
            return 1
        if normalizado in {"C2", "2", "P2", "SEGUNDO", "SEGUNDO CUATRIMESTRE"}:
            return 2

        numero = DataCleaner.limpiar_numero(texto, "int")
        if numero in {1, 2}:
            return int(numero)

        LoggerManager.warning(f"Período académico desconocido: '{valor}'")
        return None

    @staticmethod
    def normalizar_resultado_examen(valor, nota=None) -> Optional[bool]:
        """Devuelve True/False para aprobado. Si no hay texto, infiere por nota >= 4."""
        texto = DataCleaner.limpiar_string(valor)
        if texto is not None:
            normalizado = texto.lower()
            if normalizado in {"aprobado", "aprob", "sí", "si", "1", "true", "a"}:
                return True
            if normalizado in {"desaprobado", "desaprob", "no", "0", "false", "d"}:
                return False
            if normalizado in {"ausente", "pendiente", "no rindio", "no rindió"}:
                return False

        if nota is not None and not pd.isna(nota):
            return float(nota) >= 4

        return None


# ============================================
# UTILIDADES GENERALES
# ============================================


def estadisticas(
    total: int, validos: int, duplicados: int = 0, motivo: Optional[str] = None
) -> Dict[str, object]:
    total_int = int(total)
    validos_int = int(validos)
    duplicados_int = int(duplicados)
    rechazados_int = max(total_int - validos_int - duplicados_int, 0)

    resultado: Dict[str, object] = {
        "total": total_int,
        "válidos": validos_int,
        "rechazados": rechazados_int,
        "duplicados": duplicados_int,
    }
    if motivo:
        resultado["motivo"] = motivo
    return resultado


def leer_tabla_staging(nombre_tabla: str) -> pd.DataFrame:
    LoggerManager.info(f"Leyendo staging: {nombre_tabla}")
    return pd.read_sql(f"SELECT * FROM {nombre_tabla}", con=engine_stg)


def contar_tabla_dwh(nombre_tabla: str) -> int:
    with engine_dwh.connect() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {nombre_tabla}")).scalar())


def fecha_desde_anio(anio) -> Optional[date]:
    anio_limpio = DataCleaner.limpiar_numero(anio, "int")
    if anio_limpio is None:
        return None
    if 1900 <= anio_limpio <= date.today().year + 10:
        return date(int(anio_limpio), 1, 1)
    LoggerManager.warning(f"Año fuera de rango lógico: {anio}")
    return None


def calcular_edad(
    fecha_nacimiento: Optional[date], fecha_ingreso: Optional[date]
) -> Optional[int]:
    if not fecha_nacimiento or not fecha_ingreso:
        return None
    edad = fecha_ingreso.year - fecha_nacimiento.year
    if (fecha_ingreso.month, fecha_ingreso.day) < (
        fecha_nacimiento.month,
        fecha_nacimiento.day,
    ):
        edad -= 1
    return edad if 0 <= edad <= 120 else None


def tiempo_skey(fecha: Optional[date]) -> Optional[int]:
    if not fecha:
        return None
    return fecha.year * 10000 + fecha.month * 100 + fecha.day


def periodo_academico(fecha: date) -> str:
    cuatrimestre = "C1" if fecha.month <= 7 else "C2"
    return f"{cuatrimestre}-{fecha.year}"


def quitar_duplicados(
    df: pd.DataFrame, subset: List[str], keep: str = "first"
) -> Tuple[pd.DataFrame, int]:
    antes = len(df)
    limpio = df.drop_duplicates(subset=subset, keep=keep).copy()
    return limpio, antes - len(limpio)


def registrar_rechazos(nombre: str, total: int, validos: int) -> None:
    rechazados = total - validos
    if rechazados > 0:
        LoggerManager.warning(f"{nombre}: {rechazados} registros rechazados")


# ============================================
# TRANSFORMACIONES BASE DESDE STAGING
# ============================================


def transformar_estudiante_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    df["id_estudiante"] = df["id_estudiante_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["dni"] = df["dni_raw"].apply(lambda x: cleaner.limpiar_numero(x, "int"))
    df["apellido"] = df["apellido_raw"].apply(cleaner.limpiar_string)
    df["nombre"] = df["nombre_raw"].apply(cleaner.limpiar_string)
    df["genero"] = df["genero_raw"].apply(cleaner.limpiar_genero)
    df["fecha_nacimiento"] = df["fecha_nacimiento_raw"].apply(cleaner.limpiar_fecha)
    df["nacionalidad"] = df["nacionalidad_raw"].apply(cleaner.limpiar_string)
    df["id_programa"] = df["id_programa_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["anio_ingreso"] = df["anio_ingreso_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["fecha_ingreso"] = df["anio_ingreso"].apply(fecha_desde_anio)

    valido = (
        df["id_estudiante"].notna()
        & df["dni"].apply(cleaner.validar_dni)
        & df["apellido"].notna()
        & df["nombre"].notna()
        & df["id_programa"].notna()
    )

    validos = df[valido].copy()
    registrar_rechazos("stg_estudiante", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_estudiante"], keep="first")
    validos, duplicados_dni = quitar_duplicados(validos, ["dni"], keep="first")
    duplicados += duplicados_dni

    columnas = [
        "id_estudiante",
        "dni",
        "apellido",
        "nombre",
        "genero",
        "fecha_nacimiento",
        "nacionalidad",
        "id_programa",
        "fecha_ingreso",
    ]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_programa_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    df["id_programa"] = df["id_programa_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["nombre_programa"] = df["nombre_raw"].apply(cleaner.limpiar_string)
    df["tipo_programa"] = df["tipo_raw"].apply(cleaner.limpiar_string)
    df["duracion_anios_programa"] = df["duracion_anios_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_facultad"] = df["id_facultad_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )

    valido = df["id_programa"].notna() & df["nombre_programa"].notna()
    validos = df[valido].copy()
    registrar_rechazos("stg_programa", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_programa"], keep="first")

    columnas = [
        "id_programa",
        "nombre_programa",
        "tipo_programa",
        "duracion_anios_programa",
        "id_facultad",
    ]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_facultad_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    df["id_facultad"] = df["id_facultad_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["nombre_facultad"] = df["nombre_raw"].apply(cleaner.limpiar_string)
    df["ciudad_facultad"] = df["ciudad_raw"].apply(cleaner.limpiar_string)
    df["provincia_facultad"] = df["provincia_raw"].apply(cleaner.limpiar_string)

    valido = df["id_facultad"].notna() & df["nombre_facultad"].notna()
    validos = df[valido].copy()
    registrar_rechazos("stg_facultad", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_facultad"], keep="first")

    columnas = [
        "id_facultad",
        "nombre_facultad",
        "ciudad_facultad",
        "provincia_facultad",
    ]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_departamento_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    df["id_departamento"] = df["id_departamento_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["nombre_departamento"] = df["nombre_raw"].apply(cleaner.limpiar_string)
    df["id_facultad"] = df["id_facultad_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )

    valido = df["id_departamento"].notna() & df["nombre_departamento"].notna()
    validos = df[valido].copy()
    registrar_rechazos("stg_departamento", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_departamento"], keep="first")

    columnas = ["id_departamento", "nombre_departamento", "id_facultad"]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_docente_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    df["id_docente"] = df["id_docente_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["apellido_docente"] = df["apellido_raw"].apply(cleaner.limpiar_string)
    df["nombre_docente"] = df["nombre_raw"].apply(cleaner.limpiar_string)
    df["titulo_docente"] = df["titulo_raw"].apply(cleaner.limpiar_string)
    df["categoria_docente"] = df["categoria_raw"].apply(cleaner.limpiar_string)
    df["dedicacion_docente"] = df["dedicacion_raw"].apply(cleaner.limpiar_string)
    df["id_departamento"] = df["id_departamento_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )

    valido = (
        df["id_docente"].notna()
        & df["apellido_docente"].notna()
        & df["nombre_docente"].notna()
    )
    validos = df[valido].copy()
    registrar_rechazos("stg_docente", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_docente"], keep="first")

    columnas = [
        "id_docente",
        "apellido_docente",
        "nombre_docente",
        "titulo_docente",
        "categoria_docente",
        "dedicacion_docente",
        "id_departamento",
    ]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_curso_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    df["id_curso"] = df["id_curso_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["codigo_curso"] = df["codigo_raw"].apply(cleaner.limpiar_string)
    df["nombre_curso"] = df["nombre_raw"].apply(cleaner.limpiar_string)
    df["horas_teo_curso"] = df["horas_teorica_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["horas_prac_curso"] = df["horas_ejercicios_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["horas_lab_curso"] = df["horas_laboratorio_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["nivel_curso"] = df["nivel_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )

    # Validaciones de dominio: horas y nivel no negativos.
    for columna in [
        "horas_teo_curso",
        "horas_prac_curso",
        "horas_lab_curso",
        "nivel_curso",
    ]:
        df.loc[df[columna].notna() & (df[columna] < 0), columna] = None

    valido = df["id_curso"].notna() & df["nombre_curso"].notna()
    validos = df[valido].copy()
    registrar_rechazos("stg_curso", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_curso"], keep="first")

    columnas = [
        "id_curso",
        "codigo_curso",
        "nombre_curso",
        "horas_teo_curso",
        "horas_prac_curso",
        "horas_lab_curso",
        "nivel_curso",
    ]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_dictado_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    df["id_dictado"] = df["id_dictado_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_curso"] = df["id_curso_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_docente"] = df["id_docente_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_programa"] = df["id_programa_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["periodo"] = df["periodo_raw"].apply(cleaner.normalizar_periodo)
    df["turno"] = df["turno_raw"].apply(cleaner.limpiar_string)
    df["aula"] = df["aula_raw"].apply(cleaner.limpiar_string)
    df["cupo_maximo"] = df["cupo_maximo_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )

    df.loc[df["cupo_maximo"].notna() & (df["cupo_maximo"] < 0), "cupo_maximo"] = None

    valido = (
        df["id_dictado"].notna() & df["id_curso"].notna() & df["id_docente"].notna()
    )
    validos = df[valido].copy()
    registrar_rechazos("stg_dictado", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_dictado"], keep="first")

    columnas = [
        "id_dictado",
        "id_curso",
        "id_docente",
        "id_programa",
        "periodo",
        "turno",
        "aula",
        "cupo_maximo",
    ]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_inscripcion_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    df["id_inscripcion"] = df["id_inscripcion_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_estudiante"] = df["id_estudiante_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_dictado"] = df["id_dictado_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["fecha_inscripcion"] = df["fecha_inscripcion_raw"].apply(cleaner.limpiar_fecha)
    df["estado"] = df["estado_raw"].apply(cleaner.normalizar_estado_inscripcion)

    valido = (
        df["id_inscripcion"].notna()
        & df["id_estudiante"].notna()
        & df["id_dictado"].notna()
        & df["fecha_inscripcion"].notna()
    )
    validos = df[valido].copy()
    registrar_rechazos("stg_inscripcion", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_inscripcion"], keep="first")

    columnas = [
        "id_inscripcion",
        "id_estudiante",
        "id_dictado",
        "fecha_inscripcion",
        "estado",
    ]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_examen_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    df["id_examen"] = df["id_examen_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_inscripcion"] = df["id_inscripcion_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["fecha"] = df["fecha_raw"].apply(cleaner.limpiar_fecha)
    df["nota"] = df["nota_raw"].apply(lambda x: cleaner.limpiar_numero(x, "float"))
    df["numero_intento"] = df["numero_intento_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["aprobado"] = df.apply(
        lambda row: cleaner.normalizar_resultado_examen(
            row["resultado_raw"], row["nota"]
        ),
        axis=1,
    )

    valido = (
        df["id_examen"].notna()
        & df["id_inscripcion"].notna()
        & df["fecha"].notna()
        & df["nota"].apply(lambda x: cleaner.validar_nota(x, 0, 10))
        & df["numero_intento"].notna()
        & (df["numero_intento"] > 0)
    )
    validos = df[valido].copy()
    registrar_rechazos("stg_examen", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_examen"], keep="first")

    columnas = [
        "id_examen",
        "id_inscripcion",
        "fecha",
        "nota",
        "numero_intento",
        "aprobado",
    ]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_evaluacion_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    for columna_raw in ["id_estudiante_raw", "fecha_evaluacion_raw"]:
        if columna_raw not in df.columns:
            df[columna_raw] = None
            LoggerManager.warning(
                f"stg_evaluacion_curso no contiene {columna_raw}; los registros quedarán inválidos para EvaluacionDictado"
            )

    df["id_evaluacion"] = df["id_evaluacion_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_dictado"] = df["id_dictado_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_estudiante"] = df["id_estudiante_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["fecha_evaluacion"] = df["fecha_evaluacion_raw"].apply(cleaner.limpiar_fecha)
    df["puntaje_dictado"] = df["puntaje_dictado_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "float")
    )
    df["puntaje_contenido"] = df["puntaje_contenido_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "float")
    )
    df["valoracion_general"] = df["valoracion_general_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "float")
    )

    puntajes_validos = (
        df["puntaje_dictado"].apply(lambda x: cleaner.validar_nota(x, 0, 10))
        & df["puntaje_contenido"].apply(lambda x: cleaner.validar_nota(x, 0, 10))
        & df["valoracion_general"].apply(lambda x: cleaner.validar_nota(x, 0, 10))
    )

    valido = (
        df["id_evaluacion"].notna()
        & df["id_dictado"].notna()
        & df["id_estudiante"].notna()
        & df["fecha_evaluacion"].notna()
        & puntajes_validos
    )
    validos = df[valido].copy()
    registrar_rechazos("stg_evaluacion_curso", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_evaluacion"], keep="first")

    columnas = [
        "id_evaluacion",
        "id_dictado",
        "id_estudiante",
        "fecha_evaluacion",
        "puntaje_dictado",
        "puntaje_contenido",
        "valoracion_general",
    ]
    return validos[columnas], estadisticas(total, len(validos), duplicados)


# ============================================
# CONSTRUCCIÓN DE DIMENSIONES DEL DWH
# ============================================


def construir_dim_tiempo(fechas: Iterable[Optional[date]]) -> Tuple[pd.DataFrame, Dict]:
    fechas_validas = sorted({f for f in fechas if f is not None and not pd.isna(f)})
    registros = []

    for fecha in fechas_validas:
        registros.append(
            {
                "tiempoSKey": tiempo_skey(fecha),
                "fecha": fecha,
                "dia": fecha.day,
                "mes": MESES_ES[fecha.month],
                "año": fecha.year,
                "periodoAcademico": periodo_academico(fecha),
                "esFeriado": False,
            }
        )

    df_tiempo = pd.DataFrame(registros)
    return df_tiempo, estadisticas(len(fechas_validas), len(df_tiempo), 0)


def construir_dim_alumno(
    estudiantes: pd.DataFrame, programas: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict]:
    total = len(estudiantes)
    df = estudiantes.merge(programas, on="id_programa", how="left")

    faltan_programas = df["nombre_programa"].isna().sum()
    if faltan_programas > 0:
        LoggerManager.warning(
            f"Alumno: {faltan_programas} estudiantes sin programa encontrado; se cargan con atributos de programa NULL"
        )

    df["edadIngreso"] = df.apply(
        lambda row: calcular_edad(row["fecha_nacimiento"], row["fecha_ingreso"]), axis=1
    )
    df["valid_from"] = date.today()
    df["valid_to"] = None
    df["es_actual"] = True

    resultado = pd.DataFrame(
        {
            "idalumno": df["id_estudiante"],
            "dni": df["dni"],
            "nombre": df["nombre"],
            "apellido": df["apellido"],
            "genero": df["genero"],
            "fechaNacim": df["fecha_nacimiento"],
            "nacionalidad": df["nacionalidad"],
            "añoIngreso": df["fecha_ingreso"],
            "edadIngreso": df["edadIngreso"],
            "egresoCarrera": False,
            "añoEgreso": None,
            "abandonoCarrera": False,
            "añoAbandono": None,
            "nombrePrograma": df["nombre_programa"],
            "tipoPrograma": df["tipo_programa"],
            "duracionAñosPrograma": df["duracion_anios_programa"],
            "añoPlanPrograma": None,
            "valid_from": df["valid_from"],
            "valid_to": df["valid_to"],
            "es_actual": df["es_actual"],
        }
    )

    resultado, duplicados = quitar_duplicados(resultado, ["idalumno"], keep="first")
    return resultado, estadisticas(total, len(resultado), duplicados)


def construir_dim_dictado(
    dictados: pd.DataFrame,
    cursos: pd.DataFrame,
    docentes: pd.DataFrame,
    departamentos: pd.DataFrame,
    facultades: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict]:
    total = len(dictados)

    df = dictados.merge(cursos, on="id_curso", how="left")
    df = df.merge(docentes, on="id_docente", how="left")
    df = df.merge(departamentos, on="id_departamento", how="left")
    df = df.merge(facultades, on="id_facultad", how="left")

    for columna, etiqueta in [
        ("nombre_curso", "curso"),
        ("nombre_docente", "docente"),
        ("nombre_departamento", "departamento"),
        ("nombre_facultad", "facultad"),
    ]:
        faltantes = df[columna].isna().sum() if columna in df.columns else len(df)
        if faltantes > 0:
            LoggerManager.warning(
                f"Dictado: {faltantes} registros sin datos de {etiqueta}; se cargan con atributos NULL"
            )

    resultado = pd.DataFrame(
        {
            "idDictado": df["id_dictado"],
            "periodo": df["periodo"],
            "turno": df["turno"],
            "aula": df["aula"],
            "cupoMax": df["cupo_maximo"],
            "codigoCurso": df["codigo_curso"],
            "nombreCurso": df["nombre_curso"],
            "horasTeoCurso": df["horas_teo_curso"],
            "horasPracCurso": df["horas_prac_curso"],
            "horasLabCurso": df["horas_lab_curso"],
            "nivelCurso": df["nivel_curso"],
            "nombreDocente": df["nombre_docente"],
            "apellidoDocente": df["apellido_docente"],
            "tituloDocente": df["titulo_docente"],
            "categoriaDocente": df["categoria_docente"],
            "dedicacionDocente": df["dedicacion_docente"],
            "nombreDep": df["nombre_departamento"],
            "nombreFac": df["nombre_facultad"],
            "ciudadFac": df["ciudad_facultad"],
            "provFac": df["provincia_facultad"],
            "valid_from": date.today(),
            "valid_to": None,
            "es_actual": True,
        }
    )

    resultado, duplicados = quitar_duplicados(resultado, ["idDictado"], keep="first")
    return resultado, estadisticas(total, len(resultado), duplicados)


# ============================================
# MAPEO DE SURROGATE KEYS
# ============================================


def obtener_mapa_alumno() -> Dict[int, int]:
    df = pd.read_sql(
        "SELECT alumnoSKey, idalumno FROM Alumno WHERE es_actual = TRUE",
        con=engine_dwh,
    )
    return dict(zip(df["idalumno"], df["alumnoSKey"]))


def obtener_mapa_dictado() -> Dict[int, int]:
    df = pd.read_sql(
        "SELECT dictadoSKey, idDictado FROM Dictado WHERE es_actual = TRUE",
        con=engine_dwh,
    )
    return dict(zip(df["idDictado"], df["dictadoSKey"]))


def obtener_mapa_tiempo() -> Dict[date, int]:
    df = pd.read_sql("SELECT tiempoSKey, fecha FROM Tiempo", con=engine_dwh)
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
    return dict(zip(df["fecha"], df["tiempoSKey"]))


# ============================================
# CONSTRUCCIÓN DE HECHOS DEL DWH
# ============================================


def construir_fact_inscripcion(
    inscripciones: pd.DataFrame,
    mapa_alumno: Dict[int, int],
    mapa_dictado: Dict[int, int],
    mapa_tiempo: Dict[date, int],
) -> Tuple[pd.DataFrame, Dict]:
    total = len(inscripciones)
    df = inscripciones.copy()

    df["alumnoSKey"] = df["id_estudiante"].map(mapa_alumno)
    df["dictadoSKey"] = df["id_dictado"].map(mapa_dictado)
    df["tiempoSKey"] = df["fecha_inscripcion"].map(mapa_tiempo)
    df["abandono"] = (
        df["estado"]
        .fillna("")
        .str.lower()
        .isin(["abandonada", "abandonado", "abandono", "baja"])
    )

    valido = (
        df["alumnoSKey"].notna() & df["dictadoSKey"].notna() & df["tiempoSKey"].notna()
    )
    validos = df[valido].copy()
    registrar_rechazos(
        "Fact Inscripcion por claves no encontradas", total, len(validos)
    )

    resultado = validos[
        ["alumnoSKey", "tiempoSKey", "dictadoSKey", "estado", "abandono"]
    ].copy()
    resultado, duplicados = quitar_duplicados(
        resultado, ["alumnoSKey", "dictadoSKey"], keep="last"
    )

    return resultado, estadisticas(total, len(resultado), duplicados)


def construir_fact_examen_alumno(
    examenes: pd.DataFrame,
    inscripciones: pd.DataFrame,
    mapa_alumno: Dict[int, int],
    mapa_dictado: Dict[int, int],
    mapa_tiempo: Dict[date, int],
) -> Tuple[pd.DataFrame, Dict]:
    total = len(examenes)

    df = examenes.merge(
        inscripciones[["id_inscripcion", "id_estudiante", "id_dictado"]],
        on="id_inscripcion",
        how="left",
    )

    df["alumnoSKey"] = df["id_estudiante"].map(mapa_alumno)
    df["dictadoSKey"] = df["id_dictado"].map(mapa_dictado)
    df["tiempoSKey"] = df["fecha"].map(mapa_tiempo)

    valido = (
        df["alumnoSKey"].notna() & df["dictadoSKey"].notna() & df["tiempoSKey"].notna()
    )
    validos = df[valido].copy()
    registrar_rechazos(
        "Fact ExamenAlumno por claves no encontradas", total, len(validos)
    )

    validos = validos.sort_values(["fecha", "id_examen"])
    resultado = validos[
        [
            "alumnoSKey",
            "tiempoSKey",
            "dictadoSKey",
            "nota",
            "numero_intento",
            "aprobado",
        ]
    ].copy()
    resultado = resultado.rename(columns={"numero_intento": "nroIntentos"})
    resultado, duplicados = quitar_duplicados(
        resultado, ["alumnoSKey", "dictadoSKey", "nroIntentos"], keep="last"
    )

    return resultado, estadisticas(total, len(resultado), duplicados)


def construir_fact_evaluacion_dictado(
    evaluaciones: pd.DataFrame,
    mapa_dictado: Dict[int, int],
    mapa_alumno: Dict[int, int],
    mapa_tiempo: Dict[date, int],
) -> Tuple[pd.DataFrame, Dict]:
    """
    Construye la tabla de hecho EvaluacionDictado usando las claves naturales
    limpias de staging y los mapas de surrogate keys ya cargados en dimensiones.
    """
    total = len(evaluaciones)
    df = evaluaciones.copy()

    df["dictadoSKey"] = df["id_dictado"].map(mapa_dictado)
    df["alumnoSKey"] = df["id_estudiante"].map(mapa_alumno)
    df["tiempoSKey"] = df["fecha_evaluacion"].map(mapa_tiempo)

    valido = (
        df["dictadoSKey"].notna() & df["alumnoSKey"].notna() & df["tiempoSKey"].notna()
    )
    validos = df[valido].copy()
    registrar_rechazos(
        "Fact EvaluacionDictado por claves no encontradas", total, len(validos)
    )

    validos = validos.sort_values(["fecha_evaluacion", "id_evaluacion"])
    resultado = validos[
        [
            "dictadoSKey",
            "alumnoSKey",
            "tiempoSKey",
            "puntaje_dictado",
            "puntaje_contenido",
            "valoracion_general",
        ]
    ].copy()
    resultado = resultado.rename(
        columns={
            "puntaje_dictado": "notaDictado",
            "puntaje_contenido": "notaCont",
            "valoracion_general": "notaGeneral",
        }
    )
    resultado, duplicados = quitar_duplicados(
        resultado, ["dictadoSKey", "alumnoSKey", "tiempoSKey"], keep="last"
    )

    return resultado, estadisticas(total, len(resultado), duplicados)


# ============================================
# CARGA EN DWH
# ============================================


def truncar_dwh() -> None:
    """Vacía las tablas del DWH respetando dependencias mediante FK checks off."""
    LoggerManager.info("Iniciando TRUNCATE de tablas DWH")
    with engine_dwh.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        for tabla in ORDEN_TRUNCATE:
            conn.execute(text(f"TRUNCATE TABLE {tabla}"))
            LoggerManager.info(f"TRUNCATE TABLE {tabla}")
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
    LoggerManager.info("TRUNCATE de DWH finalizado")


def insertar_dataframe(
    df: pd.DataFrame, tabla_destino: str, tamanio_lote: int = 500
) -> Dict:
    """Inserta un DataFrame en una tabla ya existente del DWH."""
    if tabla_destino not in TABLAS_DWH:
        raise ValueError(f"Tabla destino no permitida para este DWH: {tabla_destino}")

    if df.empty:
        LoggerManager.warning(
            f"DataFrame vacío para {tabla_destino}; no se insertan registros"
        )
        return {"insertados": 0, "errores": 0, "lotes": 0}

    resultados = {"insertados": 0, "errores": 0, "lotes": 0}
    num_lotes = (len(df) + tamanio_lote - 1) // tamanio_lote

    for i in range(num_lotes):
        inicio = i * tamanio_lote
        fin = min(inicio + tamanio_lote, len(df))
        lote = df.iloc[inicio:fin].copy()

        try:
            lote.to_sql(
                name=tabla_destino,
                con=engine_dwh,
                if_exists="append",
                index=False,
                method="multi",
            )
            resultados["insertados"] += len(lote)
            resultados["lotes"] += 1
            LoggerManager.info(
                f"{tabla_destino} lote {i + 1}/{num_lotes}: {len(lote)} registros"
            )
        except Exception as exc:
            resultados["errores"] += len(lote)
            LoggerManager.error(
                f"Error insertando lote {i + 1} en {tabla_destino}: {exc}"
            )
            print(f"  [ERROR] {tabla_destino} - lote {i + 1}/{num_lotes}: {exc}")
            raise

    return resultados


def cargar_tabla(
    nombre_tabla: str, df: pd.DataFrame, reporte: Dict, stats_transformacion: Dict
) -> None:
    stats_insert = insertar_dataframe(df, nombre_tabla)
    final = contar_tabla_dwh(nombre_tabla)

    reporte[nombre_tabla] = {
        "transformacion": stats_transformacion,
        "insercion": stats_insert,
        "final": final,
    }

    print(
        f"  {nombre_tabla}: transformados={stats_transformacion['válidos']} | "
        f"insertados={stats_insert['insertados']} | errores={stats_insert['errores']} | final={final}"
    )


# ============================================
# ORQUESTACIÓN
# ============================================


def ejecutar_transformacion() -> Dict:
    print("\n=== Transformación dimensional STG -> DWH ===", flush=True)

    reporte: Dict = {}

    # 1. Lectura y limpieza base.
    print("[1/5] Limpieza y validación de staging...", flush=True)
    print(
        "  Nota: el DWH se reinicia en el paso [3/5], cuando la limpieza base ya terminó.",
        flush=True,
    )
    staging_transformaciones = {
        "facultades": ("stg_facultad", transformar_facultad_base),
        "departamentos": ("stg_departamento", transformar_departamento_base),
        "programas": ("stg_programa", transformar_programa_base),
        "cursos": ("stg_curso", transformar_curso_base),
        "docentes": ("stg_docente", transformar_docente_base),
        "estudiantes": ("stg_estudiante", transformar_estudiante_base),
        "dictados": ("stg_dictado", transformar_dictado_base),
        "inscripciones": ("stg_inscripcion", transformar_inscripcion_base),
        "examenes": ("stg_examen", transformar_examen_base),
        "evaluaciones": ("stg_evaluacion_curso", transformar_evaluacion_base),
    }

    datos: Dict[str, pd.DataFrame] = {}
    stats_base: Dict[str, Dict] = {}

    for clave, (tabla_stg, funcion) in staging_transformaciones.items():
        print(f"  Procesando {tabla_stg}...", flush=True)
        df_raw = leer_tabla_staging(tabla_stg)
        df_limpio, stats = funcion(df_raw)
        datos[clave] = df_limpio
        stats_base[tabla_stg] = stats
        if stats["rechazados"] > 0 or stats["duplicados"] > 0:
            print(
                f"    Atención: rechazados={stats['rechazados']} | duplicados={stats['duplicados']}",
                flush=True,
            )

    reporte["staging_limpieza"] = stats_base

    # 2. Construcción dimensional.
    print("[2/5] Construcción de dimensiones...", flush=True)
    fechas_tiempo = []
    if not datos["inscripciones"].empty:
        fechas_tiempo.extend(datos["inscripciones"]["fecha_inscripcion"].tolist())
    if not datos["examenes"].empty:
        fechas_tiempo.extend(datos["examenes"]["fecha"].tolist())
    if not datos["evaluaciones"].empty:
        fechas_tiempo.extend(datos["evaluaciones"]["fecha_evaluacion"].tolist())

    dim_tiempo, stats_tiempo = construir_dim_tiempo(fechas_tiempo)
    dim_alumno, stats_alumno = construir_dim_alumno(
        datos["estudiantes"], datos["programas"]
    )
    dim_dictado, stats_dictado = construir_dim_dictado(
        datos["dictados"],
        datos["cursos"],
        datos["docentes"],
        datos["departamentos"],
        datos["facultades"],
    )

    print(
        f"  Dimensiones listas: Tiempo={len(dim_tiempo)} | Alumno={len(dim_alumno)} | Dictado={len(dim_dictado)}",
        flush=True,
    )

    # 3. Truncate único de todo el DWH antes de cargar.
    print("[3/5] Reinicio controlado de tablas DWH...", flush=True)
    truncar_dwh()

    # 4. Carga de dimensiones.
    print("[4/5] Carga de dimensiones y hechos...", flush=True)
    cargar_tabla("Tiempo", dim_tiempo, reporte, stats_tiempo)
    cargar_tabla("Alumno", dim_alumno, reporte, stats_alumno)
    cargar_tabla("Dictado", dim_dictado, reporte, stats_dictado)

    # 5. Obtención de surrogate keys generadas.
    mapa_alumno = obtener_mapa_alumno()
    mapa_dictado = obtener_mapa_dictado()
    mapa_tiempo = obtener_mapa_tiempo()

    # 6. Construcción de hechos.
    fact_inscripcion, stats_fact_inscripcion = construir_fact_inscripcion(
        datos["inscripciones"], mapa_alumno, mapa_dictado, mapa_tiempo
    )
    fact_examen, stats_fact_examen = construir_fact_examen_alumno(
        datos["examenes"],
        datos["inscripciones"],
        mapa_alumno,
        mapa_dictado,
        mapa_tiempo,
    )
    fact_evaluacion, stats_fact_evaluacion = construir_fact_evaluacion_dictado(
        datos["evaluaciones"], mapa_dictado, mapa_alumno, mapa_tiempo
    )

    # 7. Carga de hechos.
    cargar_tabla("Inscripcion", fact_inscripcion, reporte, stats_fact_inscripcion)
    cargar_tabla("ExamenAlumno", fact_examen, reporte, stats_fact_examen)
    cargar_tabla("EvaluacionDictado", fact_evaluacion, reporte, stats_fact_evaluacion)

    # 8. Reporte final.
    imprimir_reporte(reporte)
    return reporte


def imprimir_reporte(reporte: Dict) -> None:
    print("[5/5] Reporte final", flush=True)

    problemas_staging = [
        (tabla, stats)
        for tabla, stats in reporte.get("staging_limpieza", {}).items()
        if stats["rechazados"] > 0 or stats["duplicados"] > 0
    ]
    if problemas_staging:
        print("\nStaging con registros rechazados o duplicados:")
        for tabla, stats in problemas_staging:
            print(
                f"  {tabla}: total={stats['total']} | rechazados={stats['rechazados']} | duplicados={stats['duplicados']}"
            )

    print("\nCarga DWH:")
    for tabla in TABLAS_DWH:
        stats = reporte.get(tabla)
        if not stats:
            continue
        transformacion = stats["transformacion"]
        insercion = stats["insercion"]
        print(
            f"  {tabla}: válidos={transformacion['válidos']} | "
            f"duplicados={transformacion['duplicados']} | insertados={insercion['insertados']} | "
            f"errores={insercion['errores']} | final={stats['final']}"
        )

    total_insertados = sum(
        stats["insercion"]["insertados"]
        for tabla, stats in reporte.items()
        if tabla != "staging_limpieza"
        and isinstance(stats, dict)
        and "insercion" in stats
    )
    total_errores = sum(
        stats["insercion"]["errores"]
        for tabla, stats in reporte.items()
        if tabla != "staging_limpieza"
        and isinstance(stats, dict)
        and "insercion" in stats
    )

    print("\nResumen general:")
    print(f"  Total insertados en DWH: {total_insertados}")
    print(f"  Total errores de inserción: {total_errores}")

    if total_errores == 0:
        print("\n[OK] TRANSFORMACIÓN DIMENSIONAL FINALIZADA")
        LoggerManager.info("Transformación dimensional completada exitosamente")
    else:
        print("\n[WARN] Transformación completada con errores de inserción")
        LoggerManager.warning("Transformación dimensional completada con errores")

    print(f"\nLog guardado en: {LoggerManager.obtener_ruta_logs()}")


if __name__ == "__main__":
    ejecutar_transformacion()
