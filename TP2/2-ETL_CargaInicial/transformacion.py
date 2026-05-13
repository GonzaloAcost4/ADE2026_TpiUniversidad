#!/usr/bin/env python
# coding: utf-8

"""
ETL Transformación - De STG_Universidad a dw_universidad

Este script transforma los datos crudos de staging hacia el modelo dimensional
real del Data Warehouse definido en CreacionDWH_Universidad.sql:

Dimensiones:
- dim_tiempo
- dim_estudiante
- dim_dictado

Hechos:
- fact_inscripcion
- fact_examen_estudiante
- fact_evaluacion_dictado

La transformación NO carga tablas operacionales intermedias como Facultad,
Departamento, Programa, Curso, Docente o Estudiante, porque esas tablas no
existen en el esquema dimensional del DWH. Sus datos se integran/denormalizan
dentro de las dimensiones estudiante y Dictado.
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

LoggerManager.reiniciar()
logger = LoggerManager.configurar(
    "transformacion",
    ruta_raiz=str(SCRIPT_DIR),
    carpeta_logs="logs",
)

logger.info(f"[OK] Conexiones configuradas | STG={STG_DATABASE} | DWH={DWH_DATABASE}")

# ============================================
# CONSTANTES DEL DWH (nuevos nombres según CreacionDWH_Universidad.sql)
# ============================================

TABLAS_DWH = [
    "dim_tiempo",
    "dim_dictado",
    "dim_estudiante",
    "fact_inscripcion",
    "fact_examen_estudiante",
    "fact_evaluacion_dictado",
]

# Orden de truncado: hechos primero, luego dimensiones
ORDEN_TRUNCATE = [
    "fact_evaluacion_dictado",
    "fact_examen_estudiante",
    "fact_inscripcion",
    "dim_estudiante",
    "dim_dictado",
    "dim_tiempo",
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
    def limpiar_string(valor: str) -> Optional[str]:
        """Limpia strings: espacios, codificación, minúsculas."""
        if pd.isna(valor) or valor is None:
            return None
        
        # Convertir a string si no lo es
        valor = str(valor).strip().title()

        return valor

    @staticmethod
    def _limpiar_numero_int(texto: str) -> Optional[int]:
        texto = texto.replace('.', '').replace(',', '').replace(' ', '')
        return int(float(texto))

    @staticmethod
    def _limpiar_numero_float(texto: str) -> Optional[float]:
        texto = texto.replace(' ', '')
        if ',' in texto and '.' in texto:
            if texto.rfind(',') > texto.rfind('.'):
                texto = texto.replace('.', '').replace(',', '.')
            else:
                texto = texto.replace(',', '')
        elif ',' in texto:
            texto = texto.replace(',', '.')
        return float(texto)

    @staticmethod
    def limpiar_numero(valor, tipo='float', requerido=False) -> Optional[int | float]:
        """Convierte y valida números."""
        if pd.isna(valor) or valor is None:
            if requerido:
                logger.warning("Valor requerido faltante")
            return None
        
        valor_str = str(valor).strip()
        
        if valor_str.lower() in ['', 'null', 'none', 'n/a']:
            return None
        
        try:
            if tipo == 'int':
                return DataCleaner._limpiar_numero_int(valor_str)
            if tipo == 'float':
                return DataCleaner._limpiar_numero_float(valor_str)
            return None
        except Exception:
            logger.warning(f"No se pudo convertir '{valor}' a {tipo}")
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
        if texto in { "M","MASCULINO", "MALE", "HOMBRE", "1"}:
            return "M"
        if texto in {"F", "FEMENINO", "FEMALE", "MUJER", "2"}:
            return "F"

        LoggerManager.warning(f"Género desconocido: '{valor}'")
        return None

    @staticmethod
    def limpiar_dni(dni) -> bool:
        """Valida DNI argentino en rango de 7 a 8 dígitos."""
        if pd.isna(dni) or dni is None:
            return False
        try:
            dni_int = int(dni)
            return 1_000_000 <= dni_int <= 99_999_999
        except Exception:
            return False
    
    @staticmethod
    def limpiar_nacionalidad(valor: str) -> str:
        """Normaliza valores de nacionalidad."""
        if pd.isna(valor) or valor is None:
            return 'No especificado'
        return valor
# ============================================
# UTILIDADES GENERALES
# ============================================

# Ojito con este módulo, es el corazón de la transformación y se usa en múltiples pasos para estadísticas, remapeos y detección de duplicados.
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


# ??? sacamos capaz
def fecha_desde_anio(anio) -> Optional[date]:
    anio_limpio = DataCleaner.limpiar_numero(anio, "int")
    if anio_limpio is None:
        return None
    if 1900 <= anio_limpio <= date.today().year + 10:
        return date(int(anio_limpio), 1, 1)
    LoggerManager.warning(f"Año fuera de rango lógico: {anio}")
    return None


# fecha_ingreso es solo año es decir, anio_ingreso voy a tener q adaptar esta funcion 

def calcular_edad_ingreso(
    fechas_nacimiento: pd.Series, anios_ingreso: pd.Series
) -> pd.Series:
    """Calcula edad de ingreso como diferencia simple de años."""
    nacimiento = pd.to_datetime(fechas_nacimiento, errors="coerce")
    edad = (anios_ingreso - nacimiento.dt.year).astype("Int64")
    edad = edad.where(nacimiento.notna() & anios_ingreso.notna())
    return edad.where((edad >= 0) & (edad <= 120))


def tiempo_skey(fecha: Optional[date]) -> Optional[int]:
    if not fecha:
        return None
    return fecha.year * 10000 + fecha.month * 100 + fecha.day


# Sacar a la mierda ya que C1 y C2 lo recibimos en periodo 
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


# --------------------------------------------
# Detección y persistencia de duplicados
# --------------------------------------------


def detectar_duplicados(
    df: pd.DataFrame,
    columnas_agrupacion: List[str],
    columna_id: str,
    etiqueta: str = "",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detecta registros duplicados agrupando por `columnas_agrupacion`.

    Para cada grupo, el primer registro (por orden estable) se considera
    canónico (id_tomado) y los demás se marcan como duplicados (id_repetido).

    Retorna:
      - df_limpio: DataFrame sin los registros duplicados.
      - df_mapeo: DataFrame con columnas (id_repetido, id_tomado)
        para trazabilidad y remapeos posteriores.
    """
    columnas_requeridas = set(columnas_agrupacion + [columna_id])
    if df.empty or not columnas_requeridas.issubset(df.columns):
        LoggerManager.info(f"Detectar duplicados ({etiqueta}): entrada vacía o columnas faltantes")
        return df.copy(), pd.DataFrame(columns=["id_repetido", "id_tomado"])

    trabajo = df.sort_values(
        columnas_agrupacion + [columna_id], kind="stable"
    ).copy()
    trabajo["_id_tomado"] = trabajo.groupby(columnas_agrupacion)[columna_id].transform("first")

    mascara_dup = trabajo[columna_id] != trabajo["_id_tomado"]

    mapeo = trabajo.loc[mascara_dup, [columna_id, "_id_tomado"]].rename(
        columns={columna_id: "id_repetido", "_id_tomado": "id_tomado"}
    )
    limpio = trabajo[~mascara_dup].drop(columns=["_id_tomado"])

    LoggerManager.info(
        f"Detectar duplicados ({etiqueta}): "
        f"analizados={len(df)} | duplicados={len(mapeo)} | limpios={len(limpio)}"
    )
    return limpio, mapeo


def persistir_mapeo_duplicados(
    mapeo: pd.DataFrame,
    tabla_destino: str,
) -> Dict[int, int]:
    """
    Persiste el mapeo de duplicados en la tabla de staging indicada
    y retorna el diccionario {id_repetido: id_tomado} directamente,
    sin necesidad de una lectura posterior a la BD.
    """
    with engine_stg.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {tabla_destino}"))

    if mapeo.empty:
        LoggerManager.info(f"{tabla_destino}: sin duplicados para persistir")
        return {}

    carga = mapeo[["id_repetido", "id_tomado"]].copy()
    carga["id_repetido"] = carga["id_repetido"].astype(int)
    carga["id_tomado"] = carga["id_tomado"].astype(int)
    carga.to_sql(name=tabla_destino, con=engine_stg, if_exists="append", index=False)
    LoggerManager.info(f"{tabla_destino}: {len(carga)} mapeos persistidos")

    return dict(zip(
        carga["id_repetido"],
        carga["id_tomado"],
    ))


def leer_mapeo_duplicados(tabla: str) -> Dict[int, int]:
    """
    Lee un mapeo de duplicados desde la tabla de staging indicada.

    Uso principal: carga_incremental.py, donde el mapeo se construye
    vía SQL en actualizar_mapeos_duplicados() y se lee después.
    """
    try:
        df = pd.read_sql(
            f"SELECT id_repetido, id_tomado FROM {tabla} "
            "WHERE id_repetido IS NOT NULL AND id_tomado IS NOT NULL",
            con=engine_stg,
        )
    except Exception as exc:
        LoggerManager.warning(f"No se pudo leer {tabla}: {exc}")
        return {}

    if df.empty:
        LoggerManager.info(f"No hay mapeos de duplicados en {tabla}")
        return {}

    return dict(zip(
        df["id_repetido"].apply(lambda x: DataCleaner.limpiar_numero(x, "int")),
        df["id_tomado"].apply(lambda x: DataCleaner.limpiar_numero(x, "int")),
    ))


def remapear_ids(
    df: pd.DataFrame,
    mapa_ids: Dict[int, int],
    columna_objetivo: str,
    etiqueta: Optional[str] = None,
    tamanio_lote: int = 100_000,
) -> Tuple[pd.DataFrame, int]:
    """
    Reemplaza valores en `columna_objetivo` según `mapa_ids`.

    Comentario de solución: generaliza el remapeo para `id_estudiante` o
    `id_inscripcion` según el mapa construido.
    """
    if df.empty or not mapa_ids or columna_objetivo not in df.columns:
        LoggerManager.info(f"Remapeo ids: nada que hacer para columna '{columna_objetivo}'")
        return df.copy(), 0

    resultado = df.copy()
    total = len(resultado)
    claves_mapa = set(mapa_ids.keys())
    afectados = int(resultado[columna_objetivo].isin(claves_mapa).sum())
    nombre = etiqueta or columna_objetivo

    LoggerManager.info(f"Remapeo ids: columna='{columna_objetivo}' | registros={total} | afectados detectados={afectados}")

    if afectados > 0:
        remapeados = 0
        total_lotes = (total + tamanio_lote - 1) // tamanio_lote
        for i in range(total_lotes):
            inicio = i * tamanio_lote
            fin = min(inicio + tamanio_lote, total)
            tramo = resultado.iloc[inicio:fin][columna_objetivo]
            mascara = tramo.isin(claves_mapa)
            if mascara.any():
                idx_objetivo = tramo[mascara].index
                resultado.loc[idx_objetivo, columna_objetivo] = resultado.loc[
                    idx_objetivo, columna_objetivo
                ].replace(mapa_ids)
                remapeados += int(mascara.sum())

            LoggerManager.info(f"Remapeo ids: columna='{columna_objetivo}' | lote {i + 1}/{total_lotes} | remapeados acumulados={remapeados}")

        afectados = remapeados
        LoggerManager.info(f"Remapeo ids: columna='{columna_objetivo}' | remapeados={afectados}")

    LoggerManager.info(f"Remapeo ids: columna='{columna_objetivo}' | finalizado | remapeados={afectados}")
    return resultado, afectados



def consolidar_examenes_duplicados(
    examenes: pd.DataFrame,
    inscripciones: pd.DataFrame,
    mapa_inscripciones_duplicadas: Dict[int, int],
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Consolida SOLO exámenes asociados a inscripciones duplicadas ya remapeadas
    (derivadas de estudiantes duplicados).

    Reglas aplicadas únicamente en casos impactados:
    - Reordenar cronológicamente por alumno y dictado.
    - Renumerar `numero_intento` desde 1.
    - Truncar a máximo 3 intentos por (id_estudiante, id_dictado).
    """
    if examenes.empty:
        LoggerManager.info("Consolidación exámenes: sin exámenes para procesar")
        return examenes.copy(), {"afectados": 0, "grupos": 0, "eliminados": 0}

    if inscripciones.empty or not mapa_inscripciones_duplicadas:
        LoggerManager.info("Consolidación exámenes: sin mapa de inscripciones duplicadas, no se consolidan exámenes")
        return examenes.copy(), {"afectados": 0, "grupos": 0, "eliminados": 0}

    ins_map = inscripciones[["id_inscripcion", "id_estudiante", "id_dictado"]].drop_duplicates()
    df = examenes.merge(ins_map, on="id_inscripcion", how="left")

    ids_impactados = set(mapa_inscripciones_duplicadas.keys()) | set(
        mapa_inscripciones_duplicadas.values()
    )
    mascara_impactados = (
        df["id_inscripcion"].isin(ids_impactados)
        & df["id_estudiante"].notna()
        & df["id_dictado"].notna()
    )

    afectados = df[mascara_impactados].copy()
    no_afectados = df[~mascara_impactados].copy()

    if afectados.empty:
        LoggerManager.info("Consolidación exámenes: no hay exámenes vinculados a inscripciones duplicadas")
        return examenes.copy(), {"afectados": 0, "grupos": 0, "eliminados": 0}

    grupos = list(afectados.groupby(["id_estudiante", "id_dictado"], sort=False))
    total_grupos = len(grupos)
    LoggerManager.info(f"Consolidación exámenes: grupos impactados={total_grupos} | exámenes impactados={len(afectados)}")

    piezas = []
    eliminados = 0
    for i, (_, g) in enumerate(grupos, start=1):
        g = g.sort_values(["fecha", "id_examen"]).copy()
        original = len(g)
        aprobado_mask = g["resultado"].fillna("").str.lower().eq("aprobado")

        if aprobado_mask.any():
            primer_aprobado = int(aprobado_mask.to_numpy().argmax())
            g = g.iloc[: primer_aprobado + 1].copy()
        else:
            g = g.iloc[:3].copy()

        g["numero_intento"] = list(range(1, len(g) + 1))
        eliminados += max(original - len(g), 0)
        piezas.append(g)

        if i % 5000 == 0 or i == total_grupos:
            LoggerManager.info(f"Consolidación exámenes: progreso grupos {i}/{total_grupos} | eliminados acumulados={eliminados}")

    consolidados = pd.concat(piezas, ignore_index=True, sort=False) if piezas else afectados.iloc[0:0].copy()
    final = pd.concat([consolidados, no_afectados], ignore_index=True, sort=False)

    LoggerManager.info(f"Consolidación exámenes: finalizado | afectados={len(afectados)} | eliminados={eliminados}")
    
    return final[examenes.columns], {
        "afectados": int(len(afectados)),
        "grupos": int(total_grupos),
        "eliminados": int(eliminados),
    }

# ============================================
# FUNCIÓN GENÉRICA DE TRANSFORMACIÓN
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
    df["nacionalidad"] = df["nacionalidad_raw"].apply(cleaner.limpiar_nacionalidad)
    df["id_programa"] = df["id_programa_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["anio_ingreso"] = df["anio_ingreso_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )

    valido = (
        df["id_estudiante"].notna()
        & df["dni"].apply(cleaner.limpiar_dni)
        & df["apellido"].notna()
        & df["nombre"].notna()
        & df["id_programa"].notna()
    )

    validos = df[valido].copy()
    registrar_rechazos("stg_estudiante", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_estudiante"], keep="first")
    # La deduplicación por DNI se realiza en el orquestador (paso 2.1)
    # con detectar_duplicados() para capturar el mapeo {id_repetido: id_tomado}.

    columnas = [
        "id_estudiante",
        "dni",
        "apellido",
        "nombre",
        "genero",
        "fecha_nacimiento",
        "nacionalidad",
        "id_programa",
        "anio_ingreso",
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
    df["periodo"] = df["periodo_raw"].apply(cleaner.limpiar_string)
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
    df["estado"] = df["estado_raw"].apply(cleaner.limpiar_string)

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
    df["resultado"] = df["resultado_raw"].apply(cleaner.limpiar_string)

    df.loc[df["nota"].notna() & ((df["nota"] < 0) | (df["nota"] > 10)), "nota"] = None

    valido = (
        df["id_examen"].notna()
        & df["id_inscripcion"].notna()
        & df["fecha"].notna()
        & df["nota"].notna()
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
        "resultado",
    ]

    return validos[columnas], estadisticas(total, len(validos), duplicados)


def transformar_evaluacion_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Transforma evaluaciones anónimas. NO requiere id_estudiante porque la evaluación
    es completamente anónima. Solo necesita id_dictado, fecha y puntajes.
    """
    cleaner = DataCleaner()
    total = len(df)
    df = df.copy()

    if "fecha_evaluacion_raw" not in df.columns:
        df["fecha_evaluacion_raw"] = None
        LoggerManager.warning(
            "stg_evaluacion_curso no contiene fecha_evaluacion_raw; los registros quedarán inválidos"
        )

    df["id_evaluacion"] = df["id_evaluacion_raw"].apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    df["id_dictado"] = df["id_dictado_raw"].apply(
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

    valido = (
        df["id_evaluacion"].notna()
        & df["id_dictado"].notna()
        & df["fecha_evaluacion"].notna()
    )
    validos = df[valido].copy()
    registrar_rechazos("stg_evaluacion_curso", total, len(validos))
    validos, duplicados = quitar_duplicados(validos, ["id_evaluacion"], keep="first")

    # Columnas de salida: SIN id_estudiante (anónimo)
    columnas = [
        "id_evaluacion",
        "id_dictado",
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
                "ano": fecha.year,
                "periodoAcademico": periodo_academico(fecha),
                "esFeriado": False,
            }
        )

    df_tiempo = pd.DataFrame(registros)
    return df_tiempo, estadisticas(len(fechas_validas), len(df_tiempo), 0)


def construir_dim_estudiante(
    estudiantes: pd.DataFrame, programas: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict]:
    total = len(estudiantes)
    df = estudiantes.merge(programas, on="id_programa", how="left")

    faltan_programas = df["nombre_programa"].isna().sum()
    if faltan_programas > 0:
        LoggerManager.warning(
            f"estudiante: {faltan_programas} estudiantes sin programa encontrado; se cargan con atributos de programa NULL"
        )

    resultado = (
        df.rename(
            columns={
                "id_estudiante": "idalumno",
                "fecha_nacimiento": "fechaNacim",
                "anio_ingreso": "anioIngreso",
                "nombre_programa": "nombrePrograma",
                "tipo_programa": "tipoPrograma",
                "duracion_anios_programa": "duracionAniosPrograma",
            }
        )
        .assign(
            edadIngreso=calcular_edad_ingreso(
                df["fecha_nacimiento"], df["anio_ingreso"]
            ),
            egresoCarrera=False,
            anioEgreso=None,
            abandonoCarrera=False,
            anioAbandono=None,
            anioPlanPrograma=None,
            valid_from=date.today(),
            valid_to=None,
            es_actual=True,
        )
    )

    columnas_finales = [
        "idalumno",
        "dni",
        "nombre",
        "apellido",
        "genero",
        "fechaNacim",
        "nacionalidad",
        "anioIngreso",
        "edadIngreso",
        "egresoCarrera",
        "anioEgreso",
        "abandonoCarrera",
        "anioAbandono",
        "nombrePrograma",
        "tipoPrograma",
        "duracionAniosPrograma",
        "anioPlanPrograma",
        "valid_from",
        "valid_to",
        "es_actual",
    ]

    resultado, duplicados = quitar_duplicados(resultado[columnas_finales], ["idalumno"], keep="first")
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

    resultado = (
        df.rename(
            columns={
                "id_dictado": "idDictado",
                "cupo_maximo": "cupoMax",
                "codigo_curso": "codigoCurso",
                "nombre_curso": "nombreCurso",
                "horas_teo_curso": "horasTeoCurso",
                "horas_prac_curso": "horasPracCurso",
                "horas_lab_curso": "horasLabCurso",
                "nivel_curso": "nivelCurso",
                "nombre_docente": "nombreDocente",
                "apellido_docente": "apellidoDocente",
                "titulo_docente": "tituloDocente",
                "categoria_docente": "categoriaDocente",
                "dedicacion_docente": "dedicacionDocente",
                "nombre_departamento": "nombreDep",
                "nombre_facultad": "nombreFac",
                "ciudad_facultad": "ciudadFac",
                "provincia_facultad": "provFac",
            }
        )
        .assign(
            valid_from=date.today(),
            valid_to=None,
            es_actual=True,
        )
    )

    columnas_finales = [
        "idDictado",
        "periodo",
        "turno",
        "aula",
        "cupoMax",
        "codigoCurso",
        "nombreCurso",
        "horasTeoCurso",
        "horasPracCurso",
        "horasLabCurso",
        "nivelCurso",
        "nombreDocente",
        "apellidoDocente",
        "tituloDocente",
        "categoriaDocente",
        "dedicacionDocente",
        "nombreDep",
        "nombreFac",
        "ciudadFac",
        "provFac",
        "valid_from",
        "valid_to",
        "es_actual",
    ]

    resultado, duplicados = quitar_duplicados(resultado[columnas_finales], ["idDictado"], keep="first")
    return resultado, estadisticas(total, len(resultado), duplicados)


# ============================================
# MAPEO DE SURROGATE KEYS
# ============================================


def obtener_mapa_estudiante() -> Dict[int, int]:
    df = pd.read_sql(
        "SELECT alumnoSKey, idalumno FROM dim_estudiante WHERE es_actual = TRUE",
        con=engine_dwh,
    )
    return dict(zip(df["idalumno"], df["alumnoSKey"]))


def obtener_mapa_dictado() -> Dict[int, int]:
    df = pd.read_sql(
        "SELECT dictadoSKey, idDictado FROM dim_dictado WHERE es_actual = TRUE",
        con=engine_dwh,
    )
    return dict(zip(df["idDictado"], df["dictadoSKey"]))


def obtener_mapa_tiempo() -> Dict[date, int]:
    df = pd.read_sql("SELECT tiempoSKey, fecha FROM dim_tiempo", con=engine_dwh)
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
    return dict(zip(df["fecha"], df["tiempoSKey"]))


# ============================================
# CONSTRUCCIÓN DE HECHOS DEL DWH
# ============================================


def construir_fact_inscripcion(
    inscripciones: pd.DataFrame,
    mapa_estudiante: Dict[int, int],
    mapa_dictado: Dict[int, int],
    mapa_tiempo: Dict[date, int],
) -> Tuple[pd.DataFrame, Dict]:
    total = len(inscripciones)
    df = inscripciones.copy()

    df["alumnoSKey"] = df["id_estudiante"].map(mapa_estudiante)
    df["dictadoSKey"] = df["id_dictado"].map(mapa_dictado)
    df["tiempoSKey"] = df["fecha_inscripcion"].map(mapa_tiempo)
    df["abandono"] = (
        df["estado"]
        .fillna("")
        .str.lower()
        .isin(["abandonada", "abandonado", "abandono", "baja"])
    )

    valido = (
        df["alumnoSKey"].notna()
        & df["dictadoSKey"].notna()
        & df["tiempoSKey"].notna()
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


def construir_fact_examen_estudiante(
    examenes: pd.DataFrame,
    inscripciones: pd.DataFrame,
    mapa_estudiante: Dict[int, int],
    mapa_dictado: Dict[int, int],
    mapa_tiempo: Dict[date, int],
) -> Tuple[pd.DataFrame, Dict]:
    total = len(examenes)

    df = examenes.merge(
        inscripciones[["id_inscripcion", "id_estudiante", "id_dictado"]],
        on="id_inscripcion",
        how="left",
    )

    df["alumnoSKey"] = df["id_estudiante"].map(mapa_estudiante)
    df["dictadoSKey"] = df["id_dictado"].map(mapa_dictado)
    df["tiempoSKey"] = df["fecha"].map(mapa_tiempo)

    valido = (
        df["alumnoSKey"].notna()
        & df["dictadoSKey"].notna()
        & df["tiempoSKey"].notna()
    )
    validos = df[valido].copy()
    registrar_rechazos(
        "Fact Examenestudiante por claves no encontradas", total, len(validos)
    )

    validos = validos.sort_values(["fecha", "id_examen"])
    # Crear columna aprobado: TRUE si resultado es "Aprobado", FALSE si es "Desaprobado"
    validos["aprobado"] = validos["resultado"].fillna("").str.lower() == "aprobado"
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

    # Ajustar nombres para el fact schema (nombres: estudiante_skey, tiempo_skey, dictado_skey, nota, n_intentos, aprobado)
    return resultado, estadisticas(total, len(resultado), duplicados)


def construir_fact_evaluacion_dictado(
    evaluaciones: pd.DataFrame,
    mapa_dictado: Dict[int, int],
    mapa_tiempo: Dict[date, int],
) -> Tuple[pd.DataFrame, Dict]:
    """
    Construye la tabla de hecho EvaluacionDictado usando las claves naturales
    limpias de staging. Como la evaluación es anónima, NO mapea estudiantes.
    Solo mapea dictado y tiempo.
    """
    total = len(evaluaciones)
    df = evaluaciones.copy()

    df["dictadoSKey"] = df["id_dictado"].map(mapa_dictado)
    df["tiempoSKey"] = df["fecha_evaluacion"].map(mapa_tiempo)

    valido = (
        df["dictadoSKey"].notna()
        & df["tiempoSKey"].notna()
    )
    validos = df[valido].copy()
    registrar_rechazos(
        "Fact EvaluacionDictado por claves no encontradas", total, len(validos)
    )

    validos = validos.sort_values(["fecha_evaluacion", "id_evaluacion"])
    resultado = validos[
        [
            "dictadoSKey",
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

    # NO aplicamos quitar_duplicados por dictadoSKey y tiempoSKey porque 
    # multiples alumnos pueden evaluar el mismo dictado el mismo día de forma anónima.
    # La deduplicación por id_evaluacion ya se hizo en transformar_evaluacion_base.
    return resultado, estadisticas(total, len(resultado), 0)


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

    LoggerManager.info(f"Iniciando inserción en {tabla_destino}: {len(df)} registros en {num_lotes} lotes")

    try:
        with engine_dwh.begin() as conn:
            for i in range(num_lotes):
                inicio = i * tamanio_lote
                fin = min(inicio + tamanio_lote, len(df))
                lote = df.iloc[inicio:fin].copy()

                try:
                    lote.to_sql(
                        name=tabla_destino,
                        con=conn,
                        if_exists="append",
                        index=False,
                    )
                    resultados["insertados"] += len(lote)
                    resultados["lotes"] += 1
                except Exception as exc:
                    resultados["errores"] += len(lote)
                    LoggerManager.error(
                        f"Error insertando lote {i + 1} en {tabla_destino}: {exc}"
                    )
                    raise
    except Exception as general_exc:
        raise general_exc

    LoggerManager.info(f"Inserción en {tabla_destino} finalizada. {resultados['insertados']} registros insertados.")
    return resultados


def cargar_tabla(
    nombre_tabla: str, df: pd.DataFrame, reporte: Dict, stats_transformacion: Dict
) -> None:
    # Verificar si la tabla destino está vacía antes de insertar
    with engine_dwh.connect() as conn:
        try:
            count = int(
                conn.execute(text(f"SELECT COUNT(*) FROM {nombre_tabla}")).scalar()
            )
        except Exception as e:
            LoggerManager.error(f"Error consultando tabla {nombre_tabla}: {e}")
            raise

    if count > 0:
        LoggerManager.warning(
            f"Tabla destino {nombre_tabla} tiene {count} registros; se ejecutará TRUNCATE previo a la carga"
        )
        with engine_dwh.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
            conn.execute(text(f"TRUNCATE TABLE {nombre_tabla}"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        LoggerManager.info(f"TRUNCATE TABLE {nombre_tabla} ejecutado antes de insertar")

    stats_insert = insertar_dataframe(df, nombre_tabla)
    final = contar_tabla_dwh(nombre_tabla)

    reporte[nombre_tabla] = {
        "transformacion": stats_transformacion,
        "insercion": stats_insert,
        "final": final,
    }

    LoggerManager.info(f"  {nombre_tabla}: transformados={stats_transformacion['válidos']} | insertados={stats_insert['insertados']} | errores={stats_insert['errores']} | final={final}")


# ============================================
# ORQUESTACIÓN
# ============================================


def ejecutar_transformacion() -> Dict:
    LoggerManager.info("Transformación dimensional STG -> DWH")

    reporte: Dict = {}

    # 1. Lectura y limpieza base.
    LoggerManager.info("Limpieza y validación de staging")
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
    datos_raw: Dict[str, pd.DataFrame] = {}
    stats_base: Dict[str, Dict] = {}

    for clave, (tabla_stg, funcion) in staging_transformaciones.items():
        LoggerManager.info(f"Procesando {tabla_stg}")
        df_raw = leer_tabla_staging(tabla_stg)
        datos_raw[clave] = df_raw
        df_limpio, stats = funcion(df_raw)
        datos[clave] = df_limpio
        stats_base[tabla_stg] = stats
        if stats["rechazados"] > 0 or stats["duplicados"] > 0:
            LoggerManager.warning(f"Atención: rechazados={stats['rechazados']} | duplicados={stats['duplicados']}")

    reporte["staging_limpieza"] = stats_base

    # 2. Cascada de duplicados: estudiantes → inscripciones → exámenes
    #    - Detectar estudiantes duplicados por DNI y capturar el mapeo.
    #    - Remapear id_estudiante en inscripciones hacia el canónico.
    #    - Detectar inscripciones duplicadas (mismo estudiante+dictado) SOLO
    #      para estudiantes que eran duplicados.
    #    - Remapear id_inscripcion en exámenes hacia la canónica.
    #    - Consolidar intentos de examen impactados.
    LoggerManager.info("Detección y remapeo de duplicados (estudiantes / inscripciones / examenes)")

    # 2.1 Detectar estudiantes duplicados por DNI y deduplicar
    LoggerManager.info("Detectando estudiantes duplicados por DNI")
    datos["estudiantes"], mapeo_est = detectar_duplicados(
        datos["estudiantes"], ["dni"], "id_estudiante",
        etiqueta="estudiantes por DNI",
    )
    mapa_est_dup = persistir_mapeo_duplicados(mapeo_est, "stg_estudiantes_repetidos")

    # 2.2 Remapear id_estudiante en inscripciones
    LoggerManager.info("Remapeando inscripciones por equivalencias de estudiantes")
    datos["inscripciones"], cnt_ins_remapeadas = remapear_ids(
        datos.get("inscripciones", pd.DataFrame()),
        mapa_est_dup,
        "id_estudiante",
        etiqueta="inscripciones.id_estudiante",
    )

    # 2.3 Detectar inscripciones duplicadas SOLO de estudiantes duplicados
    LoggerManager.info("Detectando inscripciones duplicadas de estudiantes duplicados")
    mapa_ins_dup: Dict[int, int] = {}
    cnt_ins_dup = 0
    if mapa_est_dup:
        ids_est_canonicos = set(mapa_est_dup.values())
        mask_afectadas = datos["inscripciones"]["id_estudiante"].isin(ids_est_canonicos)
        ins_afectadas = datos["inscripciones"][mask_afectadas].copy()
        ins_no_afectadas = datos["inscripciones"][~mask_afectadas].copy()

        ins_limpias, mapeo_ins = detectar_duplicados(
            ins_afectadas, ["id_estudiante", "id_dictado"], "id_inscripcion",
            etiqueta="inscripciones de estudiantes duplicados",
        )
        mapa_ins_dup = persistir_mapeo_duplicados(mapeo_ins, "stg_inscripciones_repetidas")
        cnt_ins_dup = len(mapeo_ins)
        datos["inscripciones"] = pd.concat(
            [ins_limpias, ins_no_afectadas], ignore_index=True
        )
    else:
        # Sin estudiantes duplicados → limpiar tabla de trazabilidad
        with engine_stg.begin() as conn:
            conn.execute(text("TRUNCATE TABLE stg_inscripciones_repetidas"))

    # 2.4 Remapear id_inscripcion en exámenes
    LoggerManager.info("Remapeando exámenes por equivalencias de inscripciones")
    datos["examenes"], cnt_exam_remapeadas = remapear_ids(
        datos.get("examenes", pd.DataFrame()),
        mapa_ins_dup,
        "id_inscripcion",
        etiqueta="examenes.id_inscripcion",
    )

    # 2.5 Consolidar intentos de examen impactados por duplicados
    LoggerManager.info("Consolidando intentos en casos impactados por duplicados")
    datos["examenes"], stats_consolidacion_examen = consolidar_examenes_duplicados(
        datos.get("examenes", pd.DataFrame()),
        datos.get("inscripciones", pd.DataFrame()),
        mapa_ins_dup,
    )

    reporte["duplicados"] = {
        "estudiantes_duplicados": len(mapeo_est),
        "inscripciones_duplicadas": cnt_ins_dup,
        "inscripciones_remapeadas": int(cnt_ins_remapeadas),
        "examenes_remapeados": int(cnt_exam_remapeadas),
        "examenes_consolidados": stats_consolidacion_examen,
    }

    LoggerManager.info(f"Duplicados: estudiantes={len(mapeo_est)} | inscripciones={cnt_ins_dup} | ins_remapeadas={cnt_ins_remapeadas} | exam_remapeados={cnt_exam_remapeadas} | exam_afectados={stats_consolidacion_examen.get('afectados',0)} | exam_eliminados={stats_consolidacion_examen.get('eliminados',0)}")

    # 3. Construcción dimensional.
    LoggerManager.info("Construcción de dimensiones")
    fechas_tiempo = []
    if not datos["inscripciones"].empty:
        fechas_tiempo.extend(datos["inscripciones"]["fecha_inscripcion"].tolist())
    if not datos["examenes"].empty:
        fechas_tiempo.extend(datos["examenes"]["fecha"].tolist())
    if not datos["evaluaciones"].empty:
        fechas_tiempo.extend(datos["evaluaciones"]["fecha_evaluacion"].tolist())

    dim_tiempo, stats_tiempo = construir_dim_tiempo(fechas_tiempo)
    dim_estudiante, stats_estudiante = construir_dim_estudiante(
        datos["estudiantes"], datos["programas"]
    )
    dim_dictado, stats_dictado = construir_dim_dictado(
        datos["dictados"],
        datos["cursos"],
        datos["docentes"],
        datos["departamentos"],
        datos["facultades"],
    )

    LoggerManager.info(f"Dimensiones listas: Tiempo={len(dim_tiempo)} | estudiante={len(dim_estudiante)} | Dictado={len(dim_dictado)}")

    # 3. Truncate único de todo el DWH antes de cargar.
    LoggerManager.info("Reinicio controlado de tablas DWH")
    truncar_dwh()

    # 4. Carga de dimensiones.
    LoggerManager.info("Carga de dimensiones y hechos")
    cargar_tabla("dim_tiempo", dim_tiempo, reporte, stats_tiempo)
    cargar_tabla("dim_estudiante", dim_estudiante, reporte, stats_estudiante)
    cargar_tabla("dim_dictado", dim_dictado, reporte, stats_dictado)

    # 5. Obtención de surrogate keys generadas.
    mapa_estudiante = obtener_mapa_estudiante()
    mapa_dictado = obtener_mapa_dictado()
    mapa_tiempo = obtener_mapa_tiempo()

    # 6. Construcción de hechos.
    fact_inscripcion, stats_fact_inscripcion = construir_fact_inscripcion(
        datos["inscripciones"], mapa_estudiante, mapa_dictado, mapa_tiempo
    )
    fact_examen, stats_fact_examen = construir_fact_examen_estudiante(
        datos["examenes"],
        datos["inscripciones"],
        mapa_estudiante,
        mapa_dictado,
        mapa_tiempo,
    )
    fact_evaluacion, stats_fact_evaluacion = construir_fact_evaluacion_dictado(
        datos["evaluaciones"], mapa_dictado, mapa_tiempo
    )

    # 7. Carga de hechos.
    cargar_tabla("fact_inscripcion", fact_inscripcion, reporte, stats_fact_inscripcion)
    cargar_tabla("fact_examen_estudiante", fact_examen, reporte, stats_fact_examen)
    cargar_tabla(
        "fact_evaluacion_dictado", fact_evaluacion, reporte, stats_fact_evaluacion
    )

    # 8. Reporte final.
    imprimir_reporte(reporte)
    return reporte


def imprimir_reporte(reporte: Dict) -> None:
    LoggerManager.info("Reporte final")

    problemas_staging = [
        (tabla, stats)
        for tabla, stats in reporte.get("staging_limpieza", {}).items()
        if stats["rechazados"] > 0 or stats["duplicados"] > 0
    ]
    if problemas_staging:
        LoggerManager.warning("Staging con registros rechazados o duplicados")
        for tabla, stats in problemas_staging:
            LoggerManager.warning(f"  {tabla}: total={stats['total']} | rechazados={stats['rechazados']} | duplicados={stats['duplicados']}")

    LoggerManager.info("Carga DWH")
    for tabla in TABLAS_DWH:
        stats = reporte.get(tabla)
        if not stats:
            continue
        transformacion = stats["transformacion"]
        insercion = stats["insercion"]
        LoggerManager.info(f"  {tabla}: válidos={transformacion['válidos']} | duplicados={transformacion['duplicados']} | insertados={insercion['insertados']} | errores={insercion['errores']} | final={stats['final']}")

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

    LoggerManager.info(f"Resumen general: Total insertados en DWH: {total_insertados} | Total errores de inserción en DWH: {total_errores}")

    if total_errores == 0:
        LoggerManager.info("Transformación dimensional completada exitosamente")
    else:
        LoggerManager.warning("Transformación dimensional completada con errores")
        
    LoggerManager.info(f"Log guardado en: {LoggerManager.obtener_ruta_logs()}")

    # Registrar marca de agua para el proceso incremental.
    # Sin esto, el incremental vería TODOS los registros de staging como delta
    # porque no tiene referencia de hasta cuándo ya se procesó.
    try:
        from datetime import datetime
        ahora = datetime.now().isoformat()
        with engine_stg.begin() as conn:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS etl_auditoria_incremental ("
                "  id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                "  inicio DATETIME NOT NULL,"
                "  fin DATETIME NULL,"
                "  ultima_extraccion DATETIME NULL,"
                "  nueva_extraccion DATETIME NULL,"
                "  estado VARCHAR(20) NOT NULL,"
                "  registros_delta INT DEFAULT 0,"
                "  mensaje_error TEXT NULL"
                ") ENGINE=InnoDB"
            ))
            conn.execute(text(
                "INSERT INTO etl_auditoria_incremental "
                "(inicio, fin, nueva_extraccion, estado, registros_delta) "
                "VALUES (:inicio, :fin, :nueva, 'OK', 0)"
            ), {"inicio": ahora, "fin": ahora, "nueva": ahora})
        LoggerManager.info(
            f"Marca de agua incremental registrada: {ahora} "
            "(el próximo incremental procesará solo registros posteriores a este momento)"
        )
    except Exception as exc:
        LoggerManager.warning(f"No se pudo registrar marca de agua incremental: {exc}")


if __name__ == "__main__":
    ejecutar_transformacion()