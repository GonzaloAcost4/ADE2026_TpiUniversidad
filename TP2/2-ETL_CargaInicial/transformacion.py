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
from typing import Dict, Iterable, List, Optional, Set, Tuple

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
from schemas import obtener_esquema, obtener_columnas_requeridas, obtener_claves_deduplicacion

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
    def limpiar_string(valor: str) -> str:
        """Limpia strings: espacios, codificación, minúsculas."""
        if pd.isna(valor) or valor is None:
            return None
        
        # Convertir a string si no lo es
        valor = str(valor).strip().title()

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
            if tipo == 'int':
                # Para enteros, sí eliminamos separadores de miles.
                valor_str = valor_str.replace('.', '').replace(',', '').replace(' ', '')
                return int(float(valor_str))
            elif tipo == 'float':
                # Para decimales, preservamos el separador decimal.
                valor_str = valor_str.replace(' ', '')
                if ',' in valor_str and '.' in valor_str:
                    if valor_str.rfind(',') > valor_str.rfind('.'):
                        valor_str = valor_str.replace('.', '').replace(',', '.')
                    else:
                        valor_str = valor_str.replace(',', '')
                elif ',' in valor_str:
                    valor_str = valor_str.replace(',', '.')
                return float(valor_str)
            else:
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
        if texto in {"M", "MASCULINO", "MALE", "HOMBRE", "1"}:
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


def calcular_edad_vectorizada(
    fechas_nacimiento: pd.Series, fechas_ingreso: pd.Series
) -> pd.Series:
    """Calcula edad de ingreso sin recorrer fila por fila."""
    nacimiento = pd.to_datetime(fechas_nacimiento, errors="coerce")
    ingreso = pd.to_datetime(fechas_ingreso, errors="coerce")

    edad = ingreso.dt.year - nacimiento.dt.year
    ajuste = (
        (ingreso.dt.month < nacimiento.dt.month)
        | (
            (ingreso.dt.month == nacimiento.dt.month)
            & (ingreso.dt.day < nacimiento.dt.day)
        )
    ).astype("Int64")

    edad = (edad - ajuste).astype("Int64")
    edad = edad.where(nacimiento.notna() & ingreso.notna())
    return edad.where((edad >= 0) & (edad <= 120))


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


# --------------------------------------------
# Detección y persistencia de duplicados
# --------------------------------------------

def construir_mapeo_estudiantes_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detecta estudiantes duplicados por DNI y devuelve filas con
    (archivo_origen, id_repetido, id_tomado) donde id_tomado es la id
    canónica (primer registro por orden estable).

    Comentario de solución: permite crear la tabla de trazabilidad
    `stg_estudiantes_repetidos` para luego remapear inscripciones y
    exámenes hacia el id_estudiante canónico.
    """
    if df.empty or "dni" not in df.columns or "id_estudiante" not in df.columns:
        LoggerManager.info("Construcción mapeo estudiantes duplicados: entrada vacía o columnas faltantes")
        return pd.DataFrame(columns=["archivo_origen", "id_repetido", "id_tomado"])

    trabajo = df.copy()
    columnas_orden = [col for col in ["dni", "id_estudiante"] if col in trabajo.columns]
    if columnas_orden:
        trabajo = trabajo.sort_values(columnas_orden, kind="stable")

    trabajo["id_tomado"] = trabajo.groupby("dni")["id_estudiante"].transform("first")
    duplicados = trabajo[trabajo["dni"].notna() & (trabajo["id_estudiante"] != trabajo["id_tomado"])].copy()

    LoggerManager.info(f"Construcción mapeo estudiantes duplicados: registros analizados={len(trabajo)} | duplicados encontrados={len(duplicados)}")

    if duplicados.empty:
        LoggerManager.info("No se encontraron estudiantes duplicados por DNI")
        return pd.DataFrame(columns=["archivo_origen", "id_repetido", "id_tomado"])

    out = duplicados[[col for col in ["archivo_origen", "id_estudiante", "id_tomado"] if col in duplicados.columns]].rename(columns={"id_estudiante": "id_repetido"})
    for col in ["archivo_origen", "id_repetido", "id_tomado"]:
        if col not in out.columns:
            out[col] = None
    return out[["archivo_origen", "id_repetido", "id_tomado"]]


def preparar_estudiantes_para_mapeo_duplicados(df_stg_estudiante: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara un dataset mínimo de estudiantes (id, dni, archivo_origen) desde staging
    sin deduplicar por DNI, para poder detectar equivalencias reales id_repetido -> id_tomado.
    """
    if df_stg_estudiante.empty:
        return pd.DataFrame(columns=["archivo_origen", "id_estudiante", "dni"])

    cleaner = DataCleaner()
    trabajo = pd.DataFrame()
    trabajo["id_estudiante"] = df_stg_estudiante.get("id_estudiante_raw", pd.Series(dtype=object)).apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    trabajo["dni"] = df_stg_estudiante.get("dni_raw", pd.Series(dtype=object)).apply(
        lambda x: cleaner.limpiar_numero(x, "int")
    )
    trabajo["archivo_origen"] = df_stg_estudiante.get("archivo_origen", None)

    trabajo = trabajo[
        trabajo["id_estudiante"].notna() & trabajo["dni"].apply(cleaner.limpiar_dni)
    ].copy()
    return trabajo


def persistir_registros_estudiantes_duplicados(duplicados: pd.DataFrame) -> Dict[str, int]:
    """
    Persiste el mapeo de estudiantes duplicados en `stg_estudiantes_repetidos`.

    Comentario de solución: crea/actualiza la tabla de staging con la
    equivalencia id_repetido -> id_tomado para uso posterior en remapeos.
    """
    columnas = ["archivo_origen", "id_repetido", "id_tomado"]
    carga = duplicados.copy() if duplicados is not None else pd.DataFrame(columns=columnas)

    for columna in columnas:
        if columna not in carga.columns:
            carga[columna] = None

    LoggerManager.info(f"Persistiendo {len(carga)} mapeos de estudiantes duplicados en stg_estudiantes_repetidos")
    with engine_stg.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stg_estudiantes_repetidos"))
        LoggerManager.info("stg_estudiantes_repetidos truncada antes de insertar nuevos registros")

    if carga.empty:
        LoggerManager.info("stg_estudiantes_repetidos vaciada sin registros duplicados para insertar")
        return {"registrados": 0}

    carga[columnas].to_sql(name="stg_estudiantes_repetidos", con=engine_stg, if_exists="append", index=False)
    LoggerManager.info(f"stg_estudiantes_repetidos actualizada con {len(carga)} ids_estudiante duplicados")
    return {"registrados": int(len(carga))}


def obtener_mapa_estudiantes_duplicados() -> Dict[int, int]:
    """Lee `stg_estudiantes_repetidos` y devuelve dict{id_repetido: id_tomado}."""
    try:
        df = pd.read_sql(
            "SELECT id_repetido, id_tomado FROM stg_estudiantes_repetidos WHERE id_repetido IS NOT NULL AND id_tomado IS NOT NULL",
            con=engine_stg,
        )
    except Exception as exc:
        LoggerManager.warning(f"No se pudo leer stg_estudiantes_repetidos para remapeo: {exc}")
        return {}

    if df.empty:
        LoggerManager.info("No hay mapeos de estudiantes duplicados en stg_estudiantes_repetidos")
        return {}

    df["id_repetido"] = df["id_repetido"].apply(lambda x: DataCleaner.limpiar_numero(x, "int"))
    df["id_tomado"] = df["id_tomado"].apply(lambda x: DataCleaner.limpiar_numero(x, "int"))
    df = df.dropna(subset=["id_repetido", "id_tomado"])
    return dict(zip(df["id_repetido"], df["id_tomado"]))


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
        print(
            f"    [REMAPPING] {etiqueta or columna_objetivo}: sin cambios (entrada vacía o sin mapa)",
            flush=True,
        )
        return df.copy(), 0

    resultado = df.copy()
    total = len(resultado)
    claves_mapa = set(mapa_ids.keys())
    afectados = int(resultado[columna_objetivo].isin(claves_mapa).sum())
    nombre = etiqueta or columna_objetivo

    print(
        f"    [REMAPPING] {nombre}: iniciando | filas={total} | candidatos={afectados}",
        flush=True,
    )
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

            print(
                f"    [REMAPPING] {nombre}: lote {i + 1}/{total_lotes} | remapeados acumulados={remapeados}",
                flush=True,
            )

        afectados = remapeados
        LoggerManager.info(f"Remapeo ids: columna='{columna_objetivo}' | remapeados={afectados}")

    print(
        f"    [REMAPPING] {nombre}: finalizado | remapeados={afectados}",
        flush=True,
    )
    return resultado, afectados


def construir_mapeo_inscripciones_duplicadas(
    df: pd.DataFrame, estudiantes_canonicos_duplicados: Optional[Set[int]] = None
) -> pd.DataFrame:
    """
    Detecta inscripciones duplicadas por (id_estudiante, id_dictado) y retorna
    DataFrame (archivo_origen, id_repetido, id_tomado).

    Comentario de solución: identifica inscripciones "fantasma" generadas por
    estudiantes duplicados y elige una inscripción canónica por orden estable.
    """
    if df.empty or not {"id_inscripcion", "id_estudiante", "id_dictado"}.issubset(df.columns):
        LoggerManager.info("Construcción mapeo inscripciones duplicadas: entrada vacía o columnas faltantes")
        return pd.DataFrame(columns=["archivo_origen", "id_repetido", "id_tomado"])

    trabajo = df.copy()
    columnas_orden = [c for c in ["id_estudiante", "id_dictado", "fecha_inscripcion", "id_inscripcion"] if c in trabajo.columns]
    if columnas_orden:
        trabajo = trabajo.sort_values(columnas_orden, kind="stable")

    trabajo["id_tomado"] = trabajo.groupby(["id_estudiante", "id_dictado"])["id_inscripcion"].transform("first")
    duplicados = trabajo[trabajo["id_inscripcion"] != trabajo["id_tomado"]].copy()

    if estudiantes_canonicos_duplicados is not None:
        antes_filtrado = len(duplicados)
        duplicados = duplicados[
            duplicados["id_estudiante"].isin(estudiantes_canonicos_duplicados)
        ].copy()
        LoggerManager.info(
            "Construcción mapeo inscripciones duplicadas (solo estudiantes duplicados): "
            f"antes={antes_filtrado} | después={len(duplicados)}"
        )

    LoggerManager.info(f"Construcción mapeo inscripciones duplicadas: registros analizados={len(trabajo)} | duplicados encontrados={len(duplicados)}")

    if duplicados.empty:
        LoggerManager.info("No se encontraron inscripciones duplicadas")
        return pd.DataFrame(columns=["archivo_origen", "id_repetido", "id_tomado"])

    out = duplicados[[col for col in ["archivo_origen", "id_inscripcion", "id_tomado"] if col in duplicados.columns]].rename(columns={"id_inscripcion": "id_repetido"})
    for col in ["archivo_origen", "id_repetido", "id_tomado"]:
        if col not in out.columns:
            out[col] = None
    return out[["archivo_origen", "id_repetido", "id_tomado"]]


def persistir_registros_inscripciones_duplicadas(duplicados: pd.DataFrame) -> Dict[str, int]:
    columnas = ["archivo_origen", "id_repetido", "id_tomado"]
    carga = duplicados.copy() if duplicados is not None else pd.DataFrame(columns=columnas)
    for columna in columnas:
        if columna not in carga.columns:
            carga[columna] = None

    LoggerManager.info(f"Persistiendo {len(carga)} mapeos de inscripciones duplicadas en stg_inscripciones_repetidas")
    with engine_stg.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stg_inscripciones_repetidas"))
        LoggerManager.info("stg_inscripciones_repetidas truncada antes de insertar nuevos registros")

    if carga.empty:
        LoggerManager.info("stg_inscripciones_repetidas vaciada sin registros duplicados para insertar")
        return {"registrados": 0}

    carga[columnas].to_sql(name="stg_inscripciones_repetidas", con=engine_stg, if_exists="append", index=False)
    LoggerManager.info(f"stg_inscripciones_repetidas actualizada con {len(carga)} ids_inscripcion duplicadas")
    return {"registrados": int(len(carga))}


def obtener_mapa_inscripciones_duplicadas() -> Dict[int, int]:
    try:
        df = pd.read_sql(
            "SELECT id_repetido, id_tomado FROM stg_inscripciones_repetidas WHERE id_repetido IS NOT NULL AND id_tomado IS NOT NULL",
            con=engine_stg,
        )
    except Exception as exc:
        LoggerManager.warning(f"No se pudo leer stg_inscripciones_repetidas para remapeo: {exc}")
        return {}

    if df.empty:
        LoggerManager.info("No hay mapeos de inscripciones duplicadas en stg_inscripciones_repetidas")
        return {}

    df["id_repetido"] = df["id_repetido"].apply(lambda x: DataCleaner.limpiar_numero(x, "int"))
    df["id_tomado"] = df["id_tomado"].apply(lambda x: DataCleaner.limpiar_numero(x, "int"))
    df = df.dropna(subset=["id_repetido", "id_tomado"])
    return dict(zip(df["id_repetido"], df["id_tomado"]))


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
        print("    [CONSOLIDACIÓN] sin exámenes para procesar", flush=True)
        return examenes.copy(), {"afectados": 0, "grupos": 0, "eliminados": 0}

    if inscripciones.empty or not mapa_inscripciones_duplicadas:
        print(
            "    [CONSOLIDACIÓN] sin mapa de inscripciones duplicadas, no se consolidan exámenes",
            flush=True,
        )
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
        print(
            "    [CONSOLIDACIÓN] no hay exámenes vinculados a inscripciones duplicadas",
            flush=True,
        )
        return examenes.copy(), {"afectados": 0, "grupos": 0, "eliminados": 0}

    grupos = list(afectados.groupby(["id_estudiante", "id_dictado"], sort=False))
    total_grupos = len(grupos)
    print(
        f"    [CONSOLIDACIÓN] grupos impactados={total_grupos} | exámenes impactados={len(afectados)}",
        flush=True,
    )

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
            print(
                f"    [CONSOLIDACIÓN] progreso grupos {i}/{total_grupos} | eliminados acumulados={eliminados}",
                flush=True,
            )

    consolidados = pd.concat(piezas, ignore_index=True, sort=False) if piezas else afectados.iloc[0:0].copy()
    final = pd.concat([consolidados, no_afectados], ignore_index=True, sort=False)

    print(
        f"    [CONSOLIDACIÓN] finalizado | afectados={len(afectados)} | eliminados={eliminados}",
        flush=True,
    )
    return final[examenes.columns], {
        "afectados": int(len(afectados)),
        "grupos": int(total_grupos),
        "eliminados": int(eliminados),
    }

# ============================================
# FUNCIÓN GENÉRICA DE TRANSFORMACIÓN
# ============================================

def transformar_entidad_base_generica(
    df: pd.DataFrame,
    esquema_nombre: str,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Función genérica que reemplaza las 10 transformaciones base.
    
    Aplica limpieza, validación y deduplicación según el esquema.
    """
    cleaner = DataCleaner()
    esquema = obtener_esquema(esquema_nombre)
    tabla_nombre = esquema["tabla_nombre"]
    total = len(df)
    resultado = df.copy()
    
    # Construir mapeo de raw -> limpio con soporte para mapeos personalizados
    mapeo_personalizado = esquema.get("mapeo_columnas", {})
    
    def obtener_col_limpia(col_raw):
        # Primero check mapeo personalizado
        if col_raw in mapeo_personalizado:
            return mapeo_personalizado[col_raw]
        # Si no, usar convención estándar
        return col_raw.replace("_raw", "")
    
    # Limpieza dinámica según tipo
    mapeo_raw_limpio = {}
    
    # Enteros
    for col_raw in esquema.get("enteros", []):
        col_limpio = obtener_col_limpia(col_raw)
        resultado[col_limpio] = resultado[col_raw].apply(
            lambda x: cleaner.limpiar_numero(x, "int")
        )
        mapeo_raw_limpio[col_raw] = col_limpio
    
    # Strings
    for col_raw in esquema.get("strings", []):
        col_limpio = obtener_col_limpia(col_raw)
        resultado[col_limpio] = resultado[col_raw].apply(cleaner.limpiar_string)
        mapeo_raw_limpio[col_raw] = col_limpio
    
    # Decimales
    for col_raw in esquema.get("decimales", []):
        col_limpio = obtener_col_limpia(col_raw)
        resultado[col_limpio] = resultado[col_raw].apply(
            lambda x: cleaner.limpiar_numero(x, "float")
        )
        mapeo_raw_limpio[col_raw] = col_limpio
    
    # Fechas
    for col_raw in esquema.get("fechas", []):
        col_limpio = obtener_col_limpia(col_raw)
        resultado[col_limpio] = resultado[col_raw].apply(cleaner.limpiar_fecha)
        mapeo_raw_limpio[col_raw] = col_limpio
    
    # Género (especial)
    for col_raw in esquema.get("genero", []):
        col_limpio = obtener_col_limpia(col_raw)
        resultado[col_limpio] = resultado[col_raw].apply(cleaner.limpiar_genero)
        mapeo_raw_limpio[col_raw] = col_limpio
    
    # Nacionalidad (especial)
    for col_raw in esquema.get("nacionalidad", []):
        col_limpio = obtener_col_limpia(col_raw)
        resultado[col_limpio] = resultado[col_raw].apply(cleaner.limpiar_nacionalidad)
        mapeo_raw_limpio[col_raw] = col_limpio
    
    # Validaciones especiales por tipo
    for validacion in esquema.get("validaciones_especiales", []):
        tipo_val = validacion.get("tipo")
        
        if tipo_val == "dni":
            col = validacion["campo"]
            resultado.loc[
                resultado[col].notna() & ~resultado[col].apply(cleaner.limpiar_dni),
                col
            ] = None
        
        elif tipo_val == "no_negativo":
            campos = validacion.get("campos", [validacion.get("campo")])
            for campo in campos:
                if campo in resultado.columns:
                    resultado.loc[resultado[campo].notna() & (resultado[campo] < 0), campo] = None
    
    # Validación de requeridos: construir máscara booleana dinámica
    valido = pd.Series(True, index=resultado.index)
    requeridos = obtener_columnas_requeridas(esquema)
    for col in requeridos:
        if col in resultado.columns:
            valido &= resultado[col].notna()
    
    validos = resultado[valido].copy()
    registrar_rechazos(tabla_nombre, total, len(validos))
    
    # Deduplicación secuencial según el esquema
    duplicados_totales = 0
    claves_dedup = obtener_claves_deduplicacion(esquema)
    for claves in claves_dedup:
        validos, duplicados = quitar_duplicados(validos, claves, keep="first")
        duplicados_totales += duplicados
    
    # Transformaciones especiales por entidad
    if esquema_nombre == "estudiante":
        # Generar fecha_ingreso desde anio_ingreso
        validos["fecha_ingreso"] = validos["anio_ingreso"].apply(fecha_desde_anio)
    
    columnas_finales = esquema["columnas_salida"]
    return validos[columnas_finales], estadisticas(total, len(validos), duplicados_totales)


# ============================================
# TRANSFORMACIONES BASE DESDE STAGING
# ============================================


def transformar_estudiante_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "estudiante")


def transformar_programa_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "programa")


def transformar_facultad_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "facultad")


def transformar_departamento_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "departamento")


def transformar_docente_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "docente")


def transformar_curso_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "curso")


def transformar_dictado_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "dictado")


def transformar_inscripcion_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "inscripcion")


def transformar_examen_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "examen")


def transformar_evaluacion_base(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    return transformar_entidad_base_generica(df, "evaluacion")


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
                "fecha_ingreso": "anioIngreso",
                "nombre_programa": "nombrePrograma",
                "tipo_programa": "tipoPrograma",
                "duracion_anios_programa": "duracionAniosPrograma",
            }
        )
        .assign(
            edadIngreso=calcular_edad_vectorizada(
                df["fecha_nacimiento"], df["fecha_ingreso"]
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
    resultado, duplicados = quitar_duplicados(
        resultado, ["dictadoSKey", "tiempoSKey"], keep="last"
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
    datos_raw: Dict[str, pd.DataFrame] = {}
    stats_base: Dict[str, Dict] = {}

    for clave, (tabla_stg, funcion) in staging_transformaciones.items():
        print(f"  Procesando {tabla_stg}...", flush=True)
        df_raw = leer_tabla_staging(tabla_stg)
        datos_raw[clave] = df_raw
        df_limpio, stats = funcion(df_raw)
        datos[clave] = df_limpio
        stats_base[tabla_stg] = stats
        if stats["rechazados"] > 0 or stats["duplicados"] > 0:
            print(
                f"    Atención: rechazados={stats['rechazados']} | duplicados={stats['duplicados']}",
                flush=True,
            )

    reporte["staging_limpieza"] = stats_base

    # 2. Consolidación y remapeos de duplicados (estudiantes -> inscripciones -> examenes)
    print("[2/5] Detección y remapeo de duplicados (estudiantes / inscripciones / examenes)...", flush=True)

    # 2.1 Detectar y persistir estudiantes duplicados por DNI
    print("  [2.1] Construyendo equivalencias de estudiantes duplicados...", flush=True)
    estudiantes_para_mapeo = preparar_estudiantes_para_mapeo_duplicados(
        datos_raw.get("estudiantes", pd.DataFrame())
    )
    registros_dup_est = construir_mapeo_estudiantes_duplicados(
        estudiantes_para_mapeo
    )
    stats_est_persist = persistir_registros_estudiantes_duplicados(registros_dup_est)
    mapa_est_dup = obtener_mapa_estudiantes_duplicados()

    # Remapear id_estudiante en inscripciones en memoria usando tabla stg_estudiantes_repetidos
    print("  [2.2] Remapeando inscripciones por equivalencias de estudiantes...", flush=True)
    datos["inscripciones"], cnt_ins_remapeadas = remapear_ids(
        datos.get("inscripciones", pd.DataFrame()),
        mapa_est_dup,
        "id_estudiante",
        etiqueta="inscripciones.id_estudiante",
    )

    # 2.3 Detectar y persistir inscripciones duplicadas SOLO para estudiantes duplicados
    print("  [2.3] Construyendo equivalencias de inscripciones duplicadas...", flush=True)
    estudiantes_canonicos_duplicados = set(mapa_est_dup.values()) if mapa_est_dup else set()
    registros_dup_ins = construir_mapeo_inscripciones_duplicadas(
        datos.get("inscripciones", pd.DataFrame()),
        estudiantes_canonicos_duplicados=estudiantes_canonicos_duplicados,
    )
    stats_ins_persist = persistir_registros_inscripciones_duplicadas(registros_dup_ins)
    mapa_ins_dup = obtener_mapa_inscripciones_duplicadas()

    # 2.4 Remapear id_inscripcion en examenes usando tabla stg_inscripciones_repetidas
    print("  [2.4] Remapeando exámenes por equivalencias de inscripciones...", flush=True)
    datos["examenes"], cnt_exam_remapeadas = remapear_ids(
        datos.get("examenes", pd.DataFrame()),
        mapa_ins_dup,
        "id_inscripcion",
        etiqueta="examenes.id_inscripcion",
    )

    # 2.5 Consolidar intentos SOLO en casos de inscripciones duplicadas remapeadas
    print("  [2.5] Consolidando intentos en casos impactados por duplicados...", flush=True)
    datos["examenes"], stats_consolidacion_examen = consolidar_examenes_duplicados(
        datos.get("examenes", pd.DataFrame()),
        datos.get("inscripciones", pd.DataFrame()),
        mapa_ins_dup,
    )

    reporte["duplicados"] = {
        "estudiantes_registrados": stats_est_persist.get("registrados", 0),
        "inscripciones_registradas": stats_ins_persist.get("registrados", 0),
        "inscripciones_remapeadas": int(cnt_ins_remapeadas),
        "examenes_remapeados": int(cnt_exam_remapeadas),
        "examenes_consolidados": stats_consolidacion_examen,
    }

    print(
        f"  Duplicados: estudiantes_reg={stats_est_persist.get('registrados',0)} | inscripciones_reg={stats_ins_persist.get('registrados',0)} | "
        f"inscripciones_remapeadas={cnt_ins_remapeadas} | examenes_remapeados={cnt_exam_remapeadas} | "
        f"examenes_afectados={stats_consolidacion_examen.get('afectados',0)} | examenes_eliminados={stats_consolidacion_examen.get('eliminados',0)}",
        flush=True,
    )

    # 3. Construcción dimensional.
    print("[3/5] Construcción de dimensiones...", flush=True)
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

    print(
        f"  Dimensiones listas: Tiempo={len(dim_tiempo)} | estudiante={len(dim_estudiante)} | Dictado={len(dim_dictado)}",
        flush=True,
    )

    # 3. Truncate único de todo el DWH antes de cargar.
    print("[3/5] Reinicio controlado de tablas DWH...", flush=True)
    truncar_dwh()

    # 4. Carga de dimensiones.
    print("[4/5] Carga de dimensiones y hechos...", flush=True)
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