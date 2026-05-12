"""
Definición de esquemas de transformación base para staging.

Este archivo centraliza toda la metadata para las transformaciones genéricas.
Elimina la duplicación de código en las 10 funciones de transformación base.
"""

from typing import Dict, List, Optional

# Definición de esquemas: qué columnas limpiar, qué tipos, qué validaciones
ESQUEMAS_TRANSFORMACION = {
    "estudiante": {
        "tabla_stg": "stg_estudiante",
        "tabla_nombre": "stg_estudiante",
        "enteros": [
            "id_estudiante_raw",
            "dni_raw",
            "id_programa_raw",
            "anio_ingreso_raw",
        ],
        "strings": ["apellido_raw", "nombre_raw", "nacionalidad_raw"],
        "genero": ["genero_raw"],
        "fechas": ["fecha_nacimiento_raw"],
        "requeridos": ["id_estudiante", "dni", "apellido", "nombre", "id_programa"],
        "validaciones_especiales": [
            {
                "campo": "dni",
                "tipo": "dni",
                "requerido": True,
            }
        ],
        "clave_deduplicacion": [["id_estudiante"], ["dni"]],
        "columnas_salida": [
            "id_estudiante",
            "dni",
            "apellido",
            "nombre",
            "genero",
            "fecha_nacimiento",
            "nacionalidad",
            "id_programa",
            "fecha_ingreso",
        ],
        "notas": "Incluye transformación especial fecha_ingreso desde anio_ingreso",
    },
    "programa": {
        "tabla_stg": "stg_programa",
        "tabla_nombre": "stg_programa",
        "enteros": ["id_programa_raw", "duracion_anios_raw", "id_facultad_raw"],
        "strings": ["nombre_raw", "tipo_raw"],
        "mapeo_columnas": {
            "nombre_raw": "nombre_programa",
            "tipo_raw": "tipo_programa",
            "duracion_anios_raw": "duracion_anios_programa",
        },
        "requeridos": ["id_programa", "nombre_programa"],
        "clave_deduplicacion": [["id_programa"]],
        "columnas_salida": [
            "id_programa",
            "nombre_programa",
            "tipo_programa",
            "duracion_anios_programa",
            "id_facultad",
        ],
    },
    "facultad": {
        "tabla_stg": "stg_facultad",
        "tabla_nombre": "stg_facultad",
        "enteros": ["id_facultad_raw"],
        "strings": ["nombre_raw", "ciudad_raw", "provincia_raw"],
        "mapeo_columnas": {
            "nombre_raw": "nombre_facultad",
            "ciudad_raw": "ciudad_facultad",
            "provincia_raw": "provincia_facultad",
        },
        "requeridos": ["id_facultad", "nombre_facultad"],
        "clave_deduplicacion": [["id_facultad"]],
        "columnas_salida": [
            "id_facultad",
            "nombre_facultad",
            "ciudad_facultad",
            "provincia_facultad",
        ],
    },
    "departamento": {
        "tabla_stg": "stg_departamento",
        "tabla_nombre": "stg_departamento",
        "enteros": ["id_departamento_raw", "id_facultad_raw"],
        "strings": ["nombre_raw"],
        "mapeo_columnas": {
            "nombre_raw": "nombre_departamento",
        },
        "requeridos": ["id_departamento", "nombre_departamento"],
        "clave_deduplicacion": [["id_departamento"]],
        "columnas_salida": ["id_departamento", "nombre_departamento", "id_facultad"],
    },
    "docente": {
        "tabla_stg": "stg_docente",
        "tabla_nombre": "stg_docente",
        "enteros": ["id_docente_raw", "id_departamento_raw"],
        "strings": [
            "apellido_raw",
            "nombre_raw",
            "titulo_raw",
            "categoria_raw",
            "dedicacion_raw",
        ],
        "mapeo_columnas": {
            "apellido_raw": "apellido_docente",
            "nombre_raw": "nombre_docente",
            "titulo_raw": "titulo_docente",
            "categoria_raw": "categoria_docente",
            "dedicacion_raw": "dedicacion_docente",
        },
        "requeridos": ["id_docente", "apellido_docente", "nombre_docente"],
        "clave_deduplicacion": [["id_docente"]],
        "columnas_salida": [
            "id_docente",
            "apellido_docente",
            "nombre_docente",
            "titulo_docente",
            "categoria_docente",
            "dedicacion_docente",
            "id_departamento",
        ],
    },
    "curso": {
        "tabla_stg": "stg_curso",
        "tabla_nombre": "stg_curso",
        "enteros": [
            "id_curso_raw",
            "horas_teorica_raw",
            "horas_ejercicios_raw",
            "horas_laboratorio_raw",
            "nivel_raw",
        ],
        "strings": ["codigo_raw", "nombre_raw"],
        "mapeo_columnas": {
            "codigo_raw": "codigo_curso",
            "nombre_raw": "nombre_curso",
            "horas_teorica_raw": "horas_teo_curso",
            "horas_ejercicios_raw": "horas_prac_curso",
            "horas_laboratorio_raw": "horas_lab_curso",
            "nivel_raw": "nivel_curso",
        },
        "requeridos": ["id_curso", "nombre_curso"],
        "validaciones_especiales": [
            {
                "campos": [
                    "horas_teo_curso",
                    "horas_prac_curso",
                    "horas_lab_curso",
                    "nivel_curso",
                ],
                "tipo": "no_negativo",
            }
        ],
        "clave_deduplicacion": [["id_curso"]],
        "columnas_salida": [
            "id_curso",
            "codigo_curso",
            "nombre_curso",
            "horas_teo_curso",
            "horas_prac_curso",
            "horas_lab_curso",
            "nivel_curso",
        ],
    },
    "dictado": {
        "tabla_stg": "stg_dictado",
        "tabla_nombre": "stg_dictado",
        "enteros": [
            "id_dictado_raw",
            "id_curso_raw",
            "id_docente_raw",
            "id_programa_raw",
            "cupo_maximo_raw",
        ],
        "strings": ["periodo_raw", "turno_raw", "aula_raw"],
        "mapeo_columnas": {
            "cupo_maximo_raw": "cupo_maximo",
        },
        "requeridos": ["id_dictado", "id_curso", "id_docente"],
        "validaciones_especiales": [
            {"campos": ["cupo_maximo"], "tipo": "no_negativo"}
        ],
        "clave_deduplicacion": [["id_dictado"]],
        "columnas_salida": [
            "id_dictado",
            "id_curso",
            "id_docente",
            "id_programa",
            "periodo",
            "turno",
            "aula",
            "cupo_maximo",
        ],
    },
    "inscripcion": {
        "tabla_stg": "stg_inscripcion",
        "tabla_nombre": "stg_inscripcion",
        "enteros": [
            "id_inscripcion_raw",
            "id_estudiante_raw",
            "id_dictado_raw",
        ],
        "strings": ["estado_raw"],
        "fechas": ["fecha_inscripcion_raw"],
        "mapeo_columnas": {
            "estado_raw": "estado",
        },
        "requeridos": [
            "id_inscripcion",
            "id_estudiante",
            "id_dictado",
            "fecha_inscripcion",
        ],
        "clave_deduplicacion": [["id_inscripcion"]],
        "columnas_salida": [
            "id_inscripcion",
            "id_estudiante",
            "id_dictado",
            "fecha_inscripcion",
            "estado",
        ],
    },
    "examen": {
        "tabla_stg": "stg_examen",
        "tabla_nombre": "stg_examen",
        "enteros": [
            "id_examen_raw",
            "id_inscripcion_raw",
            "numero_intento_raw",
        ],
        "strings": ["resultado_raw"],
        "fechas": ["fecha_raw"],
        "decimales": ["nota_raw"],
        "mapeo_columnas": {
            "fecha_raw": "fecha",
            "nota_raw": "nota",
            "numero_intento_raw": "numero_intento",
            "resultado_raw": "resultado",
        },
        "requeridos": [
            "id_examen",
            "id_inscripcion",
            "fecha",
            "nota",
            "numero_intento",
        ],
        "clave_deduplicacion": [["id_examen"]],
        "columnas_salida": [
            "id_examen",
            "id_inscripcion",
            "fecha",
            "nota",
            "numero_intento",
            "resultado",
        ],
    },
    "evaluacion": {
        "tabla_stg": "stg_evaluacion_curso",
        "tabla_nombre": "stg_evaluacion_curso",
        "enteros": ["id_evaluacion_raw", "id_dictado_raw"],
        "fechas": ["fecha_evaluacion_raw"],
        "decimales": [
            "puntaje_dictado_raw",
            "puntaje_contenido_raw",
            "valoracion_general_raw",
        ],
        "requeridos": ["id_evaluacion", "id_dictado", "fecha_evaluacion"],
        "clave_deduplicacion": [["id_evaluacion"]],
        "columnas_salida": [
            "id_evaluacion",
            "id_dictado",
            "fecha_evaluacion",
            "puntaje_dictado",
            "puntaje_contenido",
            "valoracion_general",
        ],
    },
}


def obtener_esquema(nombre_entidad: str) -> Dict:
    """
    Recupera el esquema de transformación para una entidad.

    Args:
        nombre_entidad: key en ESQUEMAS_TRANSFORMACION

    Returns:
        Dict con los metadatos de la transformación
    """
    if nombre_entidad not in ESQUEMAS_TRANSFORMACION:
        raise ValueError(
            f"Esquema desconocido: {nombre_entidad}. Opciones: {list(ESQUEMAS_TRANSFORMACION.keys())}"
        )
    return ESQUEMAS_TRANSFORMACION[nombre_entidad]


def mapear_columnas_limpias(esquema: Dict) -> Dict[str, str]:
    """
    Genera mapeo {columna_raw: columna_limpia} basado en el esquema.

    Ejemplo:
        "id_estudiante_raw" -> "id_estudiante"
        "apellido_raw" -> "apellido"
    """
    mapeo = {}
    for raw_col in (
        esquema.get("enteros", [])
        + esquema.get("strings", [])
        + esquema.get("fechas", [])
        + esquema.get("decimales", [])
        + esquema.get("genero", [])
    ):
        col_limpia = raw_col.replace("_raw", "")
        mapeo[raw_col] = col_limpia
    return mapeo


def obtener_columnas_requeridas(esquema: Dict) -> List[str]:
    """Devuelve la lista de columnas que deben estar presentes (no nulas) después de limpieza."""
    return esquema.get("requeridos", [])


def obtener_claves_deduplicacion(esquema: Dict) -> List[List[str]]:
    """Devuelve lista de listas de claves para deduplicación secuencial."""
    return esquema.get("clave_deduplicacion", [])
