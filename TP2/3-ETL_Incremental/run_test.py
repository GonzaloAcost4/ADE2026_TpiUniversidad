"""
Inserta los datos de test directamente (sin depender del parser SQL) y corre el incremental.
"""
import sys
import time
sys.path.append('e:/Documentos/Facu/2026/ADE/ADE2026_TpiUniversidad/TP2/2-ETL_CargaInicial')
from transformacion import engine_stg
from sqlalchemy import text

print("[0] Limpiando restos de test anteriores...")
with engine_stg.begin() as conn:
    for t in ['stg_examen', 'stg_evaluacion_curso', 'stg_inscripcion', 'stg_dictado', 'stg_estudiante']:
        r = conn.execute(text(f"DELETE FROM {t} WHERE archivo_origen = 'test_incremental.sql'"))
        print(f"  {t}: {r.rowcount} eliminadas")

print("[1] Insertando datos de test...")
with engine_stg.begin() as conn:
    # === ESCENARIO 1: Estudiante nuevo ===
    conn.execute(text("""
        INSERT INTO stg_estudiante (archivo_origen, fecha_carga, id_estudiante_raw, dni_raw,
            apellido_raw, nombre_raw, genero_raw, fecha_nacimiento_raw, email_raw,
            telefono_raw, nacionalidad_raw, id_programa_raw, anio_ingreso_raw)
        VALUES ('test_incremental.sql', NOW(), '9000001', '91111111', 'Pérez', 'Juan Manuel',
            'M', '2001-03-15', 'jperez_test@test.com', '1155551234', 'Argentina', '6', '2025')
    """))
    # === ESCENARIO 2: Estudiante con DNI duplicado ===
    conn.execute(text("""
        INSERT INTO stg_estudiante (archivo_origen, fecha_carga, id_estudiante_raw, dni_raw,
            apellido_raw, nombre_raw, genero_raw, fecha_nacimiento_raw, email_raw,
            telefono_raw, nacionalidad_raw, id_programa_raw, anio_ingreso_raw)
        VALUES ('test_incremental.sql', NOW(), '9000002', '91111111', 'Perez', 'Juan M.',
            'M', '2001-03-15', 'jperez2_test@test.com', '1155559999', 'Argentina', '6', '2025')
    """))
    # === ESCENARIO 3: Estudiante existente SCD2 ===
    conn.execute(text("""
        INSERT INTO stg_estudiante (archivo_origen, fecha_carga, id_estudiante_raw, dni_raw,
            apellido_raw, nombre_raw, genero_raw, fecha_nacimiento_raw, email_raw,
            telefono_raw, nacionalidad_raw, id_programa_raw, anio_ingreso_raw)
        VALUES ('test_incremental.sql', NOW(), '1', '34157347', 'Flores', 'Agustín',
            'M', '2004-04-01', 'agustin.flores96@hotmail.com', '1140999828', 'Argentina', '7', '2020')
    """))
    # === ESCENARIO 5: Nuevo dictado ===
    conn.execute(text("""
        INSERT INTO stg_dictado (archivo_origen, fecha_carga, id_dictado_raw, id_curso_raw,
            id_docente_raw, id_programa_raw, anio_academico_raw, periodo_raw, turno_raw,
            aula_raw, cupo_maximo_raw)
        VALUES ('test_incremental.sql', NOW(), '9999990', '1', '1', '6', '2026', 'C1', 'Mañana', 'LAB-A1', '60')
    """))
    # === ESCENARIO 6: Inscripción normal 9000001 ===
    conn.execute(text("""
        INSERT INTO stg_inscripcion (archivo_origen, fecha_carga, id_inscripcion_raw,
            id_estudiante_raw, id_dictado_raw, fecha_inscripcion_raw, estado_raw)
        VALUES ('test_incremental.sql', NOW(), '9900001', '9000001', '9999990', '2026-03-10', 'Activa')
    """))
    # === ESCENARIO 7: Inscripción del duplicado ===
    conn.execute(text("""
        INSERT INTO stg_inscripcion (archivo_origen, fecha_carga, id_inscripcion_raw,
            id_estudiante_raw, id_dictado_raw, fecha_inscripcion_raw, estado_raw)
        VALUES ('test_incremental.sql', NOW(), '9900002', '9000002', '9999990', '2026-03-12', 'Activa')
    """))
    # === ESCENARIO 8: Exámenes sobre 9900001 ===
    conn.execute(text("""
        INSERT INTO stg_examen (archivo_origen, fecha_carga, id_examen_raw, id_inscripcion_raw,
            fecha_raw, nota_raw, numero_intento_raw, resultado_raw)
        VALUES ('test_incremental.sql', NOW(), '9800001', '9900001', '2026-07-10', '3.50', '1', 'Desaprobado')
    """))
    conn.execute(text("""
        INSERT INTO stg_examen (archivo_origen, fecha_carga, id_examen_raw, id_inscripcion_raw,
            fecha_raw, nota_raw, numero_intento_raw, resultado_raw)
        VALUES ('test_incremental.sql', NOW(), '9800002', '9900001', '2026-07-24', '7.00', '2', 'Aprobado')
    """))
    # === ESCENARIO 9: Exámenes sobre inscripción duplicada ===
    conn.execute(text("""
        INSERT INTO stg_examen (archivo_origen, fecha_carga, id_examen_raw, id_inscripcion_raw,
            fecha_raw, nota_raw, numero_intento_raw, resultado_raw)
        VALUES ('test_incremental.sql', NOW(), '9800003', '9900002', '2026-08-05', '2.00', '1', 'Desaprobado')
    """))
    conn.execute(text("""
        INSERT INTO stg_examen (archivo_origen, fecha_carga, id_examen_raw, id_inscripcion_raw,
            fecha_raw, nota_raw, numero_intento_raw, resultado_raw)
        VALUES ('test_incremental.sql', NOW(), '9800004', '9900002', '2026-08-20', '5.50', '2', 'Desaprobado')
    """))
    # === ESCENARIO 10: Segundo alumno nuevo ===
    conn.execute(text("""
        INSERT INTO stg_estudiante (archivo_origen, fecha_carga, id_estudiante_raw, dni_raw,
            apellido_raw, nombre_raw, genero_raw, fecha_nacimiento_raw, email_raw,
            telefono_raw, nacionalidad_raw, id_programa_raw, anio_ingreso_raw)
        VALUES ('test_incremental.sql', NOW(), '9000003', '91333444', 'López', 'María Sol',
            'F', '2002-11-08', 'mslopez_test@test.com', '1166667777', 'Argentina', '6', '2025')
    """))
    conn.execute(text("""
        INSERT INTO stg_inscripcion (archivo_origen, fecha_carga, id_inscripcion_raw,
            id_estudiante_raw, id_dictado_raw, fecha_inscripcion_raw, estado_raw)
        VALUES ('test_incremental.sql', NOW(), '9900003', '9000003', '9999990', '2026-03-11', 'Activa')
    """))
    conn.execute(text("""
        INSERT INTO stg_examen (archivo_origen, fecha_carga, id_examen_raw, id_inscripcion_raw,
            fecha_raw, nota_raw, numero_intento_raw, resultado_raw)
        VALUES ('test_incremental.sql', NOW(), '9800005', '9900003', '2026-07-10', '8.50', '1', 'Aprobado')
    """))
    # === ESCENARIO 11: Evaluaciones anónimas ===
    conn.execute(text("""
        INSERT INTO stg_evaluacion_curso (archivo_origen, fecha_carga, id_evaluacion_raw,
            id_dictado_raw, fecha_evaluacion_raw, puntaje_dictado_raw, puntaje_contenido_raw, valoracion_general_raw)
        VALUES ('test_incremental.sql', NOW(), '9990001', '9999990', '2026-07-20', '8.5', '9.0', '8.75')
    """))
    conn.execute(text("""
        INSERT INTO stg_evaluacion_curso (archivo_origen, fecha_carga, id_evaluacion_raw,
            id_dictado_raw, fecha_evaluacion_raw, puntaje_dictado_raw, puntaje_contenido_raw, valoracion_general_raw)
        VALUES ('test_incremental.sql', NOW(), '9990002', '9999990', '2026-07-20', '7.0', '7.5', '7.25')
    """))

print("[1] Datos insertados OK")
print("[2] Iniciando carga_incremental.py en 2 segundos...")
time.sleep(2)

import subprocess
import sys
try:
    print("\n" + "=" * 70)
    print("  EJECUTANDO CARGA INCREMENTAL")
    print("=" * 70)
    subprocess.run([sys.executable, "carga_incremental.py"], check=True)
except subprocess.CalledProcessError as e:
    print(f"\n[ERROR] Falló la carga incremental: {e}")
    sys.exit(1)
