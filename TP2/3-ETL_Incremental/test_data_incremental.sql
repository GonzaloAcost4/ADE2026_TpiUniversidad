-- ============================================================================
-- TEST DATA: Carga incremental simulada
-- ============================================================================
-- CONVENCIÓN DE IDs (todos 7+ dígitos para evitar colisión con el CSV):
--   Estudiantes : 9000001 (nuevo), 9000002 (duplicado DNI de 9000001), 9000003 (segundo nuevo)
--   DNIs        : 91111111 (para 9000001 y 9000002), 91333444 (para 9000003)
--   Dictado     : 9999990 (nuevo)
--   Inscripcion : 9900001 (de 9000001), 9900002 (de 9000002 → duplicada), 9900003 (de 9000003)
--   Exámenes    : 9800001-9800002 (de 9900001), 9800003-9800004 (de 9900002), 9800005 (de 9900003)
--   Evaluaciones: 9990001, 9990002
--
-- PREREQUISITO: Haber corrido carga_staging.py + transformacion.py
-- EJECUCION   : Correr este SQL en stg_universidad, luego carga_incremental.py
-- ============================================================================

USE stg_universidad;

-- ============================================================================
-- ESCENARIO 1: Estudiante completamente nuevo
-- ID 9000001, DNI 91111111 (no existe en CSV), programa 6
-- ============================================================================
INSERT INTO stg_estudiante
    (archivo_origen, fecha_carga, id_estudiante_raw, dni_raw, apellido_raw,
     nombre_raw, genero_raw, fecha_nacimiento_raw, email_raw, telefono_raw,
     nacionalidad_raw, id_programa_raw, anio_ingreso_raw)
VALUES
    ('test_incremental.sql', NOW(), '9000001', '91111111', 'Pérez', 'Juan Manuel',
     'M', '2001-03-15', 'jperez_test@test.com', '1155551234',
     'Argentina', '6', '2025');

-- ============================================================================
-- ESCENARIO 2: Estudiante con DNI DUPLICADO del escenario 1
-- ID 9000002, MISMO DNI 91111111 → debe mapearse a 9000001
-- ============================================================================
INSERT INTO stg_estudiante
    (archivo_origen, fecha_carga, id_estudiante_raw, dni_raw, apellido_raw,
     nombre_raw, genero_raw, fecha_nacimiento_raw, email_raw, telefono_raw,
     nacionalidad_raw, id_programa_raw, anio_ingreso_raw)
VALUES
    ('test_incremental.sql', NOW(), '9000002', '91111111', 'Perez', 'Juan M.',
     'M', '2001-03-15', 'jperez2_test@test.com', '1155559999',
     'Argentina', '6', '2025');

-- ============================================================================
-- ESCENARIO 3: Estudiante existente con cambio SCD2
-- id_estudiante=1 ya existe en el DWH. Cambia de programa → nueva versión.
-- ============================================================================
INSERT INTO stg_estudiante
    (archivo_origen, fecha_carga, id_estudiante_raw, dni_raw, apellido_raw,
     nombre_raw, genero_raw, fecha_nacimiento_raw, email_raw, telefono_raw,
     nacionalidad_raw, id_programa_raw, anio_ingreso_raw)
VALUES
    ('test_incremental.sql', NOW(), '1', '34157347', 'Flores', 'Agustín',
     'M', '2004-04-01', 'agustin.flores96@hotmail.com', '1140999828',
     'Argentina', '7', '2020');

-- ============================================================================
-- ESCENARIO 5: Nuevo dictado (usa curso 1 y docente 1 existentes)
-- Dictado ID 9999990, cuatrimestre C1 2026
-- ============================================================================
INSERT INTO stg_dictado
    (archivo_origen, fecha_carga, id_dictado_raw, id_curso_raw, id_docente_raw,
     id_programa_raw, anio_academico_raw, periodo_raw, turno_raw, aula_raw,
     cupo_maximo_raw)
VALUES
    ('test_incremental.sql', NOW(), '9999990', '1', '1',
     '6', '2026', 'C1', 'Mañana', 'LAB-A1', '60');

-- ============================================================================
-- ESCENARIO 6: Inscripción normal del estudiante nuevo (9000001) en dictado 9999990
-- ID inscripción: 9900001
-- ============================================================================
INSERT INTO stg_inscripcion
    (archivo_origen, fecha_carga, id_inscripcion_raw, id_estudiante_raw,
     id_dictado_raw, fecha_inscripcion_raw, estado_raw)
VALUES
    ('test_incremental.sql', NOW(), '9900001', '9000001',
     '9999990', '2026-03-10', 'Activa');

-- ============================================================================
-- ESCENARIO 7: Inscripción del estudiante DUPLICADO (9000002) en el MISMO dictado
-- Tras remapeo (9000002→9000001) apunta al mismo (alumno, dictado) que 9900001.
-- Debe detectarse como duplicada y remapearse a 9900001.
-- ============================================================================
INSERT INTO stg_inscripcion
    (archivo_origen, fecha_carga, id_inscripcion_raw, id_estudiante_raw,
     id_dictado_raw, fecha_inscripcion_raw, estado_raw)
VALUES
    ('test_incremental.sql', NOW(), '9900002', '9000002',
     '9999990', '2026-03-12', 'Activa');

-- ============================================================================
-- ESCENARIO 8: Exámenes sobre inscripción 9900001 (normal)
-- Intento 1 desaprobado, intento 2 aprobado. El aprobado corta la cadena.
-- ============================================================================
INSERT INTO stg_examen
    (archivo_origen, fecha_carga, id_examen_raw, id_inscripcion_raw,
     fecha_raw, nota_raw, numero_intento_raw, resultado_raw)
VALUES
    ('test_incremental.sql', NOW(), '9800001', '9900001',
     '2026-07-10', '3.50', '1', 'Desaprobado'),
    ('test_incremental.sql', NOW(), '9800002', '9900001',
     '2026-07-24', '7.00', '2', 'Aprobado');

-- ============================================================================
-- ESCENARIO 9: Exámenes sobre inscripción DUPLICADA 9900002
-- Tras remapeo (9900002→9900001) se consolidan. Ya hay aprobado → eliminados.
-- ============================================================================
INSERT INTO stg_examen
    (archivo_origen, fecha_carga, id_examen_raw, id_inscripcion_raw,
     fecha_raw, nota_raw, numero_intento_raw, resultado_raw)
VALUES
    ('test_incremental.sql', NOW(), '9800003', '9900002',
     '2026-08-05', '2.00', '1', 'Desaprobado'),
    ('test_incremental.sql', NOW(), '9800004', '9900002',
     '2026-08-20', '5.50', '2', 'Desaprobado');

-- ============================================================================
-- ESCENARIO 10: Segundo estudiante nuevo SIN duplicados
-- ID 9000003, DNI 91333444, inscripción 9900003, examen aprobado directo
-- ============================================================================
INSERT INTO stg_estudiante
    (archivo_origen, fecha_carga, id_estudiante_raw, dni_raw, apellido_raw,
     nombre_raw, genero_raw, fecha_nacimiento_raw, email_raw, telefono_raw,
     nacionalidad_raw, id_programa_raw, anio_ingreso_raw)
VALUES
    ('test_incremental.sql', NOW(), '9000003', '91333444', 'López', 'María Sol',
     'F', '2002-11-08', 'mslopez_test@test.com', '1166667777',
     'Argentina', '6', '2025');

INSERT INTO stg_inscripcion
    (archivo_origen, fecha_carga, id_inscripcion_raw, id_estudiante_raw,
     id_dictado_raw, fecha_inscripcion_raw, estado_raw)
VALUES
    ('test_incremental.sql', NOW(), '9900003', '9000003',
     '9999990', '2026-03-11', 'Activa');

INSERT INTO stg_examen
    (archivo_origen, fecha_carga, id_examen_raw, id_inscripcion_raw,
     fecha_raw, nota_raw, numero_intento_raw, resultado_raw)
VALUES
    ('test_incremental.sql', NOW(), '9800005', '9900003',
     '2026-07-10', '8.50', '1', 'Aprobado');

-- ============================================================================
-- ESCENARIO 11: Evaluaciones anónimas del dictado 9999990
-- ============================================================================
INSERT INTO stg_evaluacion_curso
    (archivo_origen, fecha_carga, id_evaluacion_raw, id_dictado_raw,
     fecha_evaluacion_raw, puntaje_dictado_raw, puntaje_contenido_raw, valoracion_general_raw)
VALUES
    ('test_incremental.sql', NOW(), '9990001', '9999990',
     '2026-07-20', '8.5', '9.0', '8.75'),
    ('test_incremental.sql', NOW(), '9990002', '9999990',
     '2026-07-20', '7.0', '7.5', '7.25');

-- ============================================================================
-- RESULTADO ESPERADO DESPUÉS DEL INCREMENTAL:
-- ============================================================================
-- dim_estudiante:
--   + 9000001 (Juan Manuel Pérez, DNI 91111111) → nuevo
--   - 9000002 → NO insertado (duplicado DNI, mapeado a 9000001)
--   + 9000003 (María Sol López, DNI 91333444) → nuevo
--   ~ ID=1   → nueva versión SCD2 (cambió de programa 3→7)
-- dim_dictado:
--   + 9999990 → nuevo
-- fact_inscripcion:
--   + 9900001 (alumno 9000001, dictado 9999990) → insertada
--   - 9900002 → NO insertada (duplicada → mapeada a 9900001)
--   + 9900003 (alumno 9000003, dictado 9999990) → insertada
-- fact_examen_estudiante:
--   + 9900001: intento 1 (3.50 desaprobado), intento 2 (7.00 aprobado)
--   - 9900002: eliminados en consolidación (ya hay aprobado en 9900001)
--   + 9900003: intento 1 (8.50 aprobado)
-- fact_evaluacion_dictado:
--   + 2 evaluaciones de 9999990
-- ============================================================================
