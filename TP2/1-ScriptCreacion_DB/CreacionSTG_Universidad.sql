DROP DATABASE IF EXISTS stg_universidad;
CREATE DATABASE stg_universidad;
USE stg_universidad;

-- Tabla 1: STG_ESTUDIANTE
CREATE TABLE stg_estudiante (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_estudiante_raw VARCHAR(50) NULL,
    dni_raw VARCHAR(50) NULL,
    apellido_raw VARCHAR(100) NULL,
    nombre_raw VARCHAR(100) NULL,
    genero_raw VARCHAR(30) NULL,
    fecha_nacimiento_raw VARCHAR(50) NULL,
    email_raw VARCHAR(200) NULL,
    telefono_raw VARCHAR(100) NULL,
    nacionalidad_raw VARCHAR(80) NULL,
    id_programa_raw VARCHAR(50) NULL,
    anio_ingreso_raw VARCHAR(50) NULL,
    INDEX IX_stg_estudiante_id (id_estudiante_raw)
) ENGINE=InnoDB;

-- Tabla 2: STG_DOCENTE
CREATE TABLE stg_docente (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_docente_raw VARCHAR(50) NULL,
    apellido_raw VARCHAR(150) NULL,
    nombre_raw VARCHAR(150) NULL,
    titulo_raw VARCHAR(100) NULL,
    categoria_raw VARCHAR(100) NULL,
    dedicacion_raw VARCHAR(100) NULL,
    id_departamento_raw VARCHAR(50) NULL,
    INDEX IX_stg_docente_id (id_docente_raw)
) ENGINE=InnoDB;

-- Tabla 3: STG_DEPARTAMENTO
CREATE TABLE stg_departamento (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_departamento_raw VARCHAR(50) NULL,
    nombre_raw VARCHAR(150) NULL,
    id_facultad_raw VARCHAR(50) NULL,
    INDEX IX_stg_depto_id (id_departamento_raw)
) ENGINE=InnoDB;

-- Tabla 4: STG_FACULTAD
CREATE TABLE stg_facultad (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_facultad_raw VARCHAR(50) NULL,
    nombre_raw VARCHAR(150) NULL,
    ciudad_raw VARCHAR(100) NULL,
    provincia_raw VARCHAR(100) NULL,
    INDEX IX_stg_facultad_id (id_facultad_raw)
) ENGINE=InnoDB;

-- Tabla 5: STG_PROGRAMA
CREATE TABLE stg_programa (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_programa_raw VARCHAR(50) NULL,
    nombre_raw VARCHAR(150) NULL,
    tipo_raw VARCHAR(100) NULL,
    duracion_anios_raw VARCHAR(50) NULL,
    id_facultad_raw VARCHAR(50) NULL,
    INDEX IX_stg_programa_id (id_programa_raw)
) ENGINE=InnoDB;

-- Tabla 6: STG_CURSO
CREATE TABLE stg_curso (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_curso_raw VARCHAR(50) NULL,
    codigo_raw VARCHAR(50) NULL,
    nombre_raw VARCHAR(150) NULL,
    horas_teorica_raw VARCHAR(50) NULL,
    horas_ejercicios_raw VARCHAR(50) NULL,
    horas_laboratorio_raw VARCHAR(50) NULL,
    anio_plan_raw VARCHAR(50) NULL,
    nivel_raw VARCHAR(50) NULL,
    INDEX IX_stg_curso_id (id_curso_raw)
) ENGINE=InnoDB;

-- Tabla 7: STG_CURSO_PROGRAMA
CREATE TABLE stg_curso_programa (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_curso_raw VARCHAR(50) NULL,
    id_programa_raw VARCHAR(50) NULL,
    INDEX IX_stg_cur_prog_curso (id_curso_raw),
    INDEX IX_stg_cur_prog_prog (id_programa_raw)
) ENGINE=InnoDB;

-- Tabla 8: STG_DICTADO
CREATE TABLE stg_dictado (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_dictado_raw VARCHAR(50) NULL,
    id_curso_raw VARCHAR(50) NULL,
    id_docente_raw VARCHAR(50) NULL,
    id_programa_raw VARCHAR(50) NULL,
    anio_academico_raw VARCHAR(50) NULL,
    periodo_raw VARCHAR(50) NULL,
    turno_raw VARCHAR(80) NULL,
    aula_raw VARCHAR(100) NULL,
    cupo_maximo_raw VARCHAR(50) NULL,
    INDEX IX_stg_dictado_id (id_dictado_raw)
) ENGINE=InnoDB;

-- Tabla 9: STG_INSCRIPCION
CREATE TABLE stg_inscripcion (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_inscripcion_raw VARCHAR(50) NULL,
    id_estudiante_raw VARCHAR(50) NULL,
    id_dictado_raw VARCHAR(50) NULL,
    fecha_inscripcion_raw VARCHAR(80) NULL,
    estado_raw VARCHAR(100) NULL,
    INDEX IX_stg_inscrip_id (id_inscripcion_raw),
    INDEX IX_stg_inscrip_est (id_estudiante_raw)
) ENGINE=InnoDB;

-- Tabla 10: STG_EXAMEN
CREATE TABLE stg_examen (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_examen_raw VARCHAR(50) NULL,
    id_inscripcion_raw VARCHAR(50) NULL,
    fecha_raw VARCHAR(80) NULL,
    nota_raw VARCHAR(50) NULL,
    numero_intento_raw VARCHAR(50) NULL,
    resultado_raw VARCHAR(100) NULL,
    INDEX IX_stg_examen_id (id_examen_raw),
    INDEX IX_stg_examen_ins (id_inscripcion_raw)
) ENGINE=InnoDB;

-- Tabla 11: STG_EVALUACION_CURSO
CREATE TABLE stg_evaluacion_curso (
    row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    archivo_origen VARCHAR(255) NULL,
    fecha_carga DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_evaluacion_raw VARCHAR(50) NULL,
    id_dictado_raw VARCHAR(50) NULL,
    id_estudiante_raw VARCHAR(50) NULL,
    fecha_evaluacion_raw VARCHAR(80) NULL,
    puntaje_dictado_raw VARCHAR(50) NULL,
    puntaje_contenido_raw VARCHAR(50) NULL,
    valoracion_general_raw VARCHAR(50) NULL,
    INDEX IX_stg_eval_id (id_evaluacion_raw),
    INDEX IX_stg_eval_dictado (id_dictado_raw),
    INDEX IX_stg_eval_estudiante (id_estudiante_raw)
) ENGINE=InnoDB;
