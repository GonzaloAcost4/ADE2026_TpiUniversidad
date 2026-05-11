DROP DATABASE IF EXISTS dw_universidad;
CREATE DATABASE IF NOT EXISTS dw_universidad;
USE dw_universidad;

-- Tabla Dimensión: Tiempo
CREATE TABLE dim_tiempo (
    tiempo_skey INT PRIMARY KEY,
    fecha DATE NOT NULL,
    dia INT NOT NULL,
    mes VARCHAR(20) NOT NULL,
    anio INT NOT NULL,
    periodo_academico VARCHAR(50),
    es_feriado BOOLEAN DEFAULT FALSE
) ENGINE=InnoDB;

-- Tabla Dimensión: Dictado
CREATE TABLE dim_dictado (
    dictado_skey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_dictado INT NOT NULL,
    periodo INT,
    turno VARCHAR(50),
    aula VARCHAR(50),
    cupo_max INT,
    codigo_curso VARCHAR(20),
    nombre_curso VARCHAR(100),
    horas_teoria INT,
    horas_practica INT,
    horas_lab INT,
    nivel_curso INT,
    nombre_docente VARCHAR(100),
    apellido_docente VARCHAR(100),
    titulo_docente VARCHAR(100),
    categoria_docente VARCHAR(100),
    dedicacion_docente VARCHAR(100),
    nombre_dpto VARCHAR(100),
    nombre_fac VARCHAR(100),
    ciudad_fac VARCHAR(100),
    prov_fac VARCHAR(100),
    valid_from DATE NOT NULL,
    valid_to DATE,
    es_actual BOOLEAN DEFAULT TRUE,
    CONSTRAINT chk_vigencia_dictado CHECK (valid_to IS NULL OR valid_to >= valid_from)
) ENGINE=InnoDB;

-- Tabla Dimensión: estudiante
CREATE TABLE dim_estudiante (
    estudiante_skey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_estudiante INT NOT NULL,
    dni INT NOT NULL,
    nombre VARCHAR(100),
    apellido VARCHAR(100),
    genero VARCHAR(20),
    fecha_nac DATE,
    nacionalidad VARCHAR(50),
    anio_ingreso DATE,
    edad_ingreso INT,
    egreso_carrera BOOLEAN DEFAULT FALSE,
    anio_egreso DATE,
    abandono_carrera BOOLEAN DEFAULT FALSE,
    anio_abandono DATE,
    nombre_prog VARCHAR(100),
    tipo_prog VARCHAR(50),
    duracion_prog INT,
    anio_plan_prog DATE,
    valid_from DATE NOT NULL,
    valid_to DATE,
    es_actual BOOLEAN DEFAULT TRUE,
    CONSTRAINT chk_vigencia_estudiante CHECK (valid_to IS NULL OR valid_to >= valid_from)
) ENGINE=InnoDB;

-- Tabla de Hecho: Examenestudiante
CREATE TABLE fact_examen_estudiante (
    exam_alum_skey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    estudiante_skey INT NOT NULL,
    tiempo_skey INT NOT NULL,
    dictado_skey INT NOT NULL,
    nota DECIMAL(4,2),
    n_intentos INT,
    aprobado BOOLEAN,
    UNIQUE (estudiante_skey, dictado_skey, n_intentos),
    CONSTRAINT fk_exam_estudiante FOREIGN KEY (estudiante_skey) REFERENCES dim_estudiante(estudiante_skey),
    CONSTRAINT fk_exam_tiempo FOREIGN KEY (tiempo_skey) REFERENCES dim_tiempo(tiempo_skey),
    CONSTRAINT fk_exam_dictado FOREIGN KEY (dictado_skey) REFERENCES dim_dictado(dictado_skey)
) ENGINE=InnoDB;

-- Tabla de Hecho: EvaluacionDictado
CREATE TABLE fact_evaluacion_dictado (
    eval_dic_skey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    dictado_skey INT NOT NULL,
    tiempo_skey INT NOT NULL,
    nota_dictado DECIMAL(5,2),
    nota_cont DECIMAL(5,2),
    nota_general DECIMAL(5,2),
    UNIQUE (dictado_skey, tiempo_skey),
    CONSTRAINT fk_eval_dictado FOREIGN KEY (dictado_skey) REFERENCES dim_dictado(dictado_skey),
    CONSTRAINT fk_eval_tiempo FOREIGN KEY (tiempo_skey) REFERENCES dim_tiempo(tiempo_skey)
) ENGINE=InnoDB;

-- Tabla de Hecho: Inscripcion
CREATE TABLE fact_inscripcion (
    inscrip_skey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    estudiante_skey INT NOT NULL,
    tiempo_skey INT NOT NULL,
    dictado_skey INT NOT NULL,
    estado VARCHAR(50),
    abandono BOOLEAN DEFAULT FALSE,
    UNIQUE (estudiante_skey, dictado_skey),
    CONSTRAINT fk_ins_estudiante FOREIGN KEY (estudiante_skey) REFERENCES dim_estudiante(estudiante_skey),
    CONSTRAINT fk_ins_tiempo FOREIGN KEY (tiempo_skey) REFERENCES dim_tiempo(tiempo_skey),
    CONSTRAINT fk_ins_dictado FOREIGN KEY (dictado_skey) REFERENCES dim_dictado(dictado_skey)
) ENGINE=InnoDB;
