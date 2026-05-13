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
    periodo VARCHAR(10),
    es_feriado BOOLEAN DEFAULT FALSE
) ENGINE=InnoDB;

-- Tabla Dimensión: Dictado
CREATE TABLE dim_dictado (
    dictado_skey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id_dictado INT NOT NULL,
    periodo VARCHAR(10),
    turno VARCHAR(50),
    aula VARCHAR(50),
    cupo_maximo INT,
    codigo_curso VARCHAR(20),
    nombre_curso VARCHAR(100),
    horas_teorica INT,
    horas_ejercicio INT,
    horas_laboratorio INT,
    nivel_curso INT,
    nombre_docente VARCHAR(100),
    apellido_docente VARCHAR(100),
    titulo_docente VARCHAR(100),
    categoria_docente VARCHAR(100),
    dedicacion_docente VARCHAR(100),
    nombre_departamento VARCHAR(100),
    nombre_facultad VARCHAR(100),
    ciudad_facultad VARCHAR(100),
    provincia_facultad VARCHAR(100),
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
    fecha_nacimiento DATE,
    nacionalidad VARCHAR(50),
    anio_ingreso INT,
    edad_ingreso INT,
    egreso_carrera BOOLEAN DEFAULT FALSE,
    anio_egreso INT,
    abandono_carrera BOOLEAN DEFAULT FALSE,
    anio_abandono INT,
    nombre_programa VARCHAR(100),
    tipo_programa VARCHAR(50),
    duracion_programa INT,
    anio_plan_programa INT,
    valid_from DATE NOT NULL,
    valid_to DATE,
    es_actual BOOLEAN DEFAULT TRUE,
    CONSTRAINT chk_vigencia_estudiante CHECK (valid_to IS NULL OR valid_to >= valid_from)
) ENGINE=InnoDB;

-- Tabla de Hecho: Examenestudiante
CREATE TABLE fact_examen_estudiante (
    examen_estudiante_skey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    estudiante_skey INT NOT NULL,
    tiempo_skey INT NOT NULL,
    dictado_skey INT NOT NULL,
    nota DECIMAL(4,2),
    numero_intentos INT,
    aprobado BOOLEAN,
    UNIQUE (estudiante_skey, dictado_skey, numero_intentos),
    CONSTRAINT fk_examen_estudiante FOREIGN KEY (estudiante_skey) REFERENCES dim_estudiante(estudiante_skey),
    CONSTRAINT fk_examen_tiempo FOREIGN KEY (tiempo_skey) REFERENCES dim_tiempo(tiempo_skey),
    CONSTRAINT fk_examen_dictado FOREIGN KEY (dictado_skey) REFERENCES dim_dictado(dictado_skey)
) ENGINE=InnoDB;

-- Tabla de Hecho: EvaluacionDictado
CREATE TABLE fact_evaluacion_dictado (
    evaluacion_dictado_skey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    dictado_skey INT NOT NULL,
    tiempo_skey INT NOT NULL,
    puntaje_dictado DECIMAL(5,2),
    puntaje_contenido DECIMAL(5,2),
    valoracion_general DECIMAL(5,2),
    CONSTRAINT fk_evaluacion_dictado FOREIGN KEY (dictado_skey) REFERENCES dim_dictado(dictado_skey),
    CONSTRAINT fk_evaluacion_tiempo FOREIGN KEY (tiempo_skey) REFERENCES dim_tiempo(tiempo_skey)
) ENGINE=InnoDB;

-- Tabla de Hecho: Inscripcion
CREATE TABLE fact_inscripcion (
    inscripcion_skey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    estudiante_skey INT NOT NULL,
    tiempo_skey INT NOT NULL,
    dictado_skey INT NOT NULL,
    estado VARCHAR(50),
    abandono BOOLEAN DEFAULT FALSE,
    UNIQUE (estudiante_skey, dictado_skey),
    CONSTRAINT fk_inscripcion_estudiante FOREIGN KEY (estudiante_skey) REFERENCES dim_estudiante(estudiante_skey),
    CONSTRAINT fk_inscripcion_tiempo FOREIGN KEY (tiempo_skey) REFERENCES dim_tiempo(tiempo_skey),
    CONSTRAINT fk_inscripcion_dictado FOREIGN KEY (dictado_skey) REFERENCES dim_dictado(dictado_skey)
) ENGINE=InnoDB;
