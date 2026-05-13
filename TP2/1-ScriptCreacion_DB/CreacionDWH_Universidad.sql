CREATE DATABASE IF NOT EXISTS dw_universidad;
USE dw_universidad;

-- Tabla Dimensión: Tiempo
CREATE TABLE dim_tiempo (
    tiempoSKey INT PRIMARY KEY,
    fecha DATE NOT NULL,
    dia INT NOT NULL,
    mes VARCHAR(20) NOT NULL,
    ano INT NOT NULL,
    periodoAcademico VARCHAR(50),
    esFeriado BOOLEAN DEFAULT FALSE
) ENGINE=InnoDB;

-- Tabla Dimensión: Dictado
CREATE TABLE dim_dictado (
    dictadoSKey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    idDictado INT NOT NULL,
    periodo varchar(2),
    turno VARCHAR(50),
    aula VARCHAR(50),
    cupoMax INT,
    codigoCurso VARCHAR(20),
    nombreCurso VARCHAR(100),
    horasTeoCurso INT,
    horasPracCurso INT,
    horasLabCurso INT,
    nivelCurso INT,
    nombreDocente VARCHAR(100),
    apellidoDocente VARCHAR(100),
    tituloDocente VARCHAR(100),
    categoriaDocente VARCHAR(100),
    dedicacionDocente VARCHAR(100),
    nombreDep VARCHAR(100),
    nombreFac VARCHAR(100),
    ciudadFac VARCHAR(100),
    provFac VARCHAR(100),
    valid_from DATE NOT NULL,
    valid_to DATE,
    es_actual BOOLEAN DEFAULT TRUE,
    CONSTRAINT chk_vigencia_dictado CHECK (valid_to IS NULL OR valid_to >= valid_from)
) ENGINE=InnoDB;

-- Tabla Dimensión: Estudiante
CREATE TABLE dim_estudiante (
    alumnoSKey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    idalumno INT NOT NULL,
    dni INT NOT NULL,
    nombre VARCHAR(100),
    apellido VARCHAR(100),
    genero VARCHAR(20),
    fechaNacim DATE,
    nacionalidad VARCHAR(50),
    anioIngreso INT, 
    edadIngreso INT,
    egresoCarrera BOOLEAN DEFAULT FALSE,
    anioEgreso INT, 
    abandonoCarrera BOOLEAN DEFAULT FALSE,
    anioAbandono INT, 
    nombrePrograma VARCHAR(100),
    tipoPrograma VARCHAR(50),
    duracionAniosPrograma INT,
    anioPlanPrograma INT,
    valid_from DATE NOT NULL,
    valid_to DATE,
    es_actual BOOLEAN DEFAULT TRUE,
    CONSTRAINT chk_vigencia_alumno CHECK (valid_to IS NULL OR valid_to >= valid_from)
) ENGINE=InnoDB;

-- Tabla de Hecho: ExamenAlumno
CREATE TABLE fact_examen_estudiante (
    examAlumSK INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    alumnoSKey INT NOT NULL,
    tiempoSKey INT NOT NULL,
    dictadoSKey INT NOT NULL,
    nota DECIMAL(4,2),
    nroIntentos INT,
    aprobado BOOLEAN,
    UNIQUE (alumnoSKey, dictadoSKey, nroIntentos),
    CONSTRAINT fk_exam_alumno FOREIGN KEY (alumnoSKey) REFERENCES dim_estudiante(alumnoSKey),
    CONSTRAINT fk_exam_tiempo FOREIGN KEY (tiempoSKey) REFERENCES dim_tiempo(tiempoSKey),
    CONSTRAINT fk_exam_dictado FOREIGN KEY (dictadoSKey) REFERENCES dim_dictado(dictadoSKey)
) ENGINE=InnoDB;

-- Tabla de Hecho: EvaluacionDictado
CREATE TABLE fact_evaluacion_dictado (
    evalDicSKey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    dictadoSKey INT NOT NULL,
    tiempoSKey INT NOT NULL,
    notaDictado DECIMAL(5,2),
    notaCont DECIMAL(5,2),
    notaGeneral DECIMAL(5,2),
    CONSTRAINT fk_eval_dictado FOREIGN KEY (dictadoSKey) REFERENCES dim_dictado(dictadoSKey),
    CONSTRAINT fk_eval_tiempo FOREIGN KEY (tiempoSKey) REFERENCES dim_tiempo(tiempoSKey)
) ENGINE=InnoDB;

-- Tabla de Hecho: Inscripcion
CREATE TABLE fact_inscripcion (
    InscripSKey INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    alumnoSKey INT NOT NULL,
    tiempoSKey INT NOT NULL,
    dictadoSKey INT NOT NULL,
    estado VARCHAR(50),
    abandono BOOLEAN DEFAULT FALSE,
    UNIQUE (alumnoSKey, dictadoSKey),
    CONSTRAINT fk_ins_alumno FOREIGN KEY (alumnoSKey) REFERENCES dim_estudiante(alumnoSKey),
    CONSTRAINT fk_ins_tiempo FOREIGN KEY (tiempoSKey) REFERENCES dim_tiempo(tiempoSKey),
    CONSTRAINT fk_ins_dictado FOREIGN KEY (dictadoSKey) REFERENCES dim_dictado(dictadoSKey)
) ENGINE=InnoDB;
