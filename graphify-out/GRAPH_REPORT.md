# Graph Report - /home/celeste/ADE2026_TpiUniversidad  (2026-05-13)

## Corpus Check
- Corpus is ~21,797 words - fits in a single context window. You may not need a graph.

## Summary
- 216 nodes · 392 edges · 26 communities (10 shown, 16 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 3 edges (avg confidence: 0.7)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Incremental ETL Core|Incremental ETL Core]]
- [[_COMMUNITY_Incremental ETL Tests|Incremental ETL Tests]]
- [[_COMMUNITY_Initial Transformation Pipeline|Initial Transformation Pipeline]]
- [[_COMMUNITY_Project Overview and App|Project Overview and App]]
- [[_COMMUNITY_Cleaning and Fact Building|Cleaning and Fact Building]]
- [[_COMMUNITY_Logging Utilities|Logging Utilities]]
- [[_COMMUNITY_API and SQL Access|API and SQL Access]]
- [[_COMMUNITY_Staging Load Pipeline|Staging Load Pipeline]]
- [[_COMMUNITY_OLTP Schema Tables|OLTP Schema Tables]]
- [[_COMMUNITY_DWH Load Helpers|DWH Load Helpers]]
- [[_COMMUNITY_Duplicate Mapping|Duplicate Mapping]]
- [[_COMMUNITY_Evaluacion Fact Builder|Evaluacion Fact Builder]]
- [[_COMMUNITY_Logger Configure|Logger Configure]]
- [[_COMMUNITY_Logger Instance|Logger Instance]]
- [[_COMMUNITY_Logger Info|Logger Info]]
- [[_COMMUNITY_Logger Warning|Logger Warning]]
- [[_COMMUNITY_Logger Error|Logger Error]]
- [[_COMMUNITY_Logger Debug|Logger Debug]]
- [[_COMMUNITY_Logs Directory|Logs Directory]]
- [[_COMMUNITY_Logger Reset|Logger Reset]]
- [[_COMMUNITY_String Cleaning|String Cleaning]]
- [[_COMMUNITY_Number Parsing|Number Parsing]]
- [[_COMMUNITY_Date Parsing|Date Parsing]]
- [[_COMMUNITY_Gender Normalization|Gender Normalization]]
- [[_COMMUNITY_DNI Validation|DNI Validation]]
- [[_COMMUNITY_Nationality Normalization|Nationality Normalization]]

## God Nodes (most connected - your core abstractions)
1. `ejecutar_transformacion()` - 19 edges
2. `estadisticas()` - 17 edges
3. `quitar_duplicados()` - 15 edges
4. `procesar_incremental()` - 15 edges
5. `procesar_incremental()` - 15 edges
6. `limpiar_numero()` - 14 edges
7. `registrar_rechazos()` - 14 edges
8. `DataCleaner` - 13 edges
9. `TP2 ETL Universidad` - 13 edges
10. `ETL de carga inicial y transformación al Data Warehouse` - 12 edges

## Surprising Connections (you probably didn't know these)
- `DataCleaner` --uses--> `LoggerManager`  [INFERRED]
  2-ETL_CargaInicial/transformacion.py → logging_config.py
- `main()` --calls--> `ejecutar_transformacion()`  [INFERRED]
  2-ETL_CargaInicial/orquestador.py → 2-ETL_CargaInicial/transformacion.py
- `ADE 2026 TPI Universidad` --references--> `TP2 ETL Universidad`  [EXTRACTED]
  README.md → TP2/README.md
- `ADE 2026 TPI Universidad` --references--> `MySQL service`  [EXTRACTED]
  README.md → docker-compose.yml
- `Sistema Centralizado de Logging` --references--> `LoggerManager`  [EXTRACTED]
  TP2/LOGGING_README.md → TP2/logging_config.py

## Hyperedges (group relationships)
- **ETL pipeline actual** — carga_staging_script, transformacion_script, carga_incremental_script, app_script [EXTRACTED 1.00]
- **DWH model dimensional** — dim_tiempo, dim_estudiante, dim_dictado, fact_inscripcion, fact_examen_estudiante, fact_evaluacion_dictado [EXTRACTED 1.00]
- **OLTP schema relacional** — oltp_universidad_erd_facultad, oltp_universidad_erd_departamento, oltp_universidad_erd_docente, oltp_universidad_erd_programa, oltp_universidad_erd_curso, oltp_universidad_erd_curso_programa, oltp_universidad_erd_dictado, oltp_universidad_erd_estudiante, oltp_universidad_erd_inscripcion, oltp_universidad_erd_examen, oltp_universidad_erd_evaluacion_curso [EXTRACTED 1.00]

## Communities (26 total, 16 thin omitted)

### Community 0 - "Incremental ETL Core"
Cohesion: 0.11
Nodes (34): actualizar_dimension_scd1(), actualizar_mapeos_duplicados(), aplicar_scd_dictado(), aplicar_scd_estudiante(), aplicar_scd_generico(), asegurar_tabla_auditoria(), construir_lookups_completos(), dataframe_a_registros() (+26 more)

### Community 1 - "Incremental ETL Tests"
Cohesion: 0.11
Nodes (34): actualizar_dimension_scd1(), actualizar_mapeos_duplicados(), aplicar_scd_dictado(), aplicar_scd_estudiante(), aplicar_scd_generico(), asegurar_tabla_auditoria(), construir_lookups_completos(), dataframe_a_registros() (+26 more)

### Community 2 - "Initial Transformation Pipeline"
Cohesion: 0.1
Nodes (27): cargar_tabla(), consolidar_examenes_duplicados(), construir_dim_tiempo(), construir_fact_evaluacion_dictado(), contar_tabla_dwh(), detectar_duplicados(), ejecutar_transformacion(), imprimir_reporte() (+19 more)

### Community 3 - "Project Overview and App"
Cohesion: 0.13
Nodes (24): Arquitectura de tres capas, Carga full refresh, Carga incremental por delta, Consolidación de alumnos duplicados, dim_dictado, dim_estudiante, dim_tiempo, MySQL service (+16 more)

### Community 4 - "Cleaning and Fact Building"
Cohesion: 0.27
Nodes (22): construir_dim_dictado(), construir_fact_examen_estudiante(), construir_fact_inscripcion(), DataCleaner, estadisticas(), limpiar_numero(), _limpiar_numero_float(), _limpiar_numero_int() (+14 more)

### Community 5 - "Logging Utilities"
Cohesion: 0.26
Nodes (9): configurar(), debug(), error(), info(), LoggerManager, obtener(), Módulo de configuración centralizado para logging en los ETL processes.  Proporc, Gestor centralizado de logging para ETL processes.          Proporciona métodos (+1 more)

### Community 6 - "API and SQL Access"
Cohesion: 0.33
Nodes (9): api_chart(), api_data(), api_meta(), api_schema(), api_sql(), api_tables(), get_database_config(), list_tables() (+1 more)

### Community 7 - "Staging Load Pipeline"
Cohesion: 0.24
Nodes (9): cargar_csv_a_staging(), diagnostico_pre_carga(), ejecutar_carga_staging(), enriquecer_evaluacion_curso(), Lee un CSV desde Sources, lo carga con TRUNCATE (idempotente).      Estrategia:, Verifica que todos los archivos CSV existen y las tablas staging están creadas., Punto de entrada principal: diagnóstico + carga completa de CSVs a staging., Completa evaluacion_curso.csv con la fecha de evaluación.      La evaluación es (+1 more)

### Community 8 - "OLTP Schema Tables"
Cohesion: 0.2
Nodes (11): CURSO, CURSO_PROGRAMA, DEPARTAMENTO, DICTADO, DOCENTE, ESTUDIANTE, EVALUACION_CURSO, EXAMEN (+3 more)

### Community 9 - "DWH Load Helpers"
Cohesion: 0.67
Nodes (3): calcular_edad_ingreso(), construir_dim_estudiante(), Calcula edad de ingreso como diferencia simple de años.

## Knowledge Gaps
- **59 isolated node(s):** `Módulo de configuración centralizado para logging en los ETL processes.  Proporc`, `Gestor centralizado de logging para ETL processes.          Proporciona métodos`, `Configura el logger para un proceso específico.                  Args:`, `Obtiene la instancia del logger.                  Si no existe, crea una nueva c`, `Registra un mensaje de nivel INFO.                  Args:             mensaje (s` (+54 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **16 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DataCleaner` connect `Cleaning and Fact Building` to `Initial Transformation Pipeline`, `Logging Utilities`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **Why does `ejecutar_transformacion()` connect `Initial Transformation Pipeline` to `DWH Load Helpers`, `Cleaning and Fact Building`, `Staging Load Pipeline`?**
  _High betweenness centrality (0.038) - this node is a cross-community bridge._
- **Why does `LoggerManager` connect `Logging Utilities` to `Cleaning and Fact Building`?**
  _High betweenness centrality (0.033) - this node is a cross-community bridge._
- **What connects `Módulo de configuración centralizado para logging en los ETL processes.  Proporc`, `Gestor centralizado de logging para ETL processes.          Proporciona métodos`, `Configura el logger para un proceso específico.                  Args:` to the rest of the system?**
  _59 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Incremental ETL Core` be split into smaller, more focused modules?**
  _Cohesion score 0.11 - nodes in this community are weakly interconnected._
- **Should `Incremental ETL Tests` be split into smaller, more focused modules?**
  _Cohesion score 0.11 - nodes in this community are weakly interconnected._
- **Should `Initial Transformation Pipeline` be split into smaller, more focused modules?**
  _Cohesion score 0.1 - nodes in this community are weakly interconnected._