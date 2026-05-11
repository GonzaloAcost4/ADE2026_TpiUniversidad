# Web App - Exploración local de Staging y Data Warehouse

Esta aplicación Flask permite explorar localmente las bases `stg_universidad` y `dw_universidad` desde el navegador.

La app está alineada con el esquema actual del proyecto y no depende de listas hardcodeadas de tablas. Obtiene las tablas reales desde MySQL en tiempo de ejecución.

## Funcionalidades

- listar tablas reales de staging y DWH
- inspeccionar esquema de cualquier tabla
- previsualizar registros
- generar gráficos rápidos por tabla/columna
- ejecutar consultas SQL complejas en entorno local
- ejecutar CTE, `SHOW`, `EXPLAIN`, `SELECT` y también comandos SQL de mantenimiento si hace falta

## Archivos

- `app.py`: backend Flask y endpoints API
- `templates/index.html`: interfaz principal

## Requisitos

La app usa las mismas variables de entorno que el resto del proyecto:

- `DB_USER`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `STG_DATABASE`
- `DWH_DATABASE`

## Ejecución

Desde la raíz del repositorio o desde `TP2/4-Web_App`:

```bash
python TP2/4-Web_App/app.py
```

Luego abrir:

```text
http://127.0.0.1:5000
```

## Pestañas

### Dashboard
Permite:

- elegir base (`stg` o `dwh`)
- elegir tabla
- inspeccionar columnas
- ver preview de datos
- generar gráficos simples

### SQL Explorer
Permite ejecutar SQL libre contra la base seleccionada.

Está pensado para uso local del proyecto, por lo que no restringe las consultas a solo `SELECT`.

## Nota operativa

Si cambian tablas o estructuras del esquema, la app las toma automáticamente desde MySQL. No hace falta modificar listas manuales dentro del código para sincronizar nombres de tablas.
