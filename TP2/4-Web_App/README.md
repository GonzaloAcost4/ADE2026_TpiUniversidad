# Web App - Visualización del Data Warehouse y Staging

Este directorio contiene una aplicación web sencilla (Flask) que permite:

- Ver tablas y datos de la *staging* y del *data warehouse*.
- Generar gráficos (bar, pie, line) seleccionando tabla/columnas.
- Ejecutar consultas SQL (solo SELECT) contra la base `stg_universidad` o `dw_universidad` y ver resultados.

Carpeta:
- `app.py` : aplicación Flask
- `templates/index.html` : HTML principal con las dos pestañas (Dashboard y SQL Explorer)
- `static/` : carpeta para assets (vacía en esta versión)

Requisitos
----------
Python 3.8+
Paquetes (puedes instalarlos con pip):

```
pip install flask sqlalchemy pymysql python-dotenv pandas plotly
```

Nota: `plotly` en el frontend se carga por CDN para graficar; la librería Python no es estrictamente necesaria para la parte visual.

Variables de entorno
--------------------
La aplicación lee conexión a bases desde el `.env` (mismo que usan los otros scripts):

- DB_USER
- DB_PASSWORD
- DB_HOST
- DB_PORT
- STG_DATABASE
- DWH_DATABASE

Asegurate de tener un `.env` con esas variables o que estén en el entorno antes de arrancar.

Cómo ejecutar
-------------
1. Posicionate en el directorio `TP2/4-Web_App`.
2. Ejecutá:

```
python3 app.py
```

3. Abrí tu navegador en `http://127.0.0.1:5000`

Pestañas de la aplicación
-------------------------
- Dashboard: elegir base (stg/dwh), tabla, columnas X/Y y tipo de gráfico. Pulsar "Render" para ver el gráfico. Abajo verás un preview con hasta 50 filas.

- SQL Explorer: escribir una consulta SELECT (solo SELECT está permitida aquí por seguridad) y elegir la base. Al ejecutar verás la tabla de resultados.

Seguridad
---------
- La API de SQL solo permite `SELECT` para evitar modificaciones peligrosas desde la interfaz.
- Aun así, esta app está pensada para uso local y diagnóstico; no la expongas en producción sin agregar autenticación y controles adicionales.

Limitaciones y notas
--------------------
- Las consultas grandes están limitadas por el `LIMIT` agregado si el usuario no especifica uno.
- Los nombres de tablas usados por defecto en la UI están en la lista de `STG_TABLES` y `DWH_TABLES` del `app.py`. Si cambias las tablas en la BD, actualizá esas listas si hiciera falta.

Expansiones posibles
--------------------
- Autenticación y roles (lectura/ejecución SQL restringida)
- Guardado de consultas frecuentes
- Más opciones de visualización (heatmaps, series temporales, etc.)
- Paginado y descarga de resultados (CSV/Excel)
