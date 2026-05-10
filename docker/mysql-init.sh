#!/bin/sh
set -e

echo "[init] Creando base de staging..."
mysql -uroot -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sql/CreacionSTG_Universidad.sql

echo "[init] Creando base de data warehouse..."
mysql -uroot -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sql/CreacionDWH_Universidad.sql

echo "[init] Bases creadas correctamente"
