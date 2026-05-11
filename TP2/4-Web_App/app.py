#!/usr/bin/env python3
# coding: utf-8

import os
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

BASE_DIR = Path(__file__).resolve().parent
TP2_DIR = BASE_DIR.parent
load_dotenv(TP2_DIR / ".env")
load_dotenv()

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
STG_DATABASE = os.getenv("STG_DATABASE")
DWH_DATABASE = os.getenv("DWH_DATABASE")

if not all([USER, PASSWORD, HOST, PORT, STG_DATABASE, DWH_DATABASE]):
    raise EnvironmentError("Faltan variables de entorno en .env para conectar a DB")

engine_stg = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{STG_DATABASE}",
    poolclass=NullPool,
)
engine_dwh = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DWH_DATABASE}",
    poolclass=NullPool,
)

DATABASES = {
    "stg": {"name": STG_DATABASE, "engine": engine_stg, "label": "Staging"},
    "dwh": {"name": DWH_DATABASE, "engine": engine_dwh, "label": "Data Warehouse"},
}

app = Flask(__name__, template_folder="templates", static_folder="static")


def get_database_config(db_key: str) -> Dict:
    if db_key not in DATABASES:
        raise ValueError(f"Base desconocida: {db_key}")
    return DATABASES[db_key]


def list_tables(db_key: str) -> List[str]:
    config = get_database_config(db_key)
    query = text(
        "SELECT table_name "
        "FROM information_schema.tables "
        "WHERE table_schema = :schema "
        "ORDER BY table_name"
    )
    with config["engine"].connect() as conn:
        rows = conn.execute(query, {"schema": config["name"]}).fetchall()
    return [row[0] for row in rows]


def query_returns_rows(sql: str) -> bool:
    statement = sql.strip().lstrip("(")
    if not statement:
        return False
    first_token = statement.split(None, 1)[0].lower()
    return first_token in {"select", "with", "show", "describe", "desc", "explain"}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/meta")
def api_meta():
    return jsonify(
        {
            "databases": {
                clave: {
                    "label": config["label"],
                    "schema": config["name"],
                    "tables": list_tables(clave),
                }
                for clave, config in DATABASES.items()
            }
        }
    )


@app.route("/api/tables")
def api_tables():
    return jsonify({clave: list_tables(clave) for clave in DATABASES})


@app.route("/api/schema")
def api_schema():
    db = request.args.get("db", "stg")
    table = request.args.get("table")
    if not table:
        return jsonify({"error": "table parameter required"}), 400

    config = get_database_config(db)
    try:
        with config["engine"].connect() as conn:
            result = conn.execute(text(f"SHOW COLUMNS FROM `{table}`"))
            cols = [
                {
                    "Field": row[0],
                    "Type": row[1],
                    "Null": row[2],
                    "Key": row[3],
                    "Default": row[4],
                    "Extra": row[5],
                }
                for row in result.fetchall()
            ]
        return jsonify({"schema": cols})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/data")
def api_data():
    db = request.args.get("db", "stg")
    table = request.args.get("table")
    limit = int(request.args.get("limit", 100))
    if table is None:
        return jsonify({"error": "table parameter required"}), 400

    config = get_database_config(db)
    try:
        with config["engine"].connect() as conn:
            query = text(f"SELECT * FROM `{table}` LIMIT :lim")
            result = conn.execute(query, {"lim": limit})
            rows = [dict(row) for row in result.mappings().all()]
        return jsonify({"rows": rows, "count": len(rows)})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/chart", methods=["POST"])
def api_chart():
    payload = request.json or {}
    db = payload.get("db", "stg")
    table = payload.get("table")
    x = payload.get("x")
    y = payload.get("y")
    chart = payload.get("chart", "bar")
    limit = int(payload.get("limit", 5000))

    if not table or not x:
        return jsonify({"error": "table and x are required"}), 400

    config = get_database_config(db)
    try:
        with config["engine"].connect() as conn:
            if y:
                query = text(f"SELECT `{x}` AS x, `{y}` AS y FROM `{table}` LIMIT :lim")
                rows = conn.execute(query, {"lim": limit}).mappings().all()
            else:
                query = text(
                    f"SELECT `{x}` AS x, COUNT(*) AS y "
                    f"FROM `{table}` "
                    f"GROUP BY `{x}` "
                    f"ORDER BY y DESC "
                    f"LIMIT :lim"
                )
                rows = conn.execute(query, {"lim": limit}).mappings().all()
        return jsonify({"chart": {"type": chart, "data": [dict(row) for row in rows]}})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/sql", methods=["POST"])
def api_sql():
    payload = request.json or {}
    db = payload.get("db", "stg")
    query = (payload.get("query") or "").strip()

    if not query:
        return jsonify({"error": "query required"}), 400

    config = get_database_config(db)

    try:
        with config["engine"].begin() as conn:
            result = conn.execute(text(query))
            if result.returns_rows or query_returns_rows(query):
                rows = [dict(row) for row in result.mappings().all()]
                columns = list(rows[0].keys()) if rows else []
                return jsonify(
                    {
                        "mode": "rows",
                        "columns": columns,
                        "rows": rows,
                        "row_count": len(rows),
                    }
                )

            return jsonify(
                {
                    "mode": "command",
                    "message": "Sentencia ejecutada correctamente",
                    "row_count": int(result.rowcount or 0),
                }
            )
    except SQLAlchemyError as exc:
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    app.run(debug=True)
