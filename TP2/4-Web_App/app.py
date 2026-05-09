#!/usr/bin/env python3
# coding: utf-8

import os

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

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

app = Flask(__name__, template_folder="templates", static_folder="static")

# Hardcoded lists (keeps in sync with transformacion.py names)
STG_TABLES = [
    "stg_facultad",
    "stg_departamento",
    "stg_programa",
    "stg_curso",
    "stg_curso_programa",
    "stg_docente",
    "stg_estudiante",
    "stg_dictado",
    "stg_inscripcion",
    "stg_examen",
    "stg_evaluacion_curso",
]
DWH_TABLES = [
    "dim_tiempo",
    "dim_dictado",
    "dim_alumno",
    "fact_inscripcion",
    "fact_examen_alumno",
    "fact_evaluacion_dictado",
]


# ---------- Routes ----------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/tables")
def api_tables():
    return jsonify({"stg": STG_TABLES, "dwh": DWH_TABLES})


@app.route("/api/schema")
def api_schema():
    db = request.args.get("db", "stg")
    table = request.args.get("table")
    if not table:
        return jsonify({"error": "table parameter required"}), 400
    engine = engine_stg if db == "stg" else engine_dwh
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SHOW COLUMNS FROM {table}"))
            cols = [{"Field": r[0], "Type": r[1]} for r in result.fetchall()]
        return jsonify({"schema": cols})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/data")
def api_data():
    db = request.args.get("db", "stg")
    table = request.args.get("table")
    limit = int(request.args.get("limit", 100))
    if table is None:
        return jsonify({"error": "table parameter required"}), 400
    engine = engine_stg if db == "stg" else engine_dwh
    try:
        with engine.connect() as conn:
            q = text(f"SELECT * FROM {table} LIMIT :lim")
            res = conn.execute(q, {"lim": limit})
            rows = [dict(r) for r in res.mappings().all()]
        return jsonify({"rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
    engine = engine_stg if db == "stg" else engine_dwh
    try:
        # Select x and y (if y none, return counts)
        if y:
            q = text(f"SELECT `{x}` AS x, `{y}` AS y FROM {table} LIMIT :lim")
            with engine.connect() as conn:
                df = conn.execute(q, {"lim": limit}).mappings().all()
            rows = [dict(r) for r in df]
        else:
            q = text(
                f"SELECT `{x}` AS x, COUNT(*) AS y FROM {table} GROUP BY `{x}` ORDER BY y DESC LIMIT :lim"
            )
            with engine.connect() as conn:
                df = conn.execute(q, {"lim": limit}).mappings().all()
            rows = [dict(r) for r in df]
        return jsonify({"chart": {"type": chart, "data": rows}})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sql", methods=["POST"])
def api_sql():
    payload = request.json or {}
    db = payload.get("db", "stg")
    query = payload.get("query", "").strip()
    limit = int(payload.get("limit", 1000))
    if not query:
        return jsonify({"error": "query required"}), 400

    # Simple safety: only allow SELECT queries in this interface
    qlow = query.lower()
    if not qlow.startswith("select"):
        return jsonify(
            {"error": "Only SELECT statements are permitted via this tool"}
        ), 403

    engine = engine_stg if db == "stg" else engine_dwh
    try:
        # add a limit if not present to avoid huge results
        # naive approach: if 'limit' not in query, append
        if "limit" not in qlow:
            query_to_run = query.rstrip(";") + f" LIMIT {limit}"
        else:
            query_to_run = query
        with engine.connect() as conn:
            res = conn.execute(text(query_to_run))
            rows = [dict(r) for r in res.mappings().all()]
        return jsonify({"rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
