#!/usr/bin/env bash
set -euo pipefail

# Ejecuta la carga incremental desde el directorio del proyecto
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# Si existe .env, exportar variables al entorno
if [ -f "$ROOT/.env" ]; then
  # shellcheck disable=SC1090
  set -a
  . "$ROOT/.env"
  set +a
fi

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOGFILE="$ROOT/TP2/3-ETL_Incremental/logs/incremental_${TIMESTAMP}.log"

# Ejecutar con python3 del entorno
/usr/bin/env python3 "$ROOT/TP2/3-ETL_Incremental/carga_incremental.py" >> "$LOGFILE" 2>&1
