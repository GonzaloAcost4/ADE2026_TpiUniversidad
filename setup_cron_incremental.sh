#!/usr/bin/env bash
set -euo pipefail

# setup_cron_incremental.sh
# Crea un runner y agrega una entrada en crontab para ejecutar
# TP2/3-ETL_Incremental/carga_incremental.py diariamente a las 22:00.

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUNNER="$ROOT_DIR/run_incremental.sh"
LOGDIR="$ROOT_DIR/TP2/3-ETL_Incremental/logs"
CRON_MARKER="# ETL_INCREMENTAL_CRON - Managed by setup_cron_incremental.sh"
CRON_EXPR="0 22 * * *"

mkdir -p "$LOGDIR"

# Crear el runner que carga entorno y ejecuta el script Python
cat > "$RUNNER" <<'SH'
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
SH

chmod +x "$RUNNER"

# Instalar crontab: añadir entrada si no existe
existing_cron="$(crontab -l 2>/dev/null || true)"
if echo "$existing_cron" | grep -Fq "$CRON_MARKER"; then
  echo "[OK] Entrada de cron ya existe. No se realiza ningún cambio."
  exit 0
fi

# Construir nueva crontab añadiendo el bloque gestionado
new_cron="$existing_cron

$CRON_MARKER
$CRON_EXPR $RUNNER
# $CRON_MARKER
"

# Instalar la nueva crontab
printf "%s\n" "$new_cron" | crontab -

echo "[OK] Crontab instalado. El job ejecutará $RUNNER diariamente a las 22:00."
echo "Logs en: $LOGDIR"

exit 0
