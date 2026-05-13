#!/usr/bin/env python
# coding: utf-8

"""
Orquestador ETL - Carga Inicial Completa

Ejecuta el pipeline completo de carga inicial en un solo comando:
  1. Carga Staging  (CSV → stg_universidad)
  2. Transformación  (stg_universidad → dw_universidad)

Uso:
  python orquestador.py

El proceso es idempotente: puede re-ejecutarse sin efectos colaterales.
Cada paso hace TRUNCATE de sus tablas destino antes de cargar.
"""

# import os
import sys
import time
from pathlib import Path

# Asegurar que el directorio de ETL_CargaInicial esté en el path
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(SCRIPT_DIR.parent))


def main():
    print("=" * 70)
    print("  ORQUESTADOR ETL - CARGA INICIAL COMPLETA")
    print("=" * 70)

    inicio_total = time.time()

    # ── PASO 1: Carga Staging ──────────────────────────────────────────
    print("\n" + "-" * 70)
    print("  PASO 1/2: Carga CSV -> Staging")
    print("-" * 70)

    inicio_stg = time.time()

    from carga_staging import ejecutar_carga_staging

    resultados_stg = ejecutar_carga_staging()

    fallidos = sum(1 for v in resultados_stg.values() if not v)
    if fallidos > 0:
        print(f"\n[ERROR] {fallidos} archivos fallaron en la carga a staging.")
        print("Abortando pipeline. Corrija los errores e intente de nuevo.")
        sys.exit(1)

    tiempo_stg = time.time() - inicio_stg
    print(f"\n  Staging completado en {tiempo_stg:.1f}s")

    # ── PASO 2: Transformación → DWH ──────────────────────────────────
    print("\n" + "-" * 70)
    print("  PASO 2/2: Transformacion STG -> DWH")
    print("-" * 70)

    inicio_dwh = time.time()

    from transformacion import ejecutar_transformacion

    reporte = ejecutar_transformacion()

    tiempo_dwh = time.time() - inicio_dwh
    print(f"\n  Transformacion completada en {tiempo_dwh:.1f}s")

    # ── RESUMEN FINAL ─────────────────────────────────────────────────
    tiempo_total = time.time() - inicio_total
    print("\n" + "=" * 70)
    print("  RESUMEN FINAL")
    print("=" * 70)
    print(f"  Staging :  {tiempo_stg:.1f}s")
    print(f"  DWH     :  {tiempo_dwh:.1f}s")
    print(f"  Total   :  {tiempo_total:.1f}s")
    print("=" * 70)
    print("  [OK] Pipeline de carga inicial completado exitosamente")
    print("=" * 70)


if __name__ == "__main__":
    main()
