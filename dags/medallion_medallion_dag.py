"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

# --- CONFIGURACION DE RUTA AUTOMATICA ---
# Usamos __file__ para que no haya errores de comillas manuales
BASE_DIR = Path(__file__).resolve().parents[1]

if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import clean_daily_transactions

# Definimos carpetas
RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"


def _build_env(ds_nodash: str) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(PROFILES_DIR),
            "CLEAN_DIR": str(CLEAN_DIR),
            "DS_NODASH": ds_nodash,
            "DUCKDB_PATH": str(WAREHOUSE_PATH),
        }
    )
    return env


def _run_dbt_command(command: str, ds_nodash: str) -> subprocess.CompletedProcess:
    env = _build_env(ds_nodash)
    print(f"Running dbt command: {command}")
    return subprocess.run(
        ["dbt", *command.split(), "--project-dir", str(DBT_DIR)],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


# --- TASKS ---


def task_bronze_clean(ds_nodash: str, **kwargs):
    execution_date = datetime.strptime(ds_nodash, "%Y%m%d").date()
    print(f"Iniciando limpieza para fecha: {execution_date}")
    output_path = clean_daily_transactions(
        execution_date=execution_date, raw_dir=RAW_DIR, clean_dir=CLEAN_DIR
    )
    print(f"Archivo limpio generado en: {output_path}")


def task_silver_dbt_run(ds_nodash: str, **kwargs):
    print("Ejecutando dbt run...")
    result = _run_dbt_command("run", ds_nodash)
    print("STDOUT:", result.stdout)
    if result.returncode != 0:
        raise AirflowException(f"dbt run falló: {result.stderr}")


def task_gold_dbt_test(ds_nodash: str, **kwargs):
    print("Ejecutando dbt test...")
    result = _run_dbt_command("test", ds_nodash)
    status = "passed" if result.returncode == 0 else "failed"

    output_data = {
        "ds_nodash": ds_nodash,
        "status": status,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }

    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    output_file = QUALITY_DIR / f"dq_results_{ds_nodash}.json"

    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=4)

    print(f"Resultados guardados en: {output_file}")
    if result.returncode != 0:
        raise AirflowException(f"dbt test falló. Ver detalles en {output_file}")


# --- DAG ---


def build_dag() -> DAG:
    with DAG(
        description="Bronze/Silver/Gold medallion demo",
        dag_id="medallion_pipeline",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 11, 30, tz="UTC"),
        catchup=False,
        max_active_runs=1,
    ) as medallion_dag:

        bronze_task = PythonOperator(
            task_id="bronze_clean",
            python_callable=task_bronze_clean,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        silver_task = PythonOperator(
            task_id="silver_dbt_run",
            python_callable=task_silver_dbt_run,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        gold_task = PythonOperator(
            task_id="gold_dbt_tests",
            python_callable=task_gold_dbt_test,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        bronze_task >> silver_task >> gold_task

    return medallion_dag


dag = build_dag()
