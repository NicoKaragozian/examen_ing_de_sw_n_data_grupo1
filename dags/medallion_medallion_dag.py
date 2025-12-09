"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import logging
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator

# pylint: disable=import-error,wrong-import-position

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import (  # pylint: disable=wrong-import-position
    clean_daily_transactions,
)

RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"

logger = logging.getLogger(__name__)


def _build_env(ds_nodash: str) -> dict[str, str]:
    """Build environment variables needed by dbt commands."""
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
    """Execute a dbt command and return the completed process."""
    env = _build_env(ds_nodash)
    return subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            str(DBT_DIR),
        ],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


# =========================
#  Callables de cada capa
# =========================


def _bronze_clean_task(ds_nodash: str, **_context) -> None:
    """
    Capa Bronze:
    - Lee el CSV del día desde data/raw
    - Aplica limpieza con pandas
    - Escribe parquet en data/clean/transactions_<ds_nodash>_clean.parquet
    """
    # Reconstruimos la fecha a partir de ds_nodash (YYYYMMDD)
    execution_date = pendulum.from_format(ds_nodash, "YYYYMMDD")

    try:
        # clean_daily_transactions espera primero la fecha, luego los paths
        clean_daily_transactions(
            execution_date,
            RAW_DIR,
            CLEAN_DIR,
        )
    except FileNotFoundError as exc:
        # Nice to have: si no hay archivo para ese día, saltar la task
        logger.warning("No raw file found for %s: %s", execution_date, exc)
        raise AirflowSkipException(
            f"No raw data available for {execution_date.date()}, skipping bronze step."
        ) from exc


def _silver_dbt_run_task(ds_nodash: str, **_context) -> None:
    """
    Capa Silver:
    - Ejecuta `dbt run` usando el proyecto en dbt/
    - Carga la info limpia en DuckDB
    """
    result = _run_dbt_command("run", ds_nodash)
    if result.returncode != 0:
        raise AirflowException(
            f"dbt run failed with code {result.returncode}: {result.stderr}"
        )


def _gold_dbt_tests_task(ds_nodash: str, **_context) -> None:
    """
    Capa Gold:
    - Ejecuta `dbt test`
    - Escribe un JSON de data quality en data/quality/dq_results_<ds_nodash>.json
      con status, stdout y stderr.
    - Si algún test falla, marca el task en error.
    """
    result = _run_dbt_command("test", ds_nodash)

    status = "passed" if result.returncode == 0 else "failed"
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    dq_path = QUALITY_DIR / f"dq_results_{ds_nodash}.json"

    payload = {
        "ds_nodash": ds_nodash,
        "status": status,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    dq_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if result.returncode != 0:
        # Dejamos el archivo igual pero marcamos el task como fallido
        raise AirflowException("dbt tests failed, see dq_results json and logs")


def build_dag() -> DAG:
    """Construct the medallion pipeline DAG with bronze/silver/gold tasks."""
    with DAG(
        description="Bronze/Silver/Gold medallion demo with pandas, dbt, and DuckDB",
        dag_id="medallion_pipeline",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=True,
        max_active_runs=1,
    ) as medallion_dag:

        bronze_clean = PythonOperator(
            task_id="bronze_clean",
            python_callable=_bronze_clean_task,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        silver_dbt_run = PythonOperator(
            task_id="silver_dbt_run",
            python_callable=_silver_dbt_run_task,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        gold_dbt_tests = PythonOperator(
            task_id="gold_dbt_tests",
            python_callable=_gold_dbt_tests_task,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        bronze_clean >> silver_dbt_run >> gold_dbt_tests

    return medallion_dag


dag = build_dag()
