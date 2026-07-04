"""
RetailFlow Pipeline — Main Airflow DAG
=======================================

Orchestrates the full retail data pipeline:

    1. generate_fake_data   — Generate synthetic retail CSVs
    2. validate_raw_data    — Great Expectations data quality check
    3. load_raw_to_postgres — Load CSVs into PostgreSQL raw schema
    4. run_dbt_staging      — dbt staging models
    5. run_dbt_intermediate — dbt intermediate models (enrichment)
    6. run_dbt_marts        — dbt mart models (dimensions + fact)
    7. run_dbt_tests        — dbt data quality tests
    8. notify_success       — Log pipeline summary

The DAG runs daily at 6:00 AM UTC by default.

Usage:
    Place this file in airflow/dags/.
    Airflow will automatically pick it up and show it in the UI.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# WHY: Import our custom hook for fetching row counts in the notify step.
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "plugins"))
from hooks.postgres_hook import RetailFlowPostgresHook  # noqa: E402

logger = logging.getLogger(__name__)

# --- Constants ---
# WHY: Store the project root so BashOperator commands are relative to it.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DBT_DIR = PROJECT_ROOT / "dbt"
PYTHON_BIN = ".venv/Scripts/python"  # Adjust if using a different venv path

default_args = {
    "owner": "retailflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": "admin@example.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="retailflow_pipeline",
    default_args=default_args,
    description="End-to-end retail data pipeline: generate → validate → load → transform → test",
    schedule_interval="0 6 * * *",  # Run daily at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["retail", "pipeline", "etl"],
    doc_md=__doc__,
)

# =============================================================================
# Task 1: Generate fake data
# =============================================================================
generate_fake_data = BashOperator(
    task_id="generate_fake_data",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"{PYTHON_BIN} {SCRIPTS_DIR / 'generate_fake_data.py'} "
        f"--customers {{{{ var.value.get('NUM_CUSTOMERS', '10000') }}}} "
        f"--products {{{{ var.value.get('NUM_PRODUCTS', '500') }}}} "
        f"--orders {{{{ var.value.get('NUM_ORDERS', '100000') }}}}"
    ),
    dag=dag,
)

# =============================================================================
# Task 2: Validate raw data with Great Expectations
# =============================================================================
validate_raw_data = BashOperator(
    task_id="validate_raw_data",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"great_expectations checkpoint run orders_checkpoint"
    ),
    dag=dag,
)

# =============================================================================
# Task 3: Load raw CSVs into PostgreSQL raw schema
# =============================================================================
load_raw_to_postgres = BashOperator(
    task_id="load_raw_to_postgres",
    bash_command=(
        f"cd {PROJECT_ROOT} && "
        f"{PYTHON_BIN} {SCRIPTS_DIR / 'load_to_postgres.py'}"
    ),
    dag=dag,
)

# =============================================================================
# Task 4: Run dbt staging models
# =============================================================================
run_dbt_staging = BashOperator(
    task_id="run_dbt_staging",
    bash_command=f"cd {DBT_DIR} && dbt run --select staging",
    dag=dag,
)

# =============================================================================
# Task 5: Run dbt intermediate models
# =============================================================================
run_dbt_intermediate = BashOperator(
    task_id="run_dbt_intermediate",
    bash_command=f"cd {DBT_DIR} && dbt run --select intermediate",
    dag=dag,
)

# =============================================================================
# Task 6: Run dbt mart models
# =============================================================================
run_dbt_marts = BashOperator(
    task_id="run_dbt_marts",
    bash_command=f"cd {DBT_DIR} && dbt run --select marts",
    dag=dag,
)

# =============================================================================
# Task 7: Run dbt data tests
# =============================================================================
run_dbt_tests = BashOperator(
    task_id="run_dbt_tests",
    bash_command=f"cd {DBT_DIR} && dbt test",
    dag=dag,
)

# =============================================================================
# Task 8: Notify success with pipeline summary
# =============================================================================
def notify_success_fn(**context) -> None:
    """Query row counts from each layer and log a summary.

    Args:
        context: Airflow task context (unused but required by PythonOperator).
    """
    hook = RetailFlowPostgresHook()
    summary = {}

    # WHY: Count rows in each layer to confirm data flowed through correctly.
    queries = {
        "raw.orders": "SELECT COUNT(*) FROM raw.orders",
        "raw.customers": "SELECT COUNT(*) FROM raw.customers",
        "raw.products": "SELECT COUNT(*) FROM raw.products",
        "staging.orders": "SELECT COUNT(*) FROM staging.stg_orders",
        "marts.dim_customers": "SELECT COUNT(*) FROM marts.dim_customers",
        "marts.dim_products": "SELECT COUNT(*) FROM marts.dim_products",
        "marts.fct_orders": "SELECT COUNT(*) FROM marts.fct_orders",
    }

    for label, query in queries.items():
        try:
            result = hook.fetch_all(query)
            summary[label] = result[0][0] if result else 0
        except Exception as exc:
            logger.warning("Could not query %s: %s", label, exc)
            summary[label] = "ERROR"

    # WHY: Log in a structured format so it appears clearly in Airflow logs.
    logger.info("=" * 60)
    logger.info("RETAILFLOW PIPELINE — EXECUTION SUMMARY")
    logger.info("=" * 60)
    for table, count in summary.items():
        logger.info("  %-30s → %s", table, count)
    logger.info("=" * 60)

    # Push summary to XCom for downstream tasks if needed
    context["task_instance"].xcom_push(key="pipeline_summary", value=summary)


notify_success = PythonOperator(
    task_id="notify_success",
    python_callable=notify_success_fn,
    provide_context=True,
    dag=dag,
)

# =============================================================================
# Task Dependencies (execution order)
# =============================================================================
# WHY: These dependencies enforce the exact order specified in the pipeline spec.
# Each task must succeed before the next one starts.

generate_fake_data >> validate_raw_data >> load_raw_to_postgres
load_raw_to_postgres >> run_dbt_staging >> run_dbt_intermediate >> run_dbt_marts
run_dbt_marts >> run_dbt_tests >> notify_success
