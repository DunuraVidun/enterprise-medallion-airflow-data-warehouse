from pathlib import Path
import sys
from datetime import datetime, timedelta
import logging

# --------------------------------------------------------------------
# Project Path
# --------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TriggerRule

from utils.database import extract_data, truncate_table, load_data
from utils.file import read_sql
from utils.audit import start_audit, finish_audit

# --------------------------------------------------------------------
# Connections
# --------------------------------------------------------------------

SOURCE_CONN = "abc_hub_oltp"
TARGET_CONN = "abc_hub_olap"

SQL_DIR = BASE_DIR / "sql" / "bronze" / "full_load"

# --------------------------------------------------------------------
# Table Configuration
# --------------------------------------------------------------------

BRONZE_TABLES = [
    ("customer.sql", "bronze.customer"),
    ("country.sql", "bronze.country"),
    ("city.sql", "bronze.city"),
    ("customer_address.sql", "bronze.customer_address"),
    ("content_type.sql", "bronze.content_type"),
    ("content.sql", "bronze.content"),
    ("genre.sql", "bronze.genre"),
    ("content_genre.sql", "bronze.content_genre"),
    ("streaming_session.sql", "bronze.streaming_session"),
    ("review.sql", "bronze.review"),
    ("wishlist.sql", "bronze.wishlist"),
    ("warehouse.sql", "bronze.warehouse"),
    ("inventory_item.sql", "bronze.inventory_item"),
    ("rental.sql", "bronze.rental"),
    ("payment.sql", "bronze.payment"),
    ("support_ticket.sql", "bronze.support_ticket"),
]

# --------------------------------------------------------------------
# Generic Bronze Loader
# --------------------------------------------------------------------

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------
# Task Callbacks
# --------------------------------------------------------------------
def task_success(context):
    logger.info(
        "Task %s in DAG %s completed successfully for run %s.",
        context["task_instance"].task_id,
        context["dag"].dag_id,
        context["run_id"],
    )


def task_failure(context):
    logger.error(
        "Task %s in DAG %s failed for run %s.",
        context["task_instance"].task_id,
        context["dag"].dag_id,
        context["run_id"],
    )


# --------------------------------------------------------------------
# Generic Bronze Loader Function
# --------------------------------------------------------------------

def load_bronze_table(sql_file: str, target_table: str):

    """
    Perform a full load from ABC_HUB_OLTP source table into a Bronze table.

    Steps:
    1. Read SQL query.
    2. Extract data from OLTP.
    3. Skip if no records.
    4. Truncate target Bronze table.
    5. Load fresh data.
    """

    try:
        logger.info("Starting load for %s using %s", target_table, sql_file)

        sql = read_sql(SQL_DIR / sql_file)

        rows, columns = extract_data(
            conn_id=SOURCE_CONN,
            sql=sql,
        )

        logger.info("Extracted %d rows from %s", len(rows), sql_file)

        if not rows:
            logger.warning("No data found for %s", target_table)
            return 0

        truncate_table(
            conn_id=TARGET_CONN,
            table_name=target_table,
        )

        load_data(
            conn_id=TARGET_CONN,
            table_name=target_table,
            columns=columns,
            rows=rows,
        )

        logger.info("Loaded %d rows into %s", len(rows), target_table)
        return len(rows)

    except Exception:
        logger.exception("Failed loading %s", target_table)
        raise

# --------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------
default_args = {
    "owner": "abc_hub",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_success_callback": task_success,
    "on_failure_callback": task_failure,
}

with DAG(
    dag_id="01_full_load_bronze",
    description="Full Load: OLTP → Bronze Layer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["bronze", "full-load", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")

    audit_start = PythonOperator(
        task_id="start_audit",
        python_callable=start_audit,
    )

    audit_finish = PythonOperator(
        task_id="finish_audit",
        python_callable=finish_audit,
        trigger_rule=TriggerRule.ALL_DONE,
        op_kwargs={
            "task_ids": [f"load_{t.split('.')[-1]}" for _, t in BRONZE_TABLES]
        },
    )

    end = EmptyOperator(task_id="end")

    tasks = []

    for sql_file, table in BRONZE_TABLES:

        task = PythonOperator(
            task_id=f"load_{table.split('.')[-1]}",
            python_callable=load_bronze_table,
            execution_timeout=timedelta(minutes=10),
            op_kwargs={
                "sql_file": sql_file,
                "target_table": table,
            },
        )

        tasks.append(task)

    start >> audit_start >> tasks >> audit_finish >> end