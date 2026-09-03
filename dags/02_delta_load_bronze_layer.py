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

from utils.database import extract_data, load_data
from utils.file import read_sql
from utils.audit import start_audit, finish_audit

# --------------------------------------------------------------------
# Connections
# --------------------------------------------------------------------

SOURCE_CONN = "abc_hub_oltp"
TARGET_CONN = "abc_hub_olap"

SQL_DIR = BASE_DIR / "sql" / "bronze" / "delta_load"

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
# Logging
# --------------------------------------------------------------------

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Task Callbacks
# --------------------------------------------------------------------

def task_success(context):
    logger.info(
        "Task %s completed successfully for run %s.",
        context["task_instance"].task_id,
        context["run_id"],
    )


def task_failure(context):
    logger.error(
        "Task %s failed for run %s.",
        context["task_instance"].task_id,
        context["run_id"],
    )

# --------------------------------------------------------------------
# Get Last Watermark
# --------------------------------------------------------------------

def get_last_watermark(target_table: str):

    sql = f"""
        SELECT COALESCE(
            GREATEST(
                MAX(created_at),
                MAX(updated_at)
            ),
            TIMESTAMP '1900-01-01'
        ) AS last_watermark
        FROM {target_table};
    """

    rows, _ = extract_data(
        conn_id=TARGET_CONN,
        sql=sql,
    )

    watermark = rows[0][0]

    logger.info(
        "Last watermark for %s: %s",
        target_table,
        watermark,
    )

    return watermark

# --------------------------------------------------------------------
# Generic Delta Loader
# --------------------------------------------------------------------

def load_delta_table(
    sql_file: str,
    target_table: str,
    processing_date: str,
):
    try:

        logger.info(
            "Starting delta load for %s | Processing date: %s",
            target_table,
            processing_date,
        )

        last_watermark = get_last_watermark(target_table)

        sql = read_sql(SQL_DIR / sql_file)

        sql = (
            sql.replace(
                "{{ last_watermark }}",
                str(last_watermark),
            )
            .replace(
                "{{ processing_date }}",
                processing_date,
            )
        )

        rows, columns = extract_data(
            conn_id=SOURCE_CONN,
            sql=sql,
        )

        if not rows:
            logger.info("No new data for %s", target_table)
            return 0

        load_data(
            conn_id=TARGET_CONN,
            table_name=target_table,
            columns=columns,
            rows=rows,
        )

        logger.info(
            "Loaded %d rows into %s",
            len(rows),
            target_table,
        )

        return len(rows)

    except Exception:
        logger.exception("Delta load failed for %s", target_table)
        raise

# --------------------------------------------------------------------
# DAG Configuration
# --------------------------------------------------------------------

default_args = {
    "owner": "abc_hub",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_success_callback": task_success,
    "on_failure_callback": task_failure,
}

# --------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------

with DAG(
    dag_id="02_delta_load_bronze",
    description="Daily Delta Load: OLTP → Bronze Layer",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["bronze", "delta-load", "medallion"],
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
            "task_ids": [f"delta_{t.split('.')[-1]}" for _, t in BRONZE_TABLES]
        },
    )

    end = EmptyOperator(task_id="end")

    tasks = []

    for sql_file, table in BRONZE_TABLES:

        task = PythonOperator(
            task_id=f"delta_{table.split('.')[-1]}",
            python_callable=load_delta_table,
            execution_timeout=timedelta(minutes=10),
            op_kwargs={
                "sql_file": sql_file,
                "target_table": table,
                "processing_date": "{{ data_interval_end }}",
            },
        )

        tasks.append(task)

    start >> audit_start >> tasks >> audit_finish >> end