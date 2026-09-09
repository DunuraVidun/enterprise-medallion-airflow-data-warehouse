from pathlib import Path
import sys
from datetime import datetime, timedelta
import logging


# --------------------------------------------------------------------
# Project Path
# --------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


# --------------------------------------------------------------------
# Airflow Imports
# --------------------------------------------------------------------

from airflow.sdk import DAG, TriggerRule

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook


# --------------------------------------------------------------------
# Utility Imports
# --------------------------------------------------------------------

from utils.file import read_sql
from utils.audit import start_audit, finish_audit


# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------

TARGET_CONN = "abc_hub_olap"

SQL_DIR = BASE_DIR / "sql" / "gold" / "fact"

SILVER_DAG_ID = "03_silver_load_layer"

GOLD_DIMENSION_DAG_ID = "04_gold_dim_load_layer"


logger = logging.getLogger(__name__)


# ====================================================================
# TASK CALLBACKS
# ====================================================================

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


# ====================================================================
# COMMON SILVER SENSOR
# ====================================================================

def silver_sensor(
    task_id: str,
    silver_task_id: str,
):

    """
    Create a reusable sensor for waiting for a specific
    task in the Silver DAG.
    """

    return ExternalTaskSensor(

        task_id=task_id,

        external_dag_id=SILVER_DAG_ID,

        external_task_id=silver_task_id,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed",
            "skipped",
        ],

        mode="reschedule",

        poke_interval=30,

        timeout=60 * 60,
    )


# ====================================================================
# COMMON GOLD DIMENSION SENSOR
# ====================================================================

def gold_dimension_sensor(
    task_id: str,
    dimension_task_id: str,
):

    """
    Create a reusable sensor for waiting for a specific
    Gold dimension task.
    """

    return ExternalTaskSensor(

        task_id=task_id,

        external_dag_id=GOLD_DIMENSION_DAG_ID,

        external_task_id=dimension_task_id,

        allowed_states=[
            "success"
        ],

        failed_states=[
            "failed",
            "skipped",
        ],

        mode="reschedule",

        poke_interval=30,

        timeout=60 * 60,
    )


# ====================================================================
# GENERIC GOLD FACT LOADER
# ====================================================================

def load_gold_fact(
    sql_file: str,
):
    """
    Load a Gold fact table.

    Steps:
    1. Read the Gold fact SQL.
    2. Execute the upsert and capture insert/update outcome per row.
    3. Commit the transaction.
    4. Return statistics for the audit process.
    """

    logger.info(
        "Starting Gold fact load using %s",
        sql_file
    )

    hook = PostgresHook(
        postgres_conn_id=TARGET_CONN
    )

    conn = hook.get_conn()

    try:

        sql = read_sql(
            SQL_DIR / sql_file
        )

        # ------------------------------------------------------------
        # Execute fact load
        #
        # Each fact SQL ends with:
        #   RETURNING (xmax = 0) AS is_insert
        #
        # xmax = 0 means the row was freshly inserted;
        # otherwise it went through ON CONFLICT DO UPDATE.
        # ------------------------------------------------------------

        with conn.cursor() as cursor:

            cursor.execute(sql)

            rows = cursor.fetchall()

        conn.commit()

        processed_count = len(rows)

        inserted_count = sum(
            1
            for row in rows
            if row[0]
        )

        updated_count = processed_count - inserted_count

        logger.info(
            "Gold fact load complete for %s | "
            "processed=%d inserted=%d updated=%d",
            sql_file,
            processed_count,
            inserted_count,
            updated_count,
        )

        return {

            "source_record_count": processed_count,

            "processed_record_count": processed_count,

            "inserted_record_count": inserted_count,

            "updated_record_count": updated_count,

            "rejected_record_count": 0,

        }


    except Exception:

        conn.rollback()

        logger.exception(
            "Gold fact load failed for %s",
            sql_file
        )

        raise


    finally:

        conn.close()


# ====================================================================
# DEFAULT ARGUMENTS
# ====================================================================

default_args = {

    "owner": "abc_hub",

    "retries": 1,

    "retry_delay": timedelta(minutes=2),

    "on_success_callback": task_success,

    "on_failure_callback": task_failure,
}


# ====================================================================
# GOLD FACT DAG
# ====================================================================

with DAG(

    dag_id="05_gold_fact_load_layer",

    description="Load Gold Fact Tables for Analytical Reporting",

    default_args=default_args,

    schedule="@daily",

    start_date=datetime(
        2026,
        1,
        1
    ),

    catchup=False,

    max_active_runs=1,

    max_active_tasks=6,

    tags=[
        "gold",
        "facts",
        "analytical",
        "medallion",
    ],

) as dag:


    # ================================================================
    # START
    # ================================================================

    start = EmptyOperator(
        task_id="start"
    )


    # ================================================================
    # START AUDIT
    # ================================================================

    audit_start = PythonOperator(

        task_id="start_audit",

        python_callable=start_audit,
    )


    # ================================================================
    # CUSTOMER DAILY ACTIVITY
    # ================================================================

    wait_customer_streaming = silver_sensor(

        task_id="wait_for_silver_streaming_session",

        silver_task_id="load_silver_streaming_session",
    )


    wait_customer_rental = silver_sensor(

        task_id="wait_for_silver_rental",

        silver_task_id="load_silver_rental",
    )


    wait_customer_payment = silver_sensor(

        task_id="wait_for_silver_payment",

        silver_task_id="load_silver_payment",
    )


    wait_customer_support = silver_sensor(

        task_id="wait_for_silver_support_ticket",

        silver_task_id="load_silver_support_ticket",
    )


    # ---------------------------------------------------------------
    # Customer Dimension
    # ---------------------------------------------------------------

    wait_customer_dimension = gold_dimension_sensor(

        task_id="wait_for_gold_dim_customer",

        dimension_task_id="load_dim_customer",
    )


    # ---------------------------------------------------------------
    # Customer Fact
    # ---------------------------------------------------------------

    load_customer_daily_activity = PythonOperator(

        task_id="load_fact_customer_daily_activity",

        python_callable=load_gold_fact,

        op_kwargs={

            "sql_file":
                "fact_customer_daily_activity.sql",

        },

        execution_timeout=timedelta(
            minutes=30
        ),
    )


    # ================================================================
    # CONTENT MONTHLY PERFORMANCE
    # ================================================================

    wait_content_streaming = silver_sensor(

        task_id="wait_for_silver_streaming_for_content",

        silver_task_id="load_silver_streaming_session",
    )


    wait_content_rental = silver_sensor(

        task_id="wait_for_silver_rental_for_content",

        silver_task_id="load_silver_rental",
    )


    wait_content_payment = silver_sensor(

        task_id="wait_for_silver_payment_for_content",

        silver_task_id="load_silver_payment",
    )


    wait_content_review = silver_sensor(

        task_id="wait_for_silver_review",

        silver_task_id="load_silver_review",
    )


    wait_content_wishlist = silver_sensor(

        task_id="wait_for_silver_wishlist",

        silver_task_id="load_silver_wishlist",
    )


    # ---------------------------------------------------------------
    # Content Dimension
    # ---------------------------------------------------------------

    wait_content_dimension = gold_dimension_sensor(

        task_id="wait_for_gold_dim_content",

        dimension_task_id="load_dim_content",
    )


    # ---------------------------------------------------------------
    # Content Fact
    # ---------------------------------------------------------------

    load_content_monthly_performance = PythonOperator(

        task_id="load_fact_content_monthly_performance",

        python_callable=load_gold_fact,

        op_kwargs={

            "sql_file":
                "fact_content_monthly_performance.sql",

        },

        execution_timeout=timedelta(
            minutes=30
        ),
    )


    # ================================================================
    # INVENTORY DAILY UTILIZATION
    # ================================================================

    wait_inventory_rental = silver_sensor(

        task_id="wait_for_silver_rental_for_inventory",

        silver_task_id="load_silver_rental",
    )


    # ---------------------------------------------------------------
    # Inventory Dimension
    # ---------------------------------------------------------------

    wait_inventory_dimension = gold_dimension_sensor(

        task_id="wait_for_gold_dim_inventory_item",

        dimension_task_id="load_dim_inventory_item",
    )


    # ---------------------------------------------------------------
    # Inventory Fact
    # ---------------------------------------------------------------

    load_inventory_daily_utilization = PythonOperator(

        task_id="load_fact_inventory_daily_utilization",

        python_callable=load_gold_fact,

        op_kwargs={

            "sql_file":
                "fact_inventory_daily_utilization.sql",

        },

        execution_timeout=timedelta(
            minutes=30
        ),
    )


    # ================================================================
    # FINISH AUDIT
    # ================================================================

    audit_finish = PythonOperator(

        task_id="finish_audit",

        python_callable=finish_audit,

        trigger_rule=TriggerRule.ALL_DONE,

        op_kwargs={

            "task_ids": [

                "load_fact_customer_daily_activity",

                "load_fact_content_monthly_performance",

                "load_fact_inventory_daily_utilization",

            ]

        },

    )


    # ================================================================
    # END
    # ================================================================

    end = EmptyOperator(

        task_id="end",

        trigger_rule=TriggerRule.ALL_DONE,

    )


    # ================================================================
    # MAIN DEPENDENCIES
    # ================================================================

    start >> audit_start


    # ================================================================
    # CUSTOMER FACT DEPENDENCIES
    # ================================================================

    audit_start >> [

        wait_customer_streaming,

        wait_customer_rental,

        wait_customer_payment,

        wait_customer_support,

        wait_customer_dimension,

    ]


    [

        wait_customer_streaming,

        wait_customer_rental,

        wait_customer_payment,

        wait_customer_support,

        wait_customer_dimension,

    ] >> load_customer_daily_activity


    # ================================================================
    # CONTENT FACT DEPENDENCIES
    # ================================================================

    audit_start >> [

        wait_content_streaming,

        wait_content_rental,

        wait_content_payment,

        wait_content_review,

        wait_content_wishlist,

        wait_content_dimension,

    ]


    [

        wait_content_streaming,

        wait_content_rental,

        wait_content_payment,

        wait_content_review,

        wait_content_wishlist,

        wait_content_dimension,

    ] >> load_content_monthly_performance


    # ================================================================
    # INVENTORY FACT DEPENDENCIES
    # ================================================================

    audit_start >> [

        wait_inventory_rental,

        wait_inventory_dimension,

    ]


    [

        wait_inventory_rental,

        wait_inventory_dimension,

    ] >> load_inventory_daily_utilization


    # ================================================================
    # ALL FACTS → AUDIT FINISH
    # ================================================================

    [

        load_customer_daily_activity,

        load_content_monthly_performance,

        load_inventory_daily_utilization,

    ] >> audit_finish


    audit_finish >> end