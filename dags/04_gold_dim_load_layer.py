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

SQL_DIR = BASE_DIR / "sql" / "gold"

SILVER_DAG_ID = "03_silver_load_layer"


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
# GENERIC GOLD DIMENSION LOADER
# ====================================================================

def load_gold_dimension(
    sql_file: str,
    source_table: str,
):

    """
    Load a Gold dimension using the corresponding SQL file.

    Steps:
    1. Read the Gold SQL file.
    2. Get source record count from Silver profile.
    3. Execute the Gold SCD Type 2 SQL.
    4. Commit the transaction.
    5. Return audit statistics.
    """

    logger.info(
        "Starting Gold load using %s",
        sql_file
    )

    hook = PostgresHook(
        postgres_conn_id=TARGET_CONN
    )

    conn = hook.get_conn()

    try:

        # ------------------------------------------------------------
        # Source record count
        # ------------------------------------------------------------

        with conn.cursor() as cursor:

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {source_table}
                """
            )

            result = cursor.fetchone()

            source_count = result[0] if result else 0


        logger.info(
            "Source record count for %s: %d",
            source_table,
            source_count
        )


        # ------------------------------------------------------------
        # Read Gold SQL
        # ------------------------------------------------------------

        sql = read_sql(
            SQL_DIR / sql_file
        )


        # ------------------------------------------------------------
        # Execute Gold SCD Type 2 logic
        # ------------------------------------------------------------

        with conn.cursor() as cursor:

            cursor.execute(sql)


        # ------------------------------------------------------------
        # Commit
        # ------------------------------------------------------------

        conn.commit()


        logger.info(
            "Successfully processed Gold dimension: %s",
            sql_file
        )


        # ------------------------------------------------------------
        # Return audit information
        #
        # The Gold SQL performs both UPDATE and INSERT operations.
        # The current audit utility can consume this dictionary.
        #
        # processed_count represents the Silver profile records
        # considered by the Gold load.
        # ------------------------------------------------------------

        return {
            "source_record_count": source_count,

            "processed_record_count": source_count,

            "inserted_record_count": 0,

            "updated_record_count": 0,

            "rejected_record_count": 0,
        }


    except Exception:

        conn.rollback()

        logger.exception(
            "Gold load failed for %s",
            sql_file
        )

        raise


    finally:

        conn.close()


# ====================================================================
# COMMON SILVER SENSOR
# ====================================================================

def silver_sensor(
    task_id: str,
    silver_task_id: str,
):

    """
    Create a reusable sensor that waits for a specific
    task in the Silver DAG to complete successfully.
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
# DEFAULT ARGUMENTS
# ====================================================================

default_args = {

    "owner": "abc_hub",

    "retries": 1,

    "retry_delay": timedelta(
        minutes=2
    ),

    "on_success_callback": task_success,

    "on_failure_callback": task_failure,
}


# ====================================================================
# GOLD DIMENSION DAG
# ====================================================================

with DAG(

    dag_id="04_gold_dim_load_layer",

    description="Load Gold Dimension Tables using SCD Type 2",

    default_args=default_args,

    schedule="@daily",

    start_date=datetime(
        2026,
        1,
        1
    ),

    catchup=False,

    max_active_runs=1,

    max_active_tasks=4,

    tags=[
        "gold",
        "dimensions",
        "scd",
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
    # CUSTOMER
    # ================================================================

    wait_for_silver_customer_profile = silver_sensor(

        task_id="wait_for_silver_customer_profile",

        silver_task_id="load_silver_customer_profile",
    )


    load_customer = PythonOperator(

        task_id="load_dim_customer",

        python_callable=load_gold_dimension,

        op_kwargs={

            "sql_file":
                "dimension/dim_customer.sql",

            "source_table":
                "silver.customer_profile",
        },
    )


    wait_for_silver_customer_profile >> load_customer


    # ================================================================
    # CONTENT
    # ================================================================

    wait_for_silver_content_profile = silver_sensor(

        task_id="wait_for_silver_content_profile",

        silver_task_id="load_silver_content_profile",
    )


    load_content = PythonOperator(

        task_id="load_dim_content",

        python_callable=load_gold_dimension,

        op_kwargs={

            "sql_file":
                "dimension/dim_content.sql",

            "source_table":
                "silver.content_profile",
        },
    )


    wait_for_silver_content_profile >> load_content


    # ================================================================
    # INVENTORY ITEM
    # ================================================================

    wait_for_silver_inventory_item_profile = silver_sensor(

        task_id="wait_for_silver_inventory_item_profile",

        silver_task_id="load_silver_inventory_item_profile",
    )


    load_inventory_item = PythonOperator(

        task_id="load_dim_inventory_item",

        python_callable=load_gold_dimension,

        op_kwargs={

            "sql_file":
                "dimension/dim_inventory_item.sql",

            "source_table":
                "silver.inventory_item_profile",
        },
    )


    wait_for_silver_inventory_item_profile >> load_inventory_item


    # ================================================================
    # FINISH AUDIT
    # ================================================================

    audit_finish = PythonOperator(

        task_id="finish_audit",

        python_callable=finish_audit,

        trigger_rule=TriggerRule.ALL_DONE,

        op_kwargs={

            "task_ids": [

                "load_dim_customer",

                "load_dim_content",

                "load_dim_inventory_item",

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


    # ---------------------------------------------------------------
    # Start all Silver sensors after audit starts
    # ---------------------------------------------------------------

    audit_start >> [

        wait_for_silver_customer_profile,

        wait_for_silver_content_profile,

        wait_for_silver_inventory_item_profile,

    ]


    # ---------------------------------------------------------------
    # All Gold dimensions must complete before audit finishes
    # ---------------------------------------------------------------

    [

        load_customer,

        load_content,

        load_inventory_item,

    ] >> audit_finish


    # ---------------------------------------------------------------
    # Audit finish → End
    # ---------------------------------------------------------------

    audit_finish >> end