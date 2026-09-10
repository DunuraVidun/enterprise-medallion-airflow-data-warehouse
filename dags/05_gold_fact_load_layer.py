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
    Load a Gold fact table using a FULL REFRESH pattern.

    Audit logic:
    - source_record_count    = expected fact-grain rows
    - processed_record_count = rows actually present after load
    - inserted_record_count  = rows loaded during rebuild
    - updated_record_count   = 0
    - rejected_record_count  = expected rows not loaded
    """

    logger.info(
        "Starting Gold fact load using %s",
        sql_file
    )

    hook = PostgresHook(
        postgres_conn_id=TARGET_CONN
    )

    conn = hook.get_conn()

    # ------------------------------------------------------------
    # Target fact table mapping
    # ------------------------------------------------------------

    fact_table_map = {

        "fact_customer_daily_activity.sql":
            "gold.fact_customer_daily_activity",

        "fact_content_monthly_performance.sql":
            "gold.fact_content_monthly_performance",

        "fact_inventory_daily_utilization.sql":
            "gold.fact_inventory_daily_utilization",

    }

    # ------------------------------------------------------------
    # Expected fact-grain row count queries
    # ------------------------------------------------------------

    source_count_sql_map = {

        "fact_customer_daily_activity.sql":
        """
        SELECT COUNT(*)
        FROM silver.customer AS c
        JOIN gold.dim_date AS d
            ON d.full_date >= COALESCE(
                c.registration_date,
                DATE '2023-01-01'
            )
           AND d.full_date <= CURRENT_DATE
        """,

        "fact_content_monthly_performance.sql":
        """
        SELECT COUNT(*)
        FROM silver.content AS c
        JOIN gold.dim_date AS d
            ON d.day = 1
           AND d.full_date <= DATE_TRUNC(
               'month',
               CURRENT_DATE
           )::DATE
           AND d.full_date >= GREATEST(
               COALESCE(
                   DATE_TRUNC(
                       'month',
                       c.release_date
                   )::DATE,
                   DATE '2023-01-01'
               ),
               DATE '2023-01-01'
           )
        """,

        "fact_inventory_daily_utilization.sql":
        """
        SELECT COUNT(*)
        FROM silver.inventory_item AS ii
        JOIN gold.dim_date AS d
            ON d.full_date >= COALESCE(
                ii.purchase_date,
                DATE '2023-01-01'
            )
           AND d.full_date <= CURRENT_DATE
        WHERE ii.barcode IS NOT NULL
        """,

    }

    target_table = fact_table_map.get(sql_file)

    source_count_sql = source_count_sql_map.get(sql_file)

    if target_table is None:

        raise ValueError(
            f"Unknown Gold fact SQL file: {sql_file}"
        )

    if source_count_sql is None:

        raise ValueError(
            f"No source count query configured for: {sql_file}"
        )

    try:

        # --------------------------------------------------------
        # STEP 1
        # Determine expected fact rows BEFORE loading.
        # --------------------------------------------------------

        with conn.cursor() as cursor:

            cursor.execute(source_count_sql)

            source_record_count = cursor.fetchone()[0]

        logger.info(
            "Expected fact rows for %s = %d",
            sql_file,
            source_record_count,
        )

        # --------------------------------------------------------
        # STEP 2
        # Read full-refresh SQL.
        # --------------------------------------------------------

        sql = read_sql(
            SQL_DIR / sql_file
        )

        # --------------------------------------------------------
        # STEP 3
        # Execute:
        #
        # TRUNCATE
        # +
        # INSERT
        #
        # No fetchall() because this is not a RETURNING query.
        # --------------------------------------------------------

        with conn.cursor() as cursor:

            cursor.execute(sql)

        # --------------------------------------------------------
        # STEP 4
        # Commit full refresh.
        # --------------------------------------------------------

        conn.commit()

        # --------------------------------------------------------
        # STEP 5
        # Count rows actually loaded.
        # --------------------------------------------------------

        with conn.cursor() as cursor:

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {target_table}
                """
            )

            processed_record_count = cursor.fetchone()[0]

        # --------------------------------------------------------
        # STEP 6
        # Full refresh = every processed row was inserted.
        # There is no UPDATE operation.
        # --------------------------------------------------------

        inserted_record_count = processed_record_count

        updated_record_count = 0

        # --------------------------------------------------------
        # STEP 7
        # Determine rows that were expected but not loaded.
        # --------------------------------------------------------

        rejected_record_count = max(
            source_record_count -
            processed_record_count,
            0
        )

        logger.info(
            "Gold fact load complete for %s | "
            "expected=%d processed=%d inserted=%d "
            "updated=%d rejected=%d",
            sql_file,
            source_record_count,
            processed_record_count,
            inserted_record_count,
            updated_record_count,
            rejected_record_count,
        )

        # --------------------------------------------------------
        # STEP 8
        # Return audit statistics.
        # --------------------------------------------------------

        return {

            "source_record_count":
                source_record_count,

            "processed_record_count":
                processed_record_count,

            "inserted_record_count":
                inserted_record_count,

            "updated_record_count":
                updated_record_count,

            "rejected_record_count":
                rejected_record_count,

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