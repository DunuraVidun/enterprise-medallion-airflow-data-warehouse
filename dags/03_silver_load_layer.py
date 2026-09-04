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
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import TriggerRule

from utils.database import extract_data, load_data, truncate_table
from utils.file import read_sql
from utils.audit import start_audit, finish_audit

# --------------------------------------------------------------------
# Connections
# --------------------------------------------------------------------

SOURCE_CONN = "abc_hub_olap"
TARGET_CONN = "abc_hub_olap"

SQL_DIR = BASE_DIR / "sql" / "silver" 

BRONZE_DAG_ID = "02_delta_load_bronze"

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
# Generic Silver Loader Function
# --------------------------------------------------------------------

def load_silver_table(sql_file: str, target_table: str, source_table: str = None,):

    """
    Perform a full refresh from Bronze into a Silver table.

    Steps:
    1. Read the Silver SQL query.
    2. Extract cleaned and validated data from Bronze.
    3. If no records are returned, skip the load.
    4. Truncate the target Silver table.
    5. Load the refreshed data.
    6. Return the number of loaded rows.
    """

    try:

        logger.info(
            "Starting Silver load for %s using %s",
            target_table,
            sql_file
        )

        source_count = 0

        if source_table:

            source_count_sql = f"""
                SELECT COUNT(*)
                FROM {source_table}
            """

            source_rows, _ = extract_data(
                conn_id=SOURCE_CONN,
                sql=source_count_sql,
            )

            source_count = source_rows[0][0] if source_rows else 0

            logger.info(
                "Source record count for %s: %d",
                source_table,
                source_count
            )


        sql = read_sql(
            SQL_DIR / sql_file
        )


        rows, columns = extract_data(
            conn_id=SOURCE_CONN,
            sql=sql,
        )

        processed_count = len(rows)

        logger.info(
            "Extracted %d rows for %s",
            processed_count,
            target_table
        )

        if source_table:
            rejected_count = max(
                source_count - processed_count,
                0
            )
        else:
            # Profile tables are generated from Silver data and don't
            # directly perform Bronze cleansing.
            rejected_count = 0

        if not rows:

            logger.warning(
                "No valid records returned for %s. "
                "Silver table was not modified.",
                target_table
            )

            return {
                "source_record_count": source_count,
                "processed_record_count": 0,
                "inserted_record_count": 0,
                "updated_record_count": 0,
                "rejected_record_count": rejected_count,
            }


        logger.info(
            "Truncating Silver table %s",
            target_table
        )


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

        logger.info(
            "Successfully loaded %d rows into %s",
            len(rows),
            target_table
        )

        return {
            "source_record_count": source_count,
            "processed_record_count": processed_count,
            "inserted_record_count": processed_count,
            "updated_record_count": 0,
            "rejected_record_count": rejected_count,
        }

    except Exception:

        logger.exception(
            "Silver load failed for %s",
            target_table
        )

        raise

# --------------------------------------------------------------------
# External Bronze Sensor
# --------------------------------------------------------------------

def bronze_sensor(task_id: str, sensor_task_id: str):

    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=BRONZE_DAG_ID,
        external_task_id=sensor_task_id,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )


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
    dag_id="03_silver_load_layer",
    description="Load Silver layer from Bronze",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["silver", "training", "medallion"],
) as dag:

    # ================================================================
    # START
    # ================================================================

    start = EmptyOperator(
        task_id="start"
    )

    audit_start = PythonOperator(
        task_id="start_audit",
        python_callable=start_audit,
    )


    # ================================================================
    # COUNTRY
    # ================================================================

    wait_for_bronze_country = bronze_sensor(
        "wait_for_bronze_country",
        "delta_country",
    )

    load_country = PythonOperator(
        task_id="load_silver_country",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "country.sql",
            "target_table": "silver.country",
            "source_table": "bronze.country",
        },
    )

    wait_for_bronze_country >> load_country


    # ================================================================
    # CITY
    # ================================================================

    wait_for_bronze_city = bronze_sensor(
        "wait_for_bronze_city",
        "delta_city",
    )

    load_city = PythonOperator(
        task_id="load_silver_city",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "city.sql",
            "target_table": "silver.city",
            "source_table": "bronze.city",
        },
    )

    wait_for_bronze_city >> load_city

    load_country >> load_city


    # ================================================================
    # CUSTOMER
    # ================================================================

    wait_for_bronze_customer = bronze_sensor(
        "wait_for_bronze_customer",
        "delta_customer",
    )

    load_customer = PythonOperator(
        task_id="load_silver_customer",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "customer.sql",
            "target_table": "silver.customer",
            "source_table": "bronze.customer",
        },
    )

    wait_for_bronze_customer >> load_customer


    # ================================================================
    # CUSTOMER ADDRESS
    # ================================================================

    wait_for_bronze_customer_address = bronze_sensor(
        "wait_for_bronze_customer_address",
        "delta_customer_address",
    )

    load_customer_address = PythonOperator(
        task_id="load_silver_customer_address",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "customer_address.sql",
            "target_table": "silver.customer_address",
            "source_table": "bronze.customer_address",
        },
    )

    wait_for_bronze_customer_address >> load_customer_address

    [
        load_customer,
        load_city,
    ] >> load_customer_address

    # ================================================================
    # CUSTOMER PROFILE
    # ================================================================

    load_customer_profile = PythonOperator(
        task_id="load_silver_customer_profile",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "profile/customer_profile.sql",
            "target_table": "silver.customer_profile",
        },
    )

    [
        load_customer,
        load_customer_address,
        load_city,
        load_country,
    ] >> load_customer_profile


    # ================================================================
    # CONTENT TYPE
    # ================================================================

    wait_for_bronze_content_type = bronze_sensor(
        "wait_for_bronze_content_type",
        "delta_content_type",
    )

    load_content_type = PythonOperator(
        task_id="load_silver_content_type",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "content_type.sql",
            "target_table": "silver.content_type",
            "source_table": "bronze.content_type",
        },
    )

    wait_for_bronze_content_type >> load_content_type


    # ================================================================
    # CONTENT
    # ================================================================

    wait_for_bronze_content = bronze_sensor(
        "wait_for_bronze_content",
        "delta_content",
    )

    load_content = PythonOperator(
        task_id="load_silver_content",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "content.sql",
            "target_table": "silver.content",
            "source_table": "bronze.content",
        },
    )

    wait_for_bronze_content >> load_content

    load_content_type >> load_content


    # ================================================================
    # GENRE
    # ================================================================

    wait_for_bronze_genre = bronze_sensor(
        "wait_for_bronze_genre",
        "delta_genre",
    )

    load_genre = PythonOperator(
        task_id="load_silver_genre",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "genre.sql",
            "target_table": "silver.genre",
            "source_table": "bronze.genre",
        },
    )

    wait_for_bronze_genre >> load_genre


    # ================================================================
    # CONTENT GENRE
    # ================================================================

    wait_for_bronze_content_genre = bronze_sensor(
        "wait_for_bronze_content_genre",
        "delta_content_genre",
    )

    load_content_genre = PythonOperator(
        task_id="load_silver_content_genre",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "content_genre.sql",
            "target_table": "silver.content_genre",
            "source_table": "bronze.content_genre",
        },
    )

    wait_for_bronze_content_genre >> load_content_genre

    [
        load_content,
        load_genre,
    ] >> load_content_genre

    # ================================================================
    # CONTENT PROFILE
    # ================================================================

    load_content_profile = PythonOperator(
        task_id="load_silver_content_profile",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "profile/content_profile.sql",
            "target_table": "silver.content_profile",
        },
    )

    [
        load_content,
        load_content_type,
        load_content_genre,
        load_genre,
    ] >> load_content_profile


    # ================================================================
    # STREAMING SESSION
    # ================================================================

    wait_for_bronze_streaming_session = bronze_sensor(
        "wait_for_bronze_streaming_session",
        "delta_streaming_session",
    )

    load_streaming_session = PythonOperator(
        task_id="load_silver_streaming_session",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "streaming_session.sql",
            "target_table": "silver.streaming_session",
            "source_table": "bronze.streaming_session",
        },
    )

    wait_for_bronze_streaming_session >> load_streaming_session

    [
        load_customer,
        load_content,
    ] >> load_streaming_session


    # ================================================================
    # REVIEW
    # ================================================================

    wait_for_bronze_review = bronze_sensor(
        "wait_for_bronze_review",
        "delta_review",
    )

    load_review = PythonOperator(
        task_id="load_silver_review",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "review.sql",
            "target_table": "silver.review",
            "source_table": "bronze.review",
        },
    )

    wait_for_bronze_review >> load_review

    [
        load_customer,
        load_content,
    ] >> load_review


    # ================================================================
    # WISHLIST
    # ================================================================

    wait_for_bronze_wishlist = bronze_sensor(
        "wait_for_bronze_wishlist",
        "delta_wishlist",
    )

    load_wishlist = PythonOperator(
        task_id="load_silver_wishlist",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "wishlist.sql",
            "target_table": "silver.wishlist",
            "source_table": "bronze.wishlist",
        },
    )

    wait_for_bronze_wishlist >> load_wishlist

    [
        load_customer,
        load_content,
    ] >> load_wishlist


    # ================================================================
    # WAREHOUSE
    # ================================================================

    wait_for_bronze_warehouse = bronze_sensor(
        "wait_for_bronze_warehouse",
        "delta_warehouse",
    )

    load_warehouse = PythonOperator(
        task_id="load_silver_warehouse",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "warehouse.sql",
            "target_table": "silver.warehouse",
            "source_table": "bronze.warehouse",
        },
    )

    wait_for_bronze_warehouse >> load_warehouse

    load_city >> load_warehouse


    # ================================================================
    # INVENTORY ITEM
    # ================================================================

    wait_for_bronze_inventory_item = bronze_sensor(
        "wait_for_bronze_inventory_item",
        "delta_inventory_item",
    )

    load_inventory_item = PythonOperator(
        task_id="load_silver_inventory_item",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "inventory_item.sql",
            "target_table": "silver.inventory_item",
            "source_table": "bronze.inventory_item",
        },
    )

    wait_for_bronze_inventory_item >> load_inventory_item

    [
        load_content,
        load_warehouse,
    ] >> load_inventory_item

    # ================================================================
    # INVENTORY ITEM PROFILE
    # ================================================================

    load_inventory_item_profile = PythonOperator(
        task_id="load_silver_inventory_item_profile",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "profile/inventory_item_profile.sql",
            "target_table": "silver.inventory_item_profile",
        },
    )

    [
        load_inventory_item,
        load_content,
        load_warehouse,
        load_city,
    ] >> load_inventory_item_profile


    # ================================================================
    # RENTAL
    # ================================================================

    wait_for_bronze_rental = bronze_sensor(
        "wait_for_bronze_rental",
        "delta_rental",
    )

    load_rental = PythonOperator(
        task_id="load_silver_rental",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "rental.sql",
            "target_table": "silver.rental",
            "source_table": "bronze.rental",
        },
    )

    wait_for_bronze_rental >> load_rental

    [
        load_customer,
        load_inventory_item,
    ] >> load_rental


    # ================================================================
    # PAYMENT
    # ================================================================

    wait_for_bronze_payment = bronze_sensor(
        "wait_for_bronze_payment",
        "delta_payment",
    )

    load_payment = PythonOperator(
        task_id="load_silver_payment",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "payment.sql",
            "target_table": "silver.payment",
            "source_table": "bronze.payment",
        },
    )

    wait_for_bronze_payment >> load_payment

    [
        load_customer,
        load_rental,
    ] >> load_payment


    # ================================================================
    # SUPPORT TICKET
    # ================================================================

    wait_for_bronze_support_ticket = bronze_sensor(
        "wait_for_bronze_support_ticket",
        "delta_support_ticket",
    )

    load_support_ticket = PythonOperator(
        task_id="load_silver_support_ticket",
        python_callable=load_silver_table,
        op_kwargs={
            "sql_file": "support_ticket.sql",
            "target_table": "silver.support_ticket",
            "source_table": "bronze.support_ticket",
        },
    )

    wait_for_bronze_support_ticket >> load_support_ticket

    load_customer >> load_support_ticket

    # ================================================================
    # SILVER AUDIT FINISH
    # ================================================================

    audit_finish = PythonOperator(
        task_id="finish_audit",
        python_callable=finish_audit,
        trigger_rule=TriggerRule.ALL_DONE,
        op_kwargs={
            "task_ids": [
                "load_silver_country",
                "load_silver_city",
                "load_silver_customer",
                "load_silver_customer_address",
                "load_silver_customer_profile",

                "load_silver_content_type",
                "load_silver_content",
                "load_silver_genre",
                "load_silver_content_genre",
                "load_silver_content_profile",

                "load_silver_streaming_session",
                "load_silver_review",
                "load_silver_wishlist",
                "load_silver_warehouse",
                "load_silver_inventory_item",
                "load_silver_inventory_item_profile",

                "load_silver_rental",
                "load_silver_payment",
                "load_silver_support_ticket",
            ]
        },
    )

    end = EmptyOperator(
        task_id="end"
    )


    # ================================================================
    # START → ALL BRONZE SENSORS
    # ================================================================

    start >> audit_start >> [
        wait_for_bronze_country,
        wait_for_bronze_city,
        wait_for_bronze_customer,
        wait_for_bronze_customer_address,
        wait_for_bronze_content_type,
        wait_for_bronze_content,
        wait_for_bronze_genre,
        wait_for_bronze_content_genre,
        wait_for_bronze_streaming_session,
        wait_for_bronze_review,
        wait_for_bronze_wishlist,
        wait_for_bronze_warehouse,
        wait_for_bronze_inventory_item,
        wait_for_bronze_rental,
        wait_for_bronze_payment,
        wait_for_bronze_support_ticket,
    ]

# ================================================================
# ALL SILVER LOADS → AUDIT FINISH → END
# ================================================================

[
    load_country,
    load_city,
    load_customer,
    load_customer_address,
    load_customer_profile, 

    load_content_type,
    load_content,
    load_genre,
    load_content_genre,
    load_content_profile,
    load_streaming_session,
    load_review,
    load_wishlist,

    load_warehouse,
    load_inventory_item,
    load_inventory_item_profile,

    load_rental,
    load_payment,

    load_support_ticket,

] >> audit_finish >> end

