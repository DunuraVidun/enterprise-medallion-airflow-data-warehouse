from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import timezone


TARGET_CONN = "abc_hub_olap"


# --------------------------------------------------------------------
# START AUDIT
# --------------------------------------------------------------------

def start_audit(**context):

    hook = PostgresHook(
        postgres_conn_id=TARGET_CONN
    )

    hook.run(
        """
        INSERT INTO audit.etl_audit(
            batch_id,
            dag_id,
            run_id,
            start_time,
            status
        )
        VALUES(%s, %s, %s, %s, 'Running')
        """,
        parameters=(
            context["ds_nodash"],
            context["dag"].dag_id,
            context["run_id"],
            context["logical_date"],
        ),
    )


# --------------------------------------------------------------------
# FINISH AUDIT
# --------------------------------------------------------------------

def finish_audit(task_ids, **context):

    hook = PostgresHook(
        postgres_conn_id=TARGET_CONN
    )

    ti = context["ti"]

    source_total = 0
    processed_total = 0
    inserted_total = 0
    updated_total = 0
    rejected_total = 0

    failed = False

    # ---------------------------------------------------------------
    # Collect statistics returned by each task.
    # ---------------------------------------------------------------

    for task in task_ids:

        value = ti.xcom_pull(
            task_ids=task
        )

        # -----------------------------------------------------------
        # Log the XCom value for debugging
        # -----------------------------------------------------------

        print(
            f"Audit XCom | task={task} | "
            f"value={value} | "
            f"type={type(value).__name__}"
        )

        # -----------------------------------------------------------
        # No return value
        # -----------------------------------------------------------

        if value is None:

            failed = True

            continue

        # -----------------------------------------------------------
        # SILVER TASK
        #
        # Silver loader returns:
        #
        # {
        #     "source_record_count": ...,
        #     "processed_record_count": ...,
        #     "inserted_record_count": ...,
        #     "updated_record_count": ...,
        #     "rejected_record_count": ...
        # }
        # -----------------------------------------------------------

        if isinstance(value, dict):

            source_total += value.get(
                "source_record_count",
                0
            )

            processed_total += value.get(
                "processed_record_count",
                0
            )

            inserted_total += value.get(
                "inserted_record_count",
                0
            )

            updated_total += value.get(
                "updated_record_count",
                0
            )

            rejected_total += value.get(
                "rejected_record_count",
                0
            )

        # -----------------------------------------------------------
        # BRONZE TASK
        #
        # Full Bronze and Delta Bronze loaders return:
        #
        #     return extracted_count
        #
        # Therefore XCom is an integer.
        # -----------------------------------------------------------

        elif isinstance(value, int):

            source_total += value

            processed_total += value

            inserted_total += value

            # Bronze does not calculate these.
            updated_total += 0
            rejected_total += 0

        # -----------------------------------------------------------
        # Unexpected XCom type
        # -----------------------------------------------------------

        else:

            print(
                f"Unexpected XCom type from task {task}: "
                f"{type(value).__name__}"
            )

            failed = True

    # ---------------------------------------------------------------
    # Determine final DAG status.
    # ---------------------------------------------------------------

    status = "Failed" if failed else "Success"

    # ---------------------------------------------------------------
    # Log final totals
    # ---------------------------------------------------------------

    print(
        f"Audit totals | "
        f"source={source_total}, "
        f"processed={processed_total}, "
        f"inserted={inserted_total}, "
        f"updated={updated_total}, "
        f"rejected={rejected_total}, "
        f"status={status}"
    )

    # ---------------------------------------------------------------
    # Update audit record.
    # ---------------------------------------------------------------

    hook.run(
        """
        UPDATE audit.etl_audit
        SET
            end_time=%s,
            status=%s,
            source_record_count=%s,
            processed_record_count=%s,
            inserted_record_count=%s,
            updated_record_count=%s,
            rejected_record_count=%s
        WHERE
            batch_id=%s
            AND dag_id=%s
            AND run_id=%s
        """,

        parameters=(
            timezone.utcnow(),

            status,

            source_total,
            processed_total,
            inserted_total,
            updated_total,
            rejected_total,

            context["ds_nodash"],
            context["dag"].dag_id,
            context["run_id"],
        ),
    )