from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import timezone

TARGET_CONN = "abc_hub_olap"

def start_audit(**context):

    hook = PostgresHook(postgres_conn_id=TARGET_CONN)

    hook.run(
        """
        INSERT INTO audit.etl_audit(
            batch_id,
            dag_id,
            run_id,
            start_time,
            status
        )
        VALUES(%s,%s,%s,%s,'Running')
        """,
        parameters=(
            context["ds_nodash"],
            context["dag"].dag_id,
            context["run_id"],
            context["logical_date"],
        ),
    )


def finish_audit(task_ids, **context):

    hook = PostgresHook(postgres_conn_id=TARGET_CONN)
    ti = context["ti"]

    counts = []
    failed = False

    for task in task_ids:
        value = ti.xcom_pull(task_ids=task)

        # Failed tasks won't have a return value in XCom.
        if value is None:
            failed = True
            counts.append(0)
        else:
            counts.append(value)

    total = sum(counts)
    status = "Failed" if failed else "Success"

    hook.run(
        """
        UPDATE audit.etl_audit
        SET
            end_time=%s,
            status=%s,
            source_record_count=%s,
            processed_record_count=%s,
            inserted_record_count=%s
        WHERE
            batch_id=%s
            AND dag_id=%s
            AND run_id=%s
        """,
        parameters=(
            timezone.utcnow(),
            status,
            total,
            total,
            total,
            context["ds_nodash"],
            context["dag"].dag_id,
            context["run_id"],
        ),
    )