from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_connection(conn_id: str):
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_conn()

def extract_data(conn_id: str, sql: str):
    conn = get_connection(conn_id)
    cursor = conn.cursor()

    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    
    cursor.close()
    conn.close()
    return rows, columns

def truncate_table(conn_id: str, table_name: str):
    conn = get_connection(conn_id)
    cursor = conn.cursor()

    cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY")
    conn.commit()

    cursor.close()
    conn.close()

def load_data(
    conn_id: str,
    table_name: str,
    columns: list,
    rows: list,
):
    if not rows:
        print(f"No rows to load into {table_name}")
        return

    conn = get_connection(conn_id)
    cursor = conn.cursor()

    placeholders = ",".join(["%s"] * len(columns))
    column_names = ",".join(columns)

    insert_sql = f"""
        INSERT INTO {table_name}
        ({column_names})
        VALUES ({placeholders})
    """

    # Use executemany instead of execute_batch to avoid mogrify error
    cursor.executemany(insert_sql, rows)

    conn.commit()
    cursor.close()
    conn.close()