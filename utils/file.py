from pathlib import Path


def read_sql(sql_file: str) -> str:
    with open(sql_file, "r") as file:
        return file.read()