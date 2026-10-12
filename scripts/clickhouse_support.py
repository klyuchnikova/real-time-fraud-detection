"""Shared connection, DDL rendering and bounded ingestion waiting."""
import os
import re
import time
from pathlib import Path

import clickhouse_connect
import sqlparse

SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "clickhouse"


def get_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "analytics"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        connect_timeout=10, send_receive_timeout=180,
    )


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Invalid SQL identifier")
    return value


def render_ddl(path, database="fraud_analytics", topic="transactions_analytics",
               group="fraud_analytics_v1", broker="kafka:19092"):
    identifier(database)
    for value in (topic, group, broker):
        if not re.fullmatch(r"[A-Za-z0-9_.:,/-]+", value):
            raise ValueError("Invalid Kafka configuration")
    sql = Path(path).read_text(encoding="utf-8")
    sql = re.sub(r"\bfraud_analytics\b", database, sql)
    return sql.replace("'transactions_analytics'", f"'{topic}'").replace(
        "'fraud_analytics_v1'", f"'{group}'").replace("'kafka:19092'", f"'{broker}'")


def execute_ddl(client, sql):
    for statement in sqlparse.split(sql):
        client.command(statement)


def query_file(name):
    return (SQL_DIR / name).read_text(encoding="utf-8").strip().rstrip(";")


def wait_dataset(client, dataset_id, expected, database="fraud_analytics", timeout=120):
    identifier(database)
    deadline = time.monotonic() + timeout
    stats = None
    while time.monotonic() < deadline:
        stats = client.query(
            f"SELECT count(), uniqExact(transaction_id), uniqExact(row_index) "
            f"FROM {database}.transactions WHERE dataset_id = {{dataset_id:String}}",
            parameters={"dataset_id": dataset_id},
        ).first_row
        if stats[1] == expected and stats[2] == expected:
            return {"raw_rows": stats[0], "unique_transactions": stats[1], "unique_row_indices": stats[2]}
        if stats[1] > expected or stats[2] > expected:
            raise RuntimeError(f"Dataset has unexpected extra records: {stats}")
        time.sleep(0.5)
    diagnostics = client.query(
        "SELECT table, exceptions.text FROM system.kafka_consumers WHERE database = {database:String}",
        parameters={"database": database},
    ).result_rows
    raise TimeoutError(f"Dataset {dataset_id}: expected={expected}, actual={stats}, consumers={diagnostics}")
