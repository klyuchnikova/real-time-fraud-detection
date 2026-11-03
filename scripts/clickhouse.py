"""Connection and identifier validation shared by loading and reconciliation."""
import os
import re
import clickhouse_connect

def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid SQL identifier: {value}")
    return value

def connect():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST","localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT","8123")),
        username=os.getenv("CLICKHOUSE_USER","analytics"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        connect_timeout=10, send_receive_timeout=300,
        settings={"max_threads":2,"max_memory_usage":1200000000},
    )
