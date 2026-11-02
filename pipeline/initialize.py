"""Idempotent initial dataset loading and schema migrations."""
import csv
import hashlib
import os
from datetime import datetime
from pathlib import Path
from psycopg.types.json import Jsonb
from common.schema import read_transactions_csv
from common.registry import digest
from pipeline.db import apply_schema, connect, lock
from pipeline.time_utils import month_start, add_months

def initialize():
    """Read train.csv into the labeled table once per source hash, in one transaction.
    The source receipt and rows commit together; a failed COPY rolls back completely.
    """
    apply_schema()
    path = Path(os.getenv("TRAIN_DATA_CSV", "/data/train.csv"))
    fingerprint = digest(path)
    with connect() as connection:
        lock(connection)
        if connection.execute("SELECT 1 FROM dataset_sources WHERE sha256=%s", (fingerprint,)).fetchone():
            return
        if connection.execute("SELECT 1 FROM dataset_sources LIMIT 1").fetchone():
            raise ValueError("A different training source is already loaded; use a new project volume")
        count = 0
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = read_transactions_csv(stream, allow_target=True)
            if "target" not in reader.fieldnames:
                raise ValueError("Training CSV must include target")
            with connection.cursor().copy(
                "COPY labeled_transactions(transaction_id,transaction_time,target,data,batch_id) FROM STDIN"
            ) as copy:
                for index, row in enumerate(reader):
                    if None in row or any(value is None or value == "" for value in row.values()):
                        raise ValueError(f"Missing values at CSV row {index+2}")
                    target = row.pop("target")
                    if target not in ("0", "1"):
                        raise ValueError(f"Invalid target at CSV row {index+2}")
                    timestamp = datetime.fromisoformat(row["transaction_time"])
                    if timestamp.tzinfo is not None:
                        raise ValueError("Source timestamps must be timezone-naive (interpreted as UTC)")
                    identifier = hashlib.sha256(f"{fingerprint}:{index}".encode()).hexdigest()
                    copy.write_row((identifier, timestamp, int(target), Jsonb(row), "initial"))
                    count += 1
        if not count:
            raise ValueError("Empty dataset")
        if digest(path) != fingerprint:
            raise ValueError("Training source changed while loading")
        first = connection.execute("SELECT min(transaction_time) AS first FROM labeled_transactions").fetchone()["first"]
        connection.execute("INSERT INTO training_state(singleton,next_test_month) VALUES(TRUE,%s) ON CONFLICT DO NOTHING",
                           (add_months(month_start(first), 2).date(),))
        connection.execute("INSERT INTO dataset_sources(sha256,rows_loaded) VALUES(%s,%s)", (fingerprint, count))
    print(f"Loaded {count} labeled rows")

if __name__ == "__main__":
    initialize()
