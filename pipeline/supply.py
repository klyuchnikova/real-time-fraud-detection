"""Idempotent monthly supply, with separate durable reservations and retention."""
import hashlib
import json
from pathlib import Path
import pandas as pd
from psycopg.types.json import Jsonb
from pipeline.config import work_root, positive_int
from pipeline.db import connect, lock
from pipeline.data import execution_id, rows_frame, save_frame, read_frame, skip_no_data
from pipeline.time_utils import month_start, add_months, shift_to_month

def reserve_batch(run_id=None):
    """Read the latest month and freeze sampled source rows before any insert.
    A failed/new run resumes the pending batch; a repeated completed run reuses its receipt.
    """
    identifier = execution_id(run_id)
    with connect() as connection:
        lock(connection)
        row = connection.execute(
            "SELECT r.* FROM supply_runs r LEFT JOIN supply_requests q ON q.batch_id=r.batch_id "
            "WHERE q.run_id=%s OR r.run_id=%s LIMIT 1", (identifier,identifier)).fetchone()
        if row is None:
            row = connection.execute("SELECT * FROM supply_runs WHERE status <> 'complete' ORDER BY target_month LIMIT 1").fetchone()
        if row is None:
            newest = connection.execute("SELECT max(transaction_time) AS newest FROM labeled_transactions").fetchone()["newest"]
            if newest is None:
                skip_no_data("No labeled rows yet")
            target = add_months(month_start(newest), 1)
            batch_id = "supply-" + target.strftime("%Y%m")
            count = positive_int("SUPPLY_BATCH_SIZE", 100000)
            sampled = connection.execute(
                "SELECT * FROM labeled_transactions ORDER BY md5(transaction_id || %s) LIMIT %s",
                (batch_id, count)).fetchall()
            if len(sampled) < count:
                raise ValueError("Dataset is smaller than SUPPLY_BATCH_SIZE")
            directory = work_root() / batch_id
            directory.mkdir(parents=True, exist_ok=True)
            save_frame(directory / "source.jsonl", rows_frame(sampled))
            row = connection.execute(
                "INSERT INTO supply_runs(batch_id,run_id,target_month,source_ids) VALUES(%s,%s,%s,%s) RETURNING *",
                (batch_id, identifier, target.date(), Jsonb([item["transaction_id"] for item in sampled]))).fetchone()
        connection.execute("INSERT INTO supply_requests(run_id,batch_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                           (identifier,row["batch_id"]))
        return {"batch_id": row["batch_id"], "target": row["target_month"].isoformat()}

def prepare_batch(batch):
    """Read frozen sources and shift calendar dates; retry recreates the same rows and IDs."""
    directory = work_root() / batch["batch_id"]
    frame = read_frame(directory / "source.jsonl")
    frame["transaction_time"] = frame["transaction_time"].map(lambda value: shift_to_month(value, batch["target"]).isoformat())
    frame["transaction_id"] = frame["transaction_id"].map(lambda value: hashlib.sha256(f"{batch['batch_id']}:{value}".encode()).hexdigest())
    save_frame(directory / "batch.jsonl", frame)
    return batch

def insert_batch(batch):
    """Insert the complete prepared batch and its receipt in one transaction; retries are no-ops."""
    from common.schema import INPUT_COLUMNS
    frame = read_frame(work_root() / batch["batch_id"] / "batch.jsonl")
    with connect() as connection:
        lock(connection)
        state = connection.execute("SELECT status FROM supply_runs WHERE batch_id=%s FOR UPDATE", (batch["batch_id"],)).fetchone()
        if state["status"] in ("inserted", "complete"):
            return batch
        with connection.cursor().copy(
            "COPY labeled_transactions(transaction_id,transaction_time,target,data,batch_id) FROM STDIN"
        ) as copy:
            for row in frame.to_dict("records"):
                data = {name: row[name] for name in INPUT_COLUMNS}
                copy.write_row((row["transaction_id"], pd.Timestamp(row["transaction_time"]).to_pydatetime(),
                                int(row["target"]), Jsonb(data), batch["batch_id"]))
        connection.execute("UPDATE supply_runs SET status='inserted', row_count=%s WHERE batch_id=%s",
                           (len(frame), batch["batch_id"]))
    return batch

def prune_history(batch):
    """Delete expired months and finish the batch atomically; repeats never append another month.
    The unprocessed training window is protected: a fast supplier waits rather than erasing it.
    """
    with connect() as connection:
        lock(connection)
        state = connection.execute("SELECT status FROM supply_runs WHERE batch_id=%s FOR UPDATE", (batch["batch_id"],)).fetchone()
        if state["status"] == "complete":
            return batch
        if state["status"] != "inserted":
            raise ValueError("Cannot prune before batch insert")
        newest = connection.execute("SELECT max(transaction_time) AS newest FROM labeled_transactions").fetchone()["newest"]
        cutoff = add_months(month_start(newest), 1-positive_int("HISTORY_MONTHS",36))
        train = connection.execute("SELECT next_test_month FROM training_state WHERE singleton").fetchone()
        if train and cutoff > add_months(train["next_test_month"], -2):
            raise ValueError("Supply is ahead of training: retention would delete an unprocessed month")
        connection.execute("DELETE FROM labeled_transactions WHERE transaction_time < %s", (cutoff,))
        connection.execute("UPDATE supply_runs SET status='complete' WHERE batch_id=%s", (batch["batch_id"],))
    return batch
