"""Durable snapshots shared between tasks; raw input is never passed through XCom."""
import json
from pathlib import Path
import pandas as pd
from common.registry import atomic_json

def rows_frame(rows):
    return pd.DataFrame([{**row["data"], "transaction_time": row["transaction_time"].isoformat(),
                          "target": row["target"], "transaction_id": row["transaction_id"]} for row in rows])

def save_frame(path, frame):
    # Atomic replacement prevents readers from observing a partial retry.
    import os
    path = Path(path)
    temporary = path.with_suffix(".tmp")
    frame.to_json(temporary, orient="records", lines=True, date_format="iso")
    os.replace(temporary, path)

def read_frame(path):
    return pd.read_json(path, orient="records", lines=True, dtype=False, convert_dates=False)

def split_window(frame, start, test_month):
    from pipeline.time_utils import add_months
    timestamps = pd.to_datetime(frame["transaction_time"], errors="raise", utc=True)
    start = pd.Timestamp(start, tz="UTC")
    test = pd.Timestamp(test_month, tz="UTC")
    finish = pd.Timestamp(add_months(test_month, 1), tz="UTC")
    if not ((timestamps >= start) & (timestamps < finish)).all():
        raise ValueError("Snapshot includes rows outside the fixed window")
    return frame.loc[timestamps < test].copy(), frame.loc[timestamps >= test].copy()

class NoData(RuntimeError):
    pass

def skip_no_data(message):
    try:
        from airflow.exceptions import AirflowSkipException
    except ImportError:
        raise NoData(message)
    raise AirflowSkipException(message)

def execution_id(value):
    if value is not None:
        return value
    from airflow.operators.python import get_current_context
    return get_current_context()["run_id"]
