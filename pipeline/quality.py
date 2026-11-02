"""Data quality rules and standalone HTML reports, including failed checks."""
import html
import json
import math
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd
from common.schema import INPUT_COLUMNS
from common.registry import atomic_json
from pipeline.config import artifacts_root, work_root
from pipeline.data import read_frame, rows_frame, execution_id, split_window, skip_no_data
from pipeline.db import connect, lock
from pipeline.time_utils import add_months

def check_quality(frame, start, test_month):
    required = set(INPUT_COLUMNS) | {"target", "transaction_id"}
    missing = required - set(frame.columns)
    rules = []
    def record(name, count):
        rules.append({"rule": name, "violations": int(count), "passed": int(count) == 0})
    nulls = sum(int(frame[name].isna().sum()) + int(frame[name].astype(str).str.strip().eq("").sum())
                for name in required & set(frame.columns))
    record("Required fields", len(missing) + nulls + int(frame.empty))
    ids = frame.get("transaction_id", pd.Series(dtype=str))
    record("Unique nonempty transaction IDs", ids.duplicated().sum() + ids.isna().sum() + ids.astype(str).str.strip().eq("").sum())
    timestamps = pd.to_datetime(frame.get("transaction_time", pd.Series(dtype=str)), errors="coerce", utc=True, format="mixed")
    record("Valid transaction timestamps", timestamps.isna().sum() + int("transaction_time" in missing))
    targets = pd.to_numeric(frame.get("target", pd.Series(dtype=float)), errors="coerce")
    record("Binary target", (~targets.isin([0,1])).sum() + int("target" in missing))
    numeric_bad = 0
    for name in ("amount", "population_city"):
        values = pd.to_numeric(frame.get(name, pd.Series(dtype=float)), errors="coerce")
        numeric_bad += int((~np.isfinite(values) | (values < 0)).sum()) + int(name in missing)
    record("Finite nonnegative amount and population", numeric_bad)
    coordinates_bad = 0
    for name in ("lat", "lon", "merchant_lat", "merchant_lon"):
        values = pd.to_numeric(frame.get(name, pd.Series(dtype=float)), errors="coerce")
        limit = 90 if name.endswith("lat") else 180
        coordinates_bad += int((~np.isfinite(values) | (values.abs() > limit)).sum()) + int(name in missing)
    record("Coordinate ranges", coordinates_bad)
    expected = {add_months(start, index).strftime("%Y-%m") for index in range(3)}
    actual = set(timestamps.dropna().dt.strftime("%Y-%m"))
    bad_window = len(expected.symmetric_difference(actual))
    if len(timestamps) == len(targets):
        test = pd.Timestamp(test_month, tz="UTC")
        for mask in (timestamps < test, timestamps >= test):
            bad_window += int(set(targets.loc[mask].dropna()) != {0,1})
    else:
        bad_window += 2
    record("Three months and both classes in train/test", bad_window)
    return {"passed": all(rule["passed"] for rule in rules), "rows": len(frame), "rules": rules,
            "window_start": str(start), "test_month": str(test_month)}

def ensure_quality(frame, start, test_month):
    result = check_quality(frame, start, test_month)
    if not result["passed"]:
        raise ValueError("Data quality failed: " + ", ".join(item["rule"] for item in result["rules"] if not item["passed"]))
    return result

def snapshot_dataset(run_id=None):
    """Read one consistent three-month view and persist it. Retry reuses the same snapshot."""
    import hashlib
    key = "dq-" + hashlib.sha256(execution_id(run_id).encode()).hexdigest()[:20]
    directory = work_root() / key
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = directory / "snapshot.json"
    if manifest_path.exists():
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    with connect() as connection:
        lock(connection)
        state = connection.execute("SELECT next_test_month FROM training_state WHERE singleton").fetchone()
        if state is None:
            skip_no_data("Initial labeled data is not loaded")
        # Check the window currently due for training; report missing months as a DQ failure.
        test = state["next_test_month"]
        start = add_months(test, -2)
        rows = connection.execute(
            "SELECT * FROM labeled_transactions WHERE transaction_time >= %s AND transaction_time < %s ORDER BY transaction_time,transaction_id",
            (start, add_months(test,1))).fetchall()
        frame = rows_frame(rows)
        save_path = directory / "snapshot.jsonl"
        from pipeline.data import save_frame
        save_frame(save_path, frame)
        result = {"key": key, "path": str(save_path), "start": start.isoformat(), "test": test.isoformat()}
        atomic_json(manifest_path, result)
    return result

def evaluate_dataset(snapshot):
    """Read a frozen snapshot and write rule results; retry replaces only its own report data."""
    result = check_quality(read_frame(snapshot["path"]), snapshot["start"], snapshot["test"])
    result.update(dataset_id=snapshot["key"], checked_at=datetime.now(timezone.utc).isoformat())
    atomic_json(Path(snapshot["path"]).parent / "quality.json", result)
    return snapshot

def write_report(snapshot):
    """Read rule results and write escaped HTML next to models, even for failed datasets."""
    result = json.loads((Path(snapshot["path"]).parent / "quality.json").read_text(encoding="utf-8"))
    report_directory = artifacts_root() / "dq_reports"
    report_directory.mkdir(parents=True, exist_ok=True)
    rows = "".join(f"<tr><td>{html.escape(rule['rule'])}</td><td>{rule['violations']}</td><td>{'PASS' if rule['passed'] else 'FAIL'}</td></tr>" for rule in result["rules"])
    document = f"""<!doctype html><html lang="ru"><meta charset="utf-8"><title>Data Quality</title>
<style>body{{font:16px sans-serif;max-width:1050px;margin:40px auto}}td,th{{padding:10px;border:1px solid #ccc}}table{{border-collapse:collapse}}</style>
<h1>Data Quality - {'PASS' if result['passed'] else 'FAIL'}</h1>
<p>Dataset: {html.escape(result['dataset_id'])}</p><p>UTC: {html.escape(result['checked_at'])}</p>
<p>Rows: {result['rows']}; window: {html.escape(result['window_start'])} вЂ” test: {html.escape(result['test_month'])}</p>
<table><tr><th>Rule</th><th>Violations</th><th>Result</th></tr>{rows}</table></html>"""
    import os
    path = report_directory / f"{snapshot['key']}.html"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(document, encoding="utf-8")
    os.replace(temporary, path)
    atomic_json(report_directory / f"{snapshot['key']}.json", result)
    return {**snapshot, "passed": result["passed"], "report": str(path)}

def assert_report(report):
    """Read the persisted result after HTML publication; failure never suppresses the report."""
    if not report["passed"]:
        raise ValueError(f"Data quality failed; see {report['report']}")
    return report["report"]
