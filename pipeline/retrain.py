"""Restartable three-month training lifecycle; active publication and processed month differ."""
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
from catboost import CatBoostClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from common.features import fit_preprocessor, prepare_features, CATEGORIES
from common.registry import atomic_json, active_version, load_bundle, save_bundle, publish
from pipeline.config import models_root, work_root, positive_int
from pipeline.db import connect, lock
from pipeline.data import rows_frame, save_frame, read_frame, split_window, execution_id, skip_no_data
from pipeline.time_utils import add_months
from pipeline.quality import ensure_quality

def directory(request):
    path = work_root() / request["version"]
    path.mkdir(parents=True, exist_ok=True)
    return path

def reserve_window(run_id=None):
    """Read the pending/next month, reserve one candidate and freeze raw data under a DB lock.
    New runs resume an interrupted candidate; a repeated finished run returns its old reservation.
    """
    identifier = execution_id(run_id)
    with connect() as connection:
        lock(connection)
        state = connection.execute("SELECT * FROM training_state WHERE singleton FOR UPDATE").fetchone()
        if not state:
            skip_no_data("Initial labeled data is not loaded")
        row = connection.execute(
            "SELECT r.* FROM training_runs r LEFT JOIN training_requests q ON q.version=r.version "
            "WHERE q.run_id=%s OR r.run_id=%s LIMIT 1", (identifier,identifier)).fetchone()
        if row is None and state["pending_version"]:
            row = connection.execute("SELECT * FROM training_runs WHERE version=%s", (state["pending_version"],)).fetchone()
        if row is None:
            test = state["next_test_month"]
            start = add_months(test, -2)
            rows = connection.execute(
                "SELECT * FROM labeled_transactions WHERE transaction_time >= %s AND transaction_time < %s ORDER BY transaction_time,transaction_id",
                (start, add_months(test,1))).fetchall()
            frame = rows_frame(rows)
            if frame.empty or not any(str(value).startswith(test.strftime("%Y-%m")) for value in frame["transaction_time"]):
                skip_no_data(f"Waiting for month {test}")
            version = "v-" + test.strftime("%Y%m") + "-" + hashlib.sha256(identifier.encode()).hexdigest()[:12]
            request = {"version": version}
            save_frame(directory(request) / "snapshot.jsonl", frame)
            row = connection.execute(
                "INSERT INTO training_runs(version,run_id,window_start,test_month,baseline_version) VALUES(%s,%s,%s,%s,%s) RETURNING *",
                (version, identifier, start.date(), test, active_version(models_root()))).fetchone()
            connection.execute("UPDATE training_state SET pending_version=%s WHERE singleton", (version,))
        connection.execute("INSERT INTO training_requests(run_id,version) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                           (identifier,row["version"]))
        return {"version": row["version"], "start": row["window_start"].isoformat(),
                "test": row["test_month"].isoformat(), "baseline": row["baseline_version"]}

def prepare_dataset(request):
    """Read the frozen snapshot, validate it and persist train-only preprocessing.
    Repeats use the same snapshot and deterministic feature specification.
    """
    path = directory(request)
    frame = read_frame(path / "snapshot.jsonl")
    quality = ensure_quality(frame, request["start"], request["test"])
    train, test = split_window(frame, request["start"], request["test"])
    atomic_json(path / "preprocessor.json", fit_preprocessor(train))
    atomic_json(path / "dataset.json", {**quality, "train_rows": len(train), "test_rows": len(test)})
    return request

def train_candidate(request):
    """Read train rows and a train-fitted specification; persist one deterministic CPU candidate.
    Retry reuses a complete saved candidate; partially written output is atomically replaced.
    """
    import os
    path = directory(request)
    if (path / "candidate.bin").exists():
        return request
    train, _ = split_window(read_frame(path / "snapshot.jsonl"), request["start"], request["test"])
    spec = json.loads((path / "preprocessor.json").read_text(encoding="utf-8"))
    model = CatBoostClassifier(iterations=positive_int("TRAIN_ITERATIONS",100), depth=5, learning_rate=0.1,
                               loss_function="Logloss", random_seed=42, thread_count=2,
                               verbose=False, allow_writing_files=False)
    model.fit(prepare_features(train, spec), train["target"].astype(int), cat_features=CATEGORIES)
    temporary = path / "candidate.tmp"
    model.save_model(str(temporary))
    os.replace(temporary, path / "candidate.bin")
    return request

def metrics(targets, scores):
    if set(targets) != {0,1}:
        raise ValueError("Both classes are required for evaluation")
    result = {"average_precision": float(average_precision_score(targets, scores)),
              "roc_auc": float(roc_auc_score(targets, scores))}
    if not all(np.isfinite(value) for value in result.values()):
        raise ValueError("Nonfinite evaluation metrics")
    return result

def is_acceptable(candidate, baseline):
    return baseline is None or all(candidate[name] + 1e-8 >= baseline[name] for name in ("average_precision","roc_auc"))

def evaluate_candidate(request):
    """Score candidate and pinned baseline on identical test rows; persist the decision once.
    A retry never compares against a later active version or another month's stored metrics.
    """
    path = directory(request)
    if (path / "decision.json").exists():
        return request
    _, test = split_window(read_frame(path / "snapshot.jsonl"), request["start"], request["test"])
    spec = json.loads((path / "preprocessor.json").read_text(encoding="utf-8"))
    model = CatBoostClassifier()
    model.load_model(str(path / "candidate.bin"))
    candidate = metrics(test["target"], model.predict_proba(prepare_features(test, spec), thread_count=2)[:,1])
    baseline = None
    if request["baseline"]:
        previous, previous_spec, _ = load_bundle(models_root(), request["baseline"])
        baseline = metrics(test["target"], previous.predict_proba(prepare_features(test, previous_spec), thread_count=2)[:,1])
    atomic_json(path / "decision.json", {"candidate": candidate, "baseline": baseline,
                "baseline_version": request["baseline"], "accepted": is_acceptable(candidate, baseline),
                "test_month": request["test"], "test_rows": len(test)})
    return request

def save_candidate(request):
    """Seal candidate, specification, window and decision before any cursor change.
    A completed immutable version is validated and reused on retry.
    """
    path = directory(request)
    decision = json.loads((path / "decision.json").read_text(encoding="utf-8"))
    spec = json.loads((path / "preprocessor.json").read_text(encoding="utf-8"))
    from common.registry import digest
    save_bundle(models_root(), request["version"], path / "candidate.bin", spec,
                {"window_start": request["start"], "test_month": request["test"],
                 "decision": decision, "snapshot_sha256": digest(path / "snapshot.jsonl"),
                 "dataset": json.loads((path / "dataset.json").read_text(encoding="utf-8")),
                 "saved_at": datetime.now(timezone.utc).isoformat()})
    with connect() as connection:
        connection.execute("UPDATE training_runs SET status='saved' WHERE version=%s AND status='reserved'", (request["version"],))
    return request

def publish_candidate(request):
    """Validate a sealed bundle and publish only if accepted; rejected candidates stay archived.
    A DB lock protects the active pointer from stale retries after newer windows complete.
    """
    _, _, metadata = load_bundle(models_root(), request["version"])
    with connect() as connection:
        lock(connection)
        row = connection.execute("SELECT status FROM training_runs WHERE version=%s", (request["version"],)).fetchone()
        if row["status"] == "complete":
            return request
        state = connection.execute("SELECT pending_version FROM training_state WHERE singleton").fetchone()
        if state["pending_version"] != request["version"]:
            raise ValueError("Stale publication request")
        if metadata["decision"]["accepted"]:
            publish(models_root(), request["version"])
        connection.execute("UPDATE training_runs SET status='published' WHERE version=%s", (request["version"],))
    return request

def finish_month(request):
    """Commit the processed month only after a sealed decision and successful publication path.
    Retry after a filesystem/DB boundary recovers the same version and never shifts twice.
    """
    _, _, metadata = load_bundle(models_root(), request["version"])
    with connect() as connection:
        lock(connection)
        row = connection.execute("SELECT status FROM training_runs WHERE version=%s FOR UPDATE", (request["version"],)).fetchone()
        if row["status"] == "complete":
            return request
        if row["status"] != "published":
            raise ValueError("Publication task has not completed")
        if metadata["decision"]["accepted"] and active_version(models_root()) != request["version"]:
            raise ValueError("Accepted version is not active")
        state = connection.execute("SELECT * FROM training_state WHERE singleton FOR UPDATE").fetchone()
        if state["pending_version"] != request["version"] or state["next_test_month"].isoformat() != request["test"]:
            raise ValueError("Training state changed unexpectedly")
        connection.execute("UPDATE training_state SET next_test_month=%s,pending_version=NULL WHERE singleton",
                           (add_months(request["test"],1).date(),))
        connection.execute("UPDATE training_runs SET status='complete',completed_at=now() WHERE version=%s", (request["version"],))
    return request

def run_training(run_id):
    """CLI orchestration for the same task functions used by the DAG; not a substitute DAG."""
    request = reserve_window(run_id)
    for function in (prepare_dataset, train_candidate, evaluate_candidate, save_candidate, publish_candidate, finish_month):
        request = function(request)
    return request
