"""Compare baseline, optimized raw storage and the aggregate using query_log."""
import argparse
import csv
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from common.analytics import ALL_COLUMNS, save_json
from scripts.clickhouse_support import SQL_DIR, execute_ddl, get_client, query_file, render_ddl, wait_dataset


def write_csv(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def benchmark(manifest_path, runs=7, merge=False):
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if manifest.get("status") != "success":
        raise ValueError("Successful load manifest required")
    client = get_client()
    try:
        counts = wait_dataset(client, manifest["dataset_id"], manifest["selected_rows"])
        execute_ddl(client, render_ddl(SQL_DIR / "001_baseline.sql"))
        # This table is exclusively a reproducible benchmark copy, never an ingestion destination.
        client.command("TRUNCATE TABLE fraud_analytics.transactions_baseline")
        client.command(f"INSERT INTO fraud_analytics.transactions_baseline ({', '.join(ALL_COLUMNS)}) "
                       f"SELECT {', '.join(ALL_COLUMNS)} FROM fraud_analytics.transactions "
                       "WHERE dataset_id={dataset_id:String}",
                       parameters={"dataset_id": manifest["dataset_id"]})
        baseline_count = client.query("SELECT count() FROM fraud_analytics.transactions_baseline").first_row[0]
        if baseline_count != counts["raw_rows"]:
            raise AssertionError("Baseline row count differs")
        if merge:
            for table in ("transactions_baseline", "transactions", "state_max"):
                client.command(f"OPTIMIZE TABLE fraud_analytics.{table} FINAL")
        parameters = {"dataset_id": manifest["dataset_id"], "database": "fraud_analytics"}
        raw = query_file("003_max_transaction_by_state_raw.sql")
        queries = {"baseline": raw.replace(".transactions", ".transactions_baseline"),
                   "optimized_raw": raw,
                   "aggregate": query_file("004_max_transaction_by_state.sql")}
        settings = {"use_query_cache": 0, "max_threads": 2, "max_memory_usage": 536870912,
                    "log_queries": 1, "log_queries_probability": 1}
        expected = None
        for name, sql in queries.items():
            result = client.query(sql, parameters=parameters, settings=settings).result_rows
            if expected is None:
                expected = result
            elif result != expected:
                raise AssertionError(f"{name} differs from baseline")
        records = []
        names = list(queries)
        for iteration in range(runs):
            # Rotate ordering to reduce systematic position effects.
            for name in names[iteration % 3:] + names[:iteration % 3]:
                start = time.perf_counter()
                result = client.query(queries[name], parameters=parameters, settings=settings)
                elapsed = (time.perf_counter() - start) * 1000
                if result.result_rows != expected:
                    raise AssertionError(f"{name} changed during measurements")
                records.append({"variant": name, "run": iteration + 1, "query_id": result.query_id,
                                "client_ms": round(elapsed, 3)})
        client.command("SYSTEM FLUSH LOGS")
        metrics = client.query(
            "SELECT query_id, query_duration_ms, read_rows, read_bytes, memory_usage "
            "FROM system.query_log WHERE type='QueryFinish' AND query_id IN {ids:Array(String)}",
            parameters={"ids": [row["query_id"] for row in records]}).result_rows
        by_id = {row[0]: row[1:] for row in metrics}
        for row in records:
            if row["query_id"] not in by_id:
                raise RuntimeError("query_log entry missing; do not substitute client timing")
            row.update(dict(zip(("server_ms", "read_rows", "read_bytes", "memory_bytes"), by_id[row["query_id"]])))
        write_csv(Path("reports/benchmark.csv"), records,
                  ("variant", "run", "query_id", "client_ms", "server_ms", "read_rows", "read_bytes", "memory_bytes"))
        storage_rows = client.query(
            "SELECT table, sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes), sum(bytes_on_disk), count() "
            "FROM system.parts WHERE active AND database='fraud_analytics' "
            "AND table IN ('transactions_baseline','transactions','state_max') GROUP BY table ORDER BY table").result_rows
        storage = [dict(zip(("table", "rows", "compressed_bytes", "uncompressed_bytes", "bytes_on_disk", "parts"), row))
                   for row in storage_rows]
        write_csv(Path("reports/storage.csv"), storage,
                  ("table", "rows", "compressed_bytes", "uncompressed_bytes", "bytes_on_disk", "parts"))
        summary = {name: {field: statistics.median(row[field] for row in records if row["variant"] == name)
                          for field in ("client_ms", "server_ms", "read_rows", "read_bytes", "memory_bytes")}
                   for name in names}
        report = {
            "dataset_id": manifest["dataset_id"], "source_sha256": manifest["source_sha256"],
            "unique_records": manifest["selected_rows"], "raw_rows": counts["raw_rows"],
            "clickhouse_version": client.query("SELECT version()").first_row[0],
            "runs": runs, "warmups_per_variant": 1, "settings": settings, "final_merge_before_measurement": merge,
            "cache": "warm OS/server caches; query result cache disabled; rotating order",
            "storage_scope": "baseline contains selected dataset; optimized tables include all loaded datasets",
            "created_at": datetime.now(timezone.utc).isoformat(), "median": summary,
            "all_query_results_match": True,
        }
        save_json("reports/benchmark_summary.json", report)
        return report
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("reports/load_manifest.json"))
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--merge", action="store_true", help="Explicitly merge benchmark and optimized tables first")
    args = parser.parse_args()
    if args.runs < 3:
        parser.error("--runs must be at least 3")
    print(json.dumps(benchmark(args.manifest, args.runs, args.merge), indent=2))


if __name__ == "__main__":
    main()
