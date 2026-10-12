"""Export the SQL result and verify every ingested record against the CSV."""
import argparse
import csv
import hashlib
import itertools
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from common.analytics import BUSINESS_COLUMNS, file_hash, save_json
from common.schema import INPUT_COLUMNS
from scripts.clickhouse_support import get_client, identifier, query_file, wait_dataset


def reference_records(path, manifest):
    """Independent oracle: does not call the producer's parser or ID function."""
    with Path(path).open(encoding="utf-8-sig", newline="") as source:
        for index, row in enumerate(csv.DictReader(source), 1):
            if index > manifest["selected_rows"]:
                break
            txid = hashlib.sha256((manifest["dataset_id"] + ":" + str(index)).encode()).hexdigest()
            values = []
            for name in INPUT_COLUMNS:
                value = row[name]
                if name == "amount":
                    value = Decimal(value)
                elif name == "transaction_time":
                    value = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
                elif name in ("lat", "lon", "merchant_lat", "merchant_lon"):
                    value = float(value)
                elif name == "population_city":
                    value = int(value)
                elif name in ("us_state", "gender"):
                    value = value.strip().upper()
                elif name == "cat_id":
                    value = value.strip()
                values.append(value)
            target = int(row["target"]) if "target" in row else None
            yield (index, txid, *values, target)


def reference_winners(path, manifest):
    winners = {}
    positions = {name: i for i, name in enumerate(BUSINESS_COLUMNS)}
    for record in reference_records(path, manifest):
        state, category = record[positions["us_state"]], record[positions["cat_id"]]
        amount, txid = record[positions["amount"]], record[positions["transaction_id"]]
        candidate = (amount, txid, category)
        if state not in winners or candidate[:2] > winners[state][:2]:
            winners[state] = candidate
    return [(state, winners[state][2], winners[state][0], winners[state][1]) for state in sorted(winners)]


def verify_records(client, path, manifest, database):
    identifier(database)
    query = f"SELECT DISTINCT {', '.join(BUSINESS_COLUMNS)} FROM {database}.transactions " \
            "WHERE dataset_id = {dataset_id:String} ORDER BY row_index, transaction_id"
    count = 0
    with client.query_rows_stream(
        query, parameters={"dataset_id": manifest["dataset_id"]}, query_tz="UTC",
        settings={"max_threads": 2, "max_memory_usage": 536870912, "max_block_size": 8192},
    ) as stream:
        missing = object()
        for actual, expected in itertools.zip_longest(stream, reference_records(path, manifest), fillvalue=missing):
            if actual is missing or expected is missing:
                raise AssertionError("CSV and unique stored records differ in length")
            actual = list(actual)
            time_index = BUSINESS_COLUMNS.index('transaction_time')
            if actual[time_index].tzinfo is None:
                actual[time_index] = actual[time_index].replace(tzinfo=timezone.utc)
            if tuple(actual) != tuple(expected):
                fields = [name for name, a, b in zip(BUSINESS_COLUMNS, actual, expected) if a != b]
                raise AssertionError(f"Record {expected[0]} differs in fields: {fields}")
            count += 1
    if count != manifest["selected_rows"]:
        raise AssertionError("Unexpected reference row count")
    return count


def export_results(manifest_path, output, *, csv_path=None, database="fraud_analytics", timeout=120, client=None):
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if manifest.get("status") != "success" or manifest["confirmed_messages"] != manifest["selected_rows"]:
        raise ValueError("Export requires a successful complete load manifest")
    path = Path(csv_path or manifest["source_file"])
    if file_hash(path) != manifest["source_sha256"]:
        raise ValueError("Source CSV differs from load manifest")
    own_client = client is None
    client = client or get_client()
    try:
        counts = wait_dataset(client, manifest["dataset_id"], manifest["selected_rows"], database, timeout)
        verified = verify_records(client, path, manifest, database)
        parameters = {"dataset_id": manifest["dataset_id"], "database": identifier(database)}
        raw_sql = query_file("003_max_transaction_by_state_raw.sql")
        optimized_sql = query_file("004_max_transaction_by_state.sql")
        expected = reference_winners(path, manifest)
        for sql in (raw_sql, optimized_sql):
            actual = [tuple(row) for row in client.query(sql, parameters=parameters).result_rows]
            if actual != expected:
                raise AssertionError("SQL maxima differ from independent CSV reference")
        content = client.raw_query(optimized_sql, parameters=parameters, fmt="CSVWithNames")
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(output.name + ".tmp")
        temporary.write_bytes(content)
        with temporary.open(encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames != ["us_state", "cat_id", "max_amount", "transaction_id"]:
                raise AssertionError("Unexpected CSV header")
            exported = [(row["us_state"], row["cat_id"], Decimal(row["max_amount"]), row["transaction_id"]) for row in reader]
        if exported != expected:
            raise AssertionError("Exported CSV differs from reference")
        temporary.replace(output)
        metadata = {
            "dataset_id": manifest["dataset_id"], "source_file": manifest["source_file"],
            "source_sha256": manifest["source_sha256"], "selected_rows": manifest["selected_rows"],
            "verified_unique_records": verified, "regions": len(expected), **counts,
            "sql_sha256": hashlib.sha256(optimized_sql.encode()).hexdigest(),
            "csv_sha256": hashlib.sha256(content).hexdigest(),
            "clickhouse_version": client.query("SELECT version()").first_row[0],
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "tie_rule": "maximum (amount, transaction_id)", "independent_reference_matches": True,
        }
        save_json(output.with_suffix(".metadata.json"), metadata)
        return metadata
    finally:
        if own_client:
            client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("reports/load_manifest.json"))
    parser.add_argument("--output", type=Path, default=Path("results/max_transaction_by_state.csv"))
    parser.add_argument("--csv", type=Path)
    parser.add_argument("--database", default="fraud_analytics")
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    print(json.dumps(export_results(args.manifest, args.output, csv_path=args.csv,
                                   database=args.database, timeout=args.timeout), indent=2))


if __name__ == "__main__":
    main()
