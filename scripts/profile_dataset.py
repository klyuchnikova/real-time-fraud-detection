"""Profile competition CSVs without exporting personal records."""
import argparse
import csv
import hashlib
import json
import math
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from common.schema import INPUT_COLUMNS


def profile_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    counts = Counter()
    state_counts, category_counts, target_counts = Counter(), Counter(), Counter()
    category_positive = Counter()
    amount_min = amount_max = date_min = date_max = None
    amount_sum, max_scale, invalid_amount, invalid_time = Decimal(0), 0, 0, 0
    malformed_rows, row_count = 0, 0
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        columns = reader.fieldnames or []
        for row in reader:
            row_count += 1
            if None in row or any(value is None for value in row.values()):
                malformed_rows += 1
                continue
            for column in columns:
                if not row[column].strip():
                    counts[column] += 1
            if "us_state" in row:
                state_counts[row["us_state"]] += 1
            if "cat_id" in row:
                category_counts[row["cat_id"]] += 1
            if "target" in row:
                target_counts[row["target"]] += 1
                if row["target"] == "1" and "cat_id" in row:
                    category_positive[row["cat_id"]] += 1
            if "amount" in row:
                try:
                    amount = Decimal(row["amount"])
                    if not amount.is_finite():
                        raise ValueError("nonfinite")
                    max_scale = max(max_scale, max(0, -amount.as_tuple().exponent))
                    amount_min = amount if amount_min is None else min(amount_min, amount)
                    amount_max = amount if amount_max is None else max(amount_max, amount)
                    amount_sum += amount
                except (InvalidOperation, ValueError):
                    invalid_amount += 1
            if "transaction_time" in row:
                try:
                    timestamp = datetime.fromisoformat(row["transaction_time"])
                    date_min = timestamp if date_min is None else min(date_min, timestamp)
                    date_max = timestamp if date_max is None else max(date_max, timestamp)
                except (ValueError, TypeError):
                    invalid_time += 1
    result = {
        "file": path.name, "size_bytes": path.stat().st_size, "sha256": digest.hexdigest(),
        "rows": row_count, "columns": columns, "missing_by_column": dict(counts),
        "malformed_rows": malformed_rows,
        "matches_hw1_input_columns": set(columns) == set(INPUT_COLUMNS),
        "matches_hw1_training_columns": set(columns) == set(INPUT_COLUMNS) | {"target"},
    }
    if state_counts:
        result.update(states=sorted(state_counts), state_count=len(state_counts),
                      category_count=len(category_counts), categories=sorted(category_counts))
    if "target" in columns:
        result["target_counts"] = dict(target_counts)
        result["target_mean_by_category"] = {
            cat: category_positive[cat] / count for cat, count in category_counts.items()
        }
    if "amount" in columns:
        valid_amount_count = row_count - invalid_amount - malformed_rows
        result["amount"] = {
            "min": str(amount_min), "max": str(amount_max), "max_decimal_places": max_scale,
            "invalid": invalid_amount, "mean": str(amount_sum / valid_amount_count) if valid_amount_count else None,
        }
    if "transaction_time" in columns:
        result["transaction_time"] = {
            "min": date_min.isoformat() if date_min else None,
            "max": date_max.isoformat() if date_max else None, "invalid": invalid_time,
            "timezone_in_min": str(date_min.tzinfo) if date_min else None,
        }
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("teta-ml-1-2025"))
    parser.add_argument("--output", type=Path, default=Path("reports/dataset_profile.json"))
    args = parser.parse_args()
    paths = sorted(args.data_dir.glob("*.csv"))
    if not paths:
        parser.error(f"No CSV files in {args.data_dir}")
    profiles = [profile_file(path) for path in paths]
    artifact = json.loads(Path("artifacts/preprocessor.json").read_text(encoding="utf-8"))
    train = next((item for item in profiles if item["file"] == "train.csv"), None)
    comparison = {
        "original_csv_hash_available": False,
        "note": "Schema and stored training statistics can be compared; original CSV hash is not stored in hw1.",
    }
    if train:
        actual_means = train["target_mean_by_category"]
        mapping = artifact["category_maps"]["cat_id"]
        expected_means = artifact["target_means"]["cat_id_cat"]
        comparison.update(
            train_rows_match_artifact=train["rows"] == artifact["train_rows"],
            categories_match_artifact=set(actual_means) == set(mapping),
            category_target_means_match_artifact=all(
                math.isclose(mean, expected_means[mapping[cat]], abs_tol=1e-14, rel_tol=0)
                for cat, mean in actual_means.items() if cat in mapping
            ) and set(actual_means) == set(mapping),
        )
        expected_amount = artifact["numeric_means"].get("amount")
        if expected_amount is not None:
            comparison["amount_mean_match_artifact"] = math.isclose(
                float(train["amount"]["mean"]), expected_amount, rel_tol=1e-12
            )
    report = {
        "source": "https://www.kaggle.com/competitions/teta-ml-1-2025",
        "files": profiles, "comparison_with_hw1": comparison,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
