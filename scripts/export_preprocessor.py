"""Freeze the seminar preprocessing statistics for inference-only containers."""

import argparse
import hashlib
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from geopy.distance import great_circle


from scoring_service.preprocessing import CATEGORY_COLUMNS, MODEL_CATEGORIES, NUMERIC_COLUMNS


ROOT = Path(__file__).resolve().parents[1]


def add_time_features(frame: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(frame["transaction_time"]).dt
    frame["hour"] = dt.hour
    frame["year"] = dt.year
    frame["month"] = dt.month
    frame["day_of_month"] = dt.day
    frame["day_of_week"] = dt.dayofweek
    return frame.drop(columns="transaction_time")


def build_artifact(train_zip: Path, model_path: Path) -> dict:
    with zipfile.ZipFile(train_zip) as archive, archive.open("train.csv") as source:
        train = pd.read_csv(source).drop(columns=["name_1", "name_2", "street", "post_code"])
    row_count = len(train)
    train = add_time_features(train)
    category_maps = {}
    for name in CATEGORY_COLUMNS:
        counts = (
            train.groupby(name, dropna=False)[["target"]]
            .count()
            .sort_values("target", ascending=False)
            .reset_index()
            .set_axis([name, "count"], axis=1)
            .reset_index()
        )
        counts["index"] = counts.apply(
            lambda row: np.nan if pd.isna(row[name]) else row["index"], axis=1
        )
        labels = [
            "cat_NAN" if pd.isna(value) else
            f"cat_{int(value)}" if value < 50 else "cat_50+"
            for value in counts["index"]
        ]
        category_maps[name] = {
            str(raw): label for raw, label in zip(counts[name], labels) if pd.notna(raw)
        }
        train[f"{name}_cat"] = train[name].map(category_maps[name])

    target_means = {}
    for name in MODEL_CATEGORIES:
        means = train.groupby(name)["target"].mean()
        target_means[name] = {str(key): float(value) for key, value in means.items()}

    train["distance"] = train.apply(
        lambda row: great_circle(
            (row["lat"], row["lon"]), (row["merchant_lat"], row["merchant_lon"])
        ).km,
        axis=1,
    )
    numeric_means = {name: float(train[name].mean()) for name in NUMERIC_COLUMNS}
    model = CatBoostClassifier()
    model.load_model(str(model_path))
    return {
        "version": 1,
        "source": "NikitaMalykhin/mts25_mlops_hw2_real_time_fraud_detection@41540d91",
        "train_rows": row_count,
        "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
        "feature_names": model.feature_names_,
        "category_maps": category_maps,
        "target_means": target_means,
        "numeric_means": numeric_means,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=ROOT / "teta-ml-1-2025.zip")
    parser.add_argument("--model", type=Path, default=ROOT / "artifacts" / "my_catboost.cbm")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "preprocessor.json")
    args = parser.parse_args()
    artifact = build_artifact(args.data, args.model)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(f"Saved {args.output} ({artifact['train_rows']} training rows)")


if __name__ == "__main__":
    main()
