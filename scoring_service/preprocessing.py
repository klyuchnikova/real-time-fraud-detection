"""Turn one transaction into the features expected by the seminar model."""

import json
import math
from pathlib import Path

import pandas as pd
from geopy.distance import great_circle

from common.schema import INPUT_COLUMNS

CATEGORY_COLUMNS = ("gender", "merch", "cat_id", "one_city", "us_state", "jobs")
TIME_COLUMNS = ("hour", "year", "month", "day_of_month", "day_of_week")
MODEL_CATEGORIES = tuple(f"{name}_cat" for name in CATEGORY_COLUMNS) + TIME_COLUMNS
NUMERIC_COLUMNS = ("amount", "population_city", "distance")


def load_artifact(path: str | Path) -> dict:
    with open(path, encoding="utf-8") as file:
        artifact = json.load(file)
    if artifact.get("version") != 1:
        raise ValueError("Unsupported preprocessor artifact version")
    return artifact


def _number(data: dict, name: str) -> float:
    value = data[name]
    if value is None or value == "":
        return math.nan
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a finite number")
    return number


def prepare_features(data: dict, artifact: dict) -> pd.DataFrame:
    missing = set(INPUT_COLUMNS) - set(data)
    if missing:
        raise ValueError(f"Missing fields: {', '.join(sorted(missing))}")

    if not isinstance(data["transaction_time"], str) or not data["transaction_time"].strip():
        raise ValueError("transaction_time must be a nonempty string")
    timestamp = pd.to_datetime(data["transaction_time"], errors="raise")
    if pd.isna(timestamp):
        raise ValueError("transaction_time is missing")
    values: dict[str, object] = {
        "hour": int(timestamp.hour),
        "year": int(timestamp.year),
        "month": int(timestamp.month),
        "day_of_month": int(timestamp.day),
        "day_of_week": int(timestamp.dayofweek),
    }

    for name in CATEGORY_COLUMNS:
        raw = data[name]
        mapped = artifact["category_maps"][name].get(str(raw), "cat_NAN")
        values[f"{name}_cat"] = mapped

    for name in MODEL_CATEGORIES:
        key = str(values[name])
        values[f"{name}_mean_enc"] = artifact["target_means"][name].get(key, math.nan)

    latitude = _number(data, "lat")
    longitude = _number(data, "lon")
    merchant_latitude = _number(data, "merchant_lat")
    merchant_longitude = _number(data, "merchant_lon")
    if any(math.isnan(x) for x in (latitude, longitude, merchant_latitude, merchant_longitude)):
        raise ValueError("Coordinates are required")
    distance = great_circle(
        (latitude, longitude), (merchant_latitude, merchant_longitude)
    ).km
    numbers = {
        "amount": _number(data, "amount"),
        "population_city": _number(data, "population_city"),
        "distance": distance,
    }
    for name, number in numbers.items():
        if math.isnan(number):
            number = artifact["numeric_means"][name]
        if number < 0:
            raise ValueError(f"{name} must not be negative")
        values[f"{name}_log"] = math.log1p(number)

    for name in MODEL_CATEGORIES:
        values[name] = str(values[name])
    columns = artifact["feature_names"]
    if set(columns) != set(values):
        raise ValueError("Preprocessor does not match the model features")
    return pd.DataFrame([[values[name] for name in columns]], columns=columns)
