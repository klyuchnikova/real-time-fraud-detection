"""Own feature transformation, identical for batch training and online scoring."""
import numpy as np
import pandas as pd
from common.schema import INPUT_COLUMNS

CATEGORIES = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
NUMBERS = ["amount_log", "population_log", "distance_km", "hour_sin", "hour_cos",
           "month_sin", "month_cos", "day_of_week", "day_of_month", "is_weekend"]
FEATURE_NAMES = CATEGORIES + NUMBERS

def fit_preprocessor(frame):
    """Fit missing numeric defaults only from the train partition; no target statistics."""
    defaults = {}
    for column in ("amount", "population_city"):
        values = pd.to_numeric(frame[column], errors="raise")
        finite = values[np.isfinite(values)]
        if finite.empty or (finite < 0).any():
            raise ValueError(f"Invalid train values: {column}")
        defaults[column] = float(finite.median())
    return {"schema_version": 2, "feature_names": FEATURE_NAMES,
            "categorical_features": CATEGORIES, "numeric_defaults": defaults}

def prepare_features(data, artifact):
    if artifact.get("schema_version") != 2 or artifact.get("feature_names") != FEATURE_NAMES:
        raise ValueError("Unsupported feature specification")
    frame = pd.DataFrame([data]) if isinstance(data, dict) else data.copy()
    missing = set(INPUT_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"Missing fields: {', '.join(sorted(missing))}")
    times = pd.to_datetime(frame["transaction_time"], errors="raise", utc=True)
    if times.isna().any():
        raise ValueError("Missing transaction_time")
    result = pd.DataFrame(index=frame.index)
    for name in CATEGORIES:
        result[name] = frame[name].fillna("__MISSING__").astype(str).str.strip().replace("", "__MISSING__")
    for source, destination in (("amount", "amount_log"), ("population_city", "population_log")):
        values = pd.to_numeric(frame[source], errors="raise").fillna(artifact["numeric_defaults"][source])
        if not np.isfinite(values).all() or (values < 0).any():
            raise ValueError(f"{source} must be finite and nonnegative")
        result[destination] = np.log1p(values)
    coordinates = []
    for name in ("lat", "lon", "merchant_lat", "merchant_lon"):
        values = pd.to_numeric(frame[name], errors="raise")
        bound = 90 if name.endswith("lat") else 180
        if not np.isfinite(values).all() or (values.abs() > bound).any():
            raise ValueError(f"Invalid coordinates: {name}")
        coordinates.append(np.radians(values.to_numpy(dtype=float)))
    lat, lon, mlat, mlon = coordinates
    haversine = np.sin((mlat-lat)/2)**2 + np.cos(lat)*np.cos(mlat)*np.sin((mlon-lon)/2)**2
    result["distance_km"] = 6371.0088 * 2 * np.arcsin(np.sqrt(np.clip(haversine, 0, 1)))
    hours = times.dt.hour + times.dt.minute / 60
    result["hour_sin"] = np.sin(2*np.pi*hours/24)
    result["hour_cos"] = np.cos(2*np.pi*hours/24)
    result["month_sin"] = np.sin(2*np.pi*(times.dt.month-1)/12)
    result["month_cos"] = np.cos(2*np.pi*(times.dt.month-1)/12)
    result["day_of_week"] = times.dt.dayofweek
    result["day_of_month"] = times.dt.day
    result["is_weekend"] = (times.dt.dayofweek >= 5).astype(int)
    return result[FEATURE_NAMES]
