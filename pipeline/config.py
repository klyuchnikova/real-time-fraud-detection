"""Environment configuration shared by CLI and Airflow tasks."""
import os
from pathlib import Path

def artifacts_root():
    return Path(os.getenv("ARTIFACTS_ROOT", "/shared"))

def models_root():
    return artifacts_root() / "models"

def work_root():
    path = artifacts_root() / "work"
    path.mkdir(parents=True, exist_ok=True)
    return path

def positive_int(name, default):
    value = int(os.getenv(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value
