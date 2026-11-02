"""Immutable, checksummed bundles and an atomically replaced active pointer."""
import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from catboost import CatBoostClassifier
from common.features import FEATURE_NAMES, CATEGORIES

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, allow_nan=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)

def valid_version(version):
    if not isinstance(version, str) or not re.fullmatch(r"[a-z0-9_-]{1,100}", version):
        raise ValueError("Invalid model version")
    return version

def active_version(root):
    path = Path(root) / "active.json"
    if not path.exists():
        return None
    return valid_version(json.loads(path.read_text(encoding="utf-8"))["model_version"])

def load_bundle(root, version):
    version = valid_version(version)
    directory = Path(root) / "versions" / version
    metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
    if metadata["model_version"] != version:
        raise ValueError("Version mismatch")
    for name in ("model.bin", "preprocessor.json"):
        if digest(directory / name) != metadata["sha256"][name]:
            raise ValueError(f"Checksum mismatch: {name}")
    spec = json.loads((directory / "preprocessor.json").read_text(encoding="utf-8"))
    if spec.get("schema_version") != 2 or spec.get("feature_names") != FEATURE_NAMES or spec.get("categorical_features") != CATEGORIES:
        raise ValueError("Incompatible feature schema")
    model = CatBoostClassifier()
    model.load_model(str(directory / "model.bin"))
    if model.feature_names_ != FEATURE_NAMES:
        raise ValueError("Model feature names mismatch")
    if model.get_cat_feature_indices() != list(range(len(CATEGORIES))):
        raise ValueError("Model categorical features mismatch")
    return model, spec, metadata

def save_bundle(root, version, model_path, specification, metadata):
    """Seal one candidate. A retry validates and reuses an already sealed bundle."""
    valid_version(version)
    parent = Path(root) / "versions"
    parent.mkdir(parents=True, exist_ok=True)
    destination = parent / version
    if destination.exists():
        load_bundle(root, version)
        return str(destination)
    temporary = Path(tempfile.mkdtemp(prefix=".candidate-", dir=parent))
    try:
        shutil.copyfile(model_path, temporary / "model.bin")
        atomic_json(temporary / "preprocessor.json", specification)
        record = {**metadata, "model_version": version,
                  "sha256": {name: digest(temporary / name) for name in ("model.bin", "preprocessor.json")}}
        atomic_json(temporary / "metadata.json", record)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    load_bundle(root, version)
    return str(destination)

def publish(root, version):
    """Publish only a fully readable immutable bundle; repeated publication is harmless."""
    load_bundle(root, version)
    atomic_json(Path(root) / "active.json", {"model_version": version})
