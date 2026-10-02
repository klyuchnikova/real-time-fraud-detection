"""Load the trained model and score one prepared transaction."""

import hashlib
import math
from pathlib import Path

from catboost import CatBoostClassifier

from scoring_service.preprocessing import load_artifact, prepare_features


class Scorer:
    def __init__(self, model_path: str, artifact_path: str, threshold: float = 0.98):
        if not 0 <= threshold <= 1:
            raise ValueError("FRAUD_THRESHOLD must be between 0 and 1")
        self.model = CatBoostClassifier()
        self.model.load_model(model_path)
        self.artifact = load_artifact(artifact_path)
        digest = hashlib.sha256(Path(model_path).read_bytes()).hexdigest()
        if self.artifact["model_sha256"] != digest:
            raise ValueError("Preprocessor artifact was made for another model")
        if self.artifact["feature_names"] != self.model.feature_names_:
            raise ValueError("Model feature names do not match the preprocessor")
        self.threshold = threshold

    def score(self, transaction: dict) -> dict:
        if not isinstance(transaction, dict):
            raise ValueError("Transaction must be an object")
        transaction_id = transaction.get("transaction_id")
        if not isinstance(transaction_id, str) or not transaction_id:
            raise ValueError("transaction_id must be a nonempty string")
        data = transaction.get("data")
        if not isinstance(data, dict):
            raise ValueError("data must be an object")
        features = prepare_features(data, self.artifact)
        score = float(self.model.predict_proba(features, thread_count=1)[0, 1])
        if not math.isfinite(score) or not 0 <= score <= 1:
            raise ValueError("Model returned an invalid score")
        return {
            "transaction_id": transaction_id,
            "score": score,
            "fraud_flag": int(score > self.threshold),
        }
