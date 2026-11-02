"""Score with a validated immutable bundle and reload without restarting the process."""
import logging
import math
from pathlib import Path
from common.features import prepare_features
from common.registry import active_version, load_bundle

logger = logging.getLogger(__name__)

class ModelNotReady(RuntimeError):
    pass

class Scorer:
    def __init__(self, models_path, threshold=0.98):
        if not 0 <= threshold <= 1:
            raise ValueError("FRAUD_THRESHOLD must be between 0 and 1")
        self.root = Path(models_path)
        self.threshold = threshold
        self.model = self.artifact = self.version = None
        self._failed_version = None

    def refresh(self):
        try:
            version = active_version(self.root)
            if version is None:
                raise FileNotFoundError("No active bundle yet")
            if version != self.version:
                # Swap references only after the entire replacement passes validation.
                model, artifact, _ = load_bundle(self.root, version)
                self.model, self.artifact, self.version = model, artifact, version
                self._failed_version = None
                logger.info("Activated model_version=%s", version)
        except Exception as error:
            # Deserialization libraries can raise their own exception classes.
            key = str(error)
            if self._failed_version != key:
                logger.warning("Cannot reload model bundle: %s", error)
                self._failed_version = key
            if self.model is None:
                raise ModelNotReady("Waiting for the first valid published model") from error
        return self.version

    def score(self, transaction):
        if not isinstance(transaction, dict):
            raise ValueError("Transaction must be an object")
        transaction_id = transaction.get("transaction_id")
        if not isinstance(transaction_id, str) or not transaction_id.strip():
            raise ValueError("transaction_id must be a nonempty string")
        data = transaction.get("data")
        if not isinstance(data, dict):
            raise ValueError("data must be an object")
        self.refresh()
        features = prepare_features(data, self.artifact)
        score = float(self.model.predict_proba(features, thread_count=1)[0,1])
        if not math.isfinite(score) or not 0 <= score <= 1:
            raise ValueError("Model returned an invalid score")
        return {"transaction_id": transaction_id, "score": score,
                "fraud_flag": int(score > self.threshold), "model_version": self.version}
