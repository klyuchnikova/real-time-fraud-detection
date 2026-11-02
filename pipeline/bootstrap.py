"""One-time initial version through the exact same functions used in the retraining DAG."""
from common.registry import active_version, load_bundle
from pipeline.config import models_root
from pipeline.retrain import run_training

def bootstrap():
    """Validate the current version or resume the durable initial training reservation."""
    version = active_version(models_root())
    if version:
        load_bundle(models_root(), version)
        return
    run_training("bootstrap")

if __name__ == "__main__":
    bootstrap()
