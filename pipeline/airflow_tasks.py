"""Lightweight Airflow callables: import ML dependencies only inside running tasks."""

def reserve_window(run_id=None):
    """Delegate retrain.reserve_window; input/output and retry semantics stay in that task module."""
    from pipeline.retrain import reserve_window as execute
    return execute(run_id)

def prepare_dataset(request):
    """Delegate retrain.prepare_dataset; input/output and retry semantics stay in that task module."""
    from pipeline.retrain import prepare_dataset as execute
    return execute(request)

def train_candidate(request):
    """Delegate retrain.train_candidate; input/output and retry semantics stay in that task module."""
    from pipeline.retrain import train_candidate as execute
    return execute(request)

def evaluate_candidate(request):
    """Delegate retrain.evaluate_candidate; input/output and retry semantics stay in that task module."""
    from pipeline.retrain import evaluate_candidate as execute
    return execute(request)

def save_candidate(request):
    """Delegate retrain.save_candidate; input/output and retry semantics stay in that task module."""
    from pipeline.retrain import save_candidate as execute
    return execute(request)

def publish_candidate(request):
    """Delegate retrain.publish_candidate; input/output and retry semantics stay in that task module."""
    from pipeline.retrain import publish_candidate as execute
    return execute(request)

def finish_month(request):
    """Delegate retrain.finish_month; input/output and retry semantics stay in that task module."""
    from pipeline.retrain import finish_month as execute
    return execute(request)

def reserve_batch(run_id=None):
    """Delegate supply.reserve_batch; input/output and retry semantics stay in that task module."""
    from pipeline.supply import reserve_batch as execute
    return execute(run_id)

def prepare_batch(request):
    """Delegate supply.prepare_batch; input/output and retry semantics stay in that task module."""
    from pipeline.supply import prepare_batch as execute
    return execute(request)

def insert_batch(request):
    """Delegate supply.insert_batch; input/output and retry semantics stay in that task module."""
    from pipeline.supply import insert_batch as execute
    return execute(request)

def prune_history(request):
    """Delegate supply.prune_history; input/output and retry semantics stay in that task module."""
    from pipeline.supply import prune_history as execute
    return execute(request)

def snapshot_dataset(run_id=None):
    """Delegate quality.snapshot_dataset; input/output and retry semantics stay in that task module."""
    from pipeline.quality import snapshot_dataset as execute
    return execute(run_id)

def evaluate_dataset(request):
    """Delegate quality.evaluate_dataset; input/output and retry semantics stay in that task module."""
    from pipeline.quality import evaluate_dataset as execute
    return execute(request)

def write_report(request):
    """Delegate quality.write_report; input/output and retry semantics stay in that task module."""
    from pipeline.quality import write_report as execute
    return execute(request)

def assert_report(request):
    """Delegate quality.assert_report; input/output and retry semantics stay in that task module."""
    from pipeline.quality import assert_report as execute
    return execute(request)

