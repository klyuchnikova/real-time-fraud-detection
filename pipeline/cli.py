"""Manual invocation of the same task modules; Airflow remains the scheduler."""
import argparse
import json
from pipeline.supply import reserve_batch, prepare_batch, insert_batch, prune_history
from pipeline.quality import snapshot_dataset, evaluate_dataset, write_report, assert_report
from pipeline.retrain import run_training

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["supply","retrain","dq"])
    parser.add_argument("--run-id", required=True, help="Reuse exactly this ID to retry a completed or failed operation")
    args = parser.parse_args()
    if args.operation == "supply":
        value = prune_history(insert_batch(prepare_batch(reserve_batch(args.run_id))))
    elif args.operation == "retrain":
        value = run_training(args.run_id)
    else:
        value = assert_report(write_report(evaluate_dataset(snapshot_dataset(args.run_id))))
    print(json.dumps(value, ensure_ascii=False))

if __name__ == "__main__":
    main()
