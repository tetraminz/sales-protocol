"""Main demo entrypoint: ingest -> scan(evaluator+judge) -> report."""

from dialogs.demo import run_demo


if __name__ == "__main__":
    result = run_demo(db_path="dialogs.db", csv_dir="csv")
    print(f"demo_ok run={result['run_id']} canonical={result['canonical_run_id']}")
