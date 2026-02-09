"""Executive demo entrypoint: run full baseline vs SGR flow in one command."""

from dialogs.demo import run_demo


if __name__ == "__main__":
    result = run_demo(db_path="dialogs.db", csv_dir="csv")
    print(f"demo_ok baseline={result['baseline_run']} sgr={result['sgr_run']}")
