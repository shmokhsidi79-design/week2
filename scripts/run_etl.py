import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from bootcamp_data.etl import ETLConfig, run_etl


def main() -> None:
    cfg = ETLConfig(
        raw_orders=ROOT / "data" / "raw" / "orders.csv",
        raw_users=ROOT / "data" / "raw" / "users.csv",
        orders_clean=ROOT / "data" / "processed" / "orders_clean.parquet",
        users_clean=ROOT / "data" / "processed" / "users.parquet",
        analytics_table=ROOT / "data" / "processed" / "analytics_table.parquet",
        run_meta=ROOT / "data" / "processed" / "run_meta.json",
    )
    run_etl(cfg)


if __name__ == "__main__":
    main()
