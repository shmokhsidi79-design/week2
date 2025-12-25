from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from data_workflow.transforms import (
    parse_datetime,
    add_time_parts,
    winsorize,
)
from data_workflow.joins import safe_left_join
from data_workflow.quality import assert_unique_key, require_columns, assert_non_empty

try:
    from data_workflow.transforms import add_outlier_flag
except ImportError:
    add_outlier_flag = None


def main():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)

    data_path = ROOT / "data" / "processed"

    orders = pd.read_parquet(data_path / "orders_clean.parquet")
    users = pd.read_parquet(data_path / "users.parquet")

    require_columns(
        orders,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"],
    )
    require_columns(users, ["user_id", "country"])

    assert_non_empty(orders, "orders")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

   
    orders = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    log.info("missing created_at: %s", orders["created_at"].isna().sum())

   
    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    if len(joined) != len(orders):
        raise AssertionError("Row count changed after join!")

   
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))

    if add_outlier_flag:
        joined = add_outlier_flag(joined, "amount")

  
    out_path = data_path / "analytics_table.parquet"
    joined.to_parquet(out_path, index=False)

    log.info("Saved analytics table → %s", out_path)


if __name__ == "__main__":
    main()