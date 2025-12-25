from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import pandas as pd


from data_workflow.io import read1orders, read1users, write1parquet
from data_workflow.transforms import (
    enforce_schema,
    normalize1text,
    add_missing_flags,
    parse_datetime,
    add_time_parts,
    winsorize,
    dedupe_keep_latest,
)


@dataclass(frozen=True)
class ETLConfig:
    raw_orders: Path
    raw_users: Path

    orders_clean: Path
    users_clean: Path
    analytics_table: Path

    run_meta: Path


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders_raw = read1orders(cfg.raw_orders)
    users_raw = read1users(cfg.raw_users)
    return orders_raw, users_raw


def transform(orders: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    
    required_orders = ["order_id", "user_id", "amount", "quantity", "created_at", "status"]
    required_users = ["user_id"]

    missing_orders = [c for c in required_orders if c not in orders.columns]
    if missing_orders:
        raise AssertionError(f"orders missing required columns: {missing_orders}")

    missing_users = [c for c in required_users if c not in users.columns]
    if missing_users:
        raise AssertionError(f"users missing required columns: {missing_users}")

   
    orders2 = enforce_schema(orders)

   
    users2 = users.copy()
    users2["user_id"] = pd.to_numeric(users2["user_id"], errors="coerce").astype("Int64")

    
    status_norm = normalize1text(orders2["status"])
    mapping = {
        "paid": "paid",
        "completed": "paid",
        "shipped": "shipped",
        "cancelled": "cancelled",
        "refunded": "refunded",
        "pending": "pending",
    }
    orders2 = orders2.assign(status=status_norm.map(lambda x: mapping.get(x, x)))

    orders2 = add_missing_flags(orders2, ["amount", "quantity", "status", "created_at"])

   
    orders2 = parse_datetime(orders2, "created_at", utc=True)
    orders2 = add_time_parts(orders2, "created_at")

   
    if users2["user_id"].dropna().duplicated().any():
        if "created_at" in users2.columns:
            users2 = parse_datetime(users2, "created_at", utc=True)
            users2 = dedupe_keep_latest(users2, key_cols=["user_id"], ts_col="created_at")
        else:
            raise AssertionError("users.user_id has duplicates and users has no created_at to dedupe by.")

    before_rows = len(orders2)
    analytics = orders2.merge(users2, on="user_id", how="left", validate="many_to_one", suffixes=("", "_user"))

    if len(analytics) != before_rows:
        raise AssertionError("Row count changed after join (should remain same for left join).")

    w = winsorize(analytics["amount"], lo1=0.01, hi1=0.99)
    analytics = analytics.assign(
        amount_winsor=w,
        is_amount_outlier=analytics["amount"].notna() & (analytics["amount"] != w),
    )

    return analytics


def load_outputs(
    cfg: ETLConfig,
    orders_clean: pd.DataFrame,
    users_clean: pd.DataFrame,
    analytics: pd.DataFrame,) -> None:
    write1parquet(orders_clean, cfg.orders_clean)
    write1parquet(users_clean, cfg.users_clean)
    write1parquet(analytics, cfg.analytics_table)

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)

    meta: dict = {
        "rows": {
            "orders_clean": int(len(orders_clean)),
            "users_clean": int(len(users_clean)),
            "analytics_table": int(len(analytics)),
        },
        "missing_timestamps": {},
        "join_match_rate": {},
        "config_paths": {
            "raw_orders": str(cfg.raw_orders),
            "raw_users": str(cfg.raw_users),
            "orders_clean": str(cfg.orders_clean),
            "users_clean": str(cfg.users_clean),
            "analytics_table": str(cfg.analytics_table),
            "run_meta": str(cfg.run_meta),
        },
    }

  
    meta["missing_timestamps"]["orders.created_at"] = int(orders_clean["created_at"].isna().sum())

   
    if "country" in analytics.columns:
        meta["join_match_rate"]["country_non_null_rate"] = float(analytics["country"].notna().mean())
    elif "country_user" in analytics.columns:
        meta["join_match_rate"]["country_user_non_null_rate"] = float(analytics["country_user"].notna().mean())

    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    orders_raw, users_raw = load_inputs(cfg)

    orders_clean = enforce_schema(orders_raw)

    users_clean = users_raw.copy()
    users_clean["user_id"] = pd.to_numeric(users_clean["user_id"], errors="coerce").astype("Int64")
    if users_clean["user_id"].dropna().duplicated().any():
        if "created_at" in users_clean.columns:
            users_clean = parse_datetime(users_clean, "created_at", utc=True)
            users_clean = dedupe_keep_latest(users_clean, key_cols=["user_id"], ts_col="created_at")
        else:
            raise AssertionError("users.user_id has duplicates and users has no created_at to dedupe by.")

    analytics = transform(orders_clean, users_clean)

    load_outputs(cfg, orders_clean, users_clean, analytics)
