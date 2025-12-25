from pathlib import Path
import pandas as pd
import sys

root = Path(__file__).resolve().parents[1]
sys.path.append(str(root / "src"))

from data_workflow.config import make_paths
from data_workflow.io import read1orders, read1users, write1parquet
from data_workflow.transforms import enforce_schema

p = make_paths(root)

print("raw:", p.raw)
print("processed:", p.processed)
print("cache:", p.cache)
print("external:", p.external)

print((p.raw / "orders.csv").exists())
print((p.raw / "users.csv").exists())

orders = read1orders(p.raw / "orders.csv")
orders = enforce_schema(orders)

users = read1users(p.raw / "users.csv")

out = p.processed / "orders.parquet"
print("will write to:", out)
write1parquet(orders, out)
print(
    "written?",
    out.exists(),
    "size:",
    out.stat().st_size if out.exists() else "NA"
)

out_users = p.processed / "users.parquet"
print("will write to:", out_users)
write1parquet(users, out_users)
print(
    "written?",
    out_users.exists(),
    "size:",
    out_users.stat().st_size if out_users.exists() else "NA"
)

orders1pq = pd.read_parquet(p.processed / "orders.parquet")
users1pq = pd.read_parquet(p.processed / "users.parquet")

print(orders.head())
print(users.head())

print("orders parquet shape:", orders1pq.shape)
print("users parquet shape:", users1pq.shape)

print(orders1pq.head())
print(users1pq.head())