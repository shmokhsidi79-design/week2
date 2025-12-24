from pathlib import Path 
import pandas as pd
import sys

root = Path(__file__).resolve().parents[1]
sys.path.append(str(root / "src"))

from data_workflow.config import paths
paths = paths(root)

print("raw:", paths["raw"])
print("processed:", paths["processed"])
print("cache:", paths["cache"])
print("external:", paths["external"])

print((paths["raw"] / "orders.csv").exists())
print((paths["raw"] / "users.csv").exists())

from data_workflow.io import read1orders, read1users, write1parquet
from data_workflow.transforms import enforce_schema


orders = read1orders(paths["raw"] / "orders.csv")
orders = enforce_schema(orders) 
users = read1users(paths["raw"] / "users.csv")


out = paths["processed"] / "orders.parquet"
print("will write to:", out)
write1parquet(orders, out)
print(
    "written?",
    out.exists(),
    "size:",
    out.stat().st_size if out.exists() else "NA"
)


out_users = paths["processed"] / "users.parquet"
print("will write to:", out_users)
write1parquet(users, out_users)
print(
    "written?",
    out_users.exists(),
    "size:",
    out_users.stat().st_size if out_users.exists() else "NA"
)


orders1pq = pd.read_parquet(paths["processed"] / "orders.parquet")
users1pq  = pd.read_parquet(paths["processed"] / "users.parquet")

print(orders.head())
print(users.head())

print("orders parquet shape:", orders1pq.shape)
print("users parquet shape:", users1pq.shape)

print(orders1pq.head())
print(users1pq.head())