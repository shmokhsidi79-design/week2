from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

#imports
from data_workflow.config import paths
from data_workflow.io import read1orders, read1users, write1parquet
from data_workflow.transforms import (
    normalize1text,
    add_missing_flags,
    missingness_report,
    enforce_schema,
)
from data_workflow.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
    assert1range,
)


paths_map = paths(ROOT)

#تحميل البيانات
orders = read1orders(paths_map["raw"] / "orders.csv")
users = read1users(paths_map["raw"] / "users.csv")


require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
require_columns(users, ["user_id", "country", "signup_date"])

assert_non_empty(orders, "orders")
assert_non_empty(users, "users")

orders = enforce_schema(orders)

report = missingness_report(orders)
reports_dir = ROOT / "reports"
reports_dir.mkdir(parents=True, exist_ok=True)
report.to_csv(reports_dir / "missingness_orders.csv", index=False)

#تنظيف البيانات
orders["status_clean"] = normalize1text(orders["status"])
orders = add_missing_flags(orders, ["amount", "quantity"])


#الكوالتي شيك
assert1range(orders["amount"], lo=0, name="amount")
assert1range(orders["quantity"], lo=0, name="quantity")


write1parquet(orders, paths_map["processed"] / "orders_clean.parquet")
write1parquet(users, paths_map["processed"] / "users.parquet")


print("orders shape:", orders.shape)
print("users shape:", users.shape)
print("orders_clean path:", paths_map["processed"] / "orders_clean.parquet")
print("report path:", (ROOT / "reports" / "missingness_orders.csv"))

out_orders = paths_map["processed"] / "orders_clean.parquet"
out_report = ROOT / "reports" / "missingness_orders.csv"

print("orders_clean exists?", out_orders.exists())
print("report exists?", out_report.exists())