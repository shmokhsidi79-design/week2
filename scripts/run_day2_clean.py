from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

# imports
from data_workflow.config import make_paths
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

# paths
p = make_paths(ROOT)

# تحميل البيانات
orders = read1orders(p.raw / "orders.csv")
users = read1users(p.raw / "users.csv")

# quality checks (وجود الأعمدة + الداتا مو فاضية)
require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
require_columns(users, ["user_id", "country", "signup_date"])

assert_non_empty(orders, "orders")
assert_non_empty(users, "users")

# (اختياري) unique key checks لو مطلوب
# assert_unique_key(orders, ["order_id"])
# assert_unique_key(users, ["user_id"])

# enforce schema
orders = enforce_schema(orders)

# missingness report
report = missingness_report(orders)
reports_dir = ROOT / "reports"
reports_dir.mkdir(parents=True, exist_ok=True)
report.to_csv(reports_dir / "missingness_orders.csv", index=False)

# تنظيف البيانات
orders["status_clean"] = normalize1text(orders["status"])
orders = add_missing_flags(orders, ["amount", "quantity"])

# الكوالتي شيك (قيم سالبة)
assert1range(orders["amount"], lo=0, name="amount")
assert1range(orders["quantity"], lo=0, name="quantity")

# write outputs
write1parquet(orders, p.processed / "orders_clean.parquet")
write1parquet(users, p.processed / "users.parquet")

# prints / checkpoints
print("orders shape:", orders.shape)
print("users shape:", users.shape)
print("orders_clean path:", p.processed / "orders_clean.parquet")
print("report path:", (ROOT / "reports" / "missingness_orders.csv"))

out_orders = p.processed / "orders_clean.parquet"
out_report = ROOT / "reports" / "missingness_orders.csv"

print("orders_clean exists?", out_orders.exists())
print("report exists?", out_report.exists())
