import pandas as pd
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from data_workflow.transforms import iqr_bounds, winsorize

orders = pd.read_parquet("data/processed/orders_clean.parquet")

lo, hi = iqr_bounds(orders["amount"])
print("IQR bounds:", lo, hi)

w = winsorize(orders["amount"])
print("Winsorized head:")
print(w.head())