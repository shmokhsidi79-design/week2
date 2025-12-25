import pandas as pd 


def require_columns(df: pd.DataFrame, cols: list[str]):
    is_missing = [c for c in cols if c not in df.columns]
    if is_missing:
        raise AssertionError(f"Missing required columns: {is_missing}. Found: {list(df.columns)}")


def assert_non_empty(df: pd.DataFrame, name="df"):
    if df is None or df.empty:
        raise AssertionError(f"{name} empty")


def assert_unique_key(df: pd.DataFrame, key, allow_na=False):
    if key not in df.columns:
        raise AssertionError(f"Key column '{key}' not found")

    s = df[key]
    if not allow_na:
        s = s.dropna()

    dup = s[s.duplicated(keep=False)]
    if not dup.empty:
        sample = dup.head(10).tolist()
        raise AssertionError(f"Key '{key}' has duplicates. Sample: {sample}")


def assert1range(series: pd.Series, *, lo=0, hi=None, name="value"):
    s = series.dropna()

    if lo is not None:
        not_good = s < lo
        if not_good.any():
            test = s[not_good].head(7).tolist()
            raise ValueError(f"{name} has values below {lo}")

    if hi is not None:
        not_good = s > hi
        if not_good.any():
            test = s[not_good].head(5).tolist()
            raise ValueError(f"{name} has values above {hi}")