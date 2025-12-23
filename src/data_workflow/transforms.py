import pandas as pd

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )
def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.isna().sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / len(df))
        .sort_values("p_missing", ascending=False)
    )

def normalize_text(s: pd.Series) -> pd.Series:
    import re
    _ws = re.compile(r"\s+")
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True)
    )
def parse_datetime(
    df: pd.DataFrame,
    col: str,
    *,
    utc: bool = True
) -> pd.DataFrame:
  
    return df.assign(
        **{
            col: pd.to_datetime(
                df[col],
                errors="coerce",
                utc=utc
            )
        }
    )
def add_time_parts(
    df: pd.DataFrame,
    ts_col: str
) -> pd.DataFrame:
   
    return df.assign(
        date=df[ts_col].dt.date,
        year=df[ts_col].dt.year,
        month=df[ts_col].dt.month,
        dow=df[ts_col].dt.dayofweek,  # Monday=0
        hour=df[ts_col].dt.hour,
    )
def iqr_bounds(
    s: pd.Series,
    k: float = 1.5
) -> tuple[float, float]:
    
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1

    lower = q1 - k * iqr
    upper = q3 + k * iqr

    return lower, upper
def winsorize(
    s: pd.Series,
    lo: float = 0.01,
    hi: float = 0.99
) -> pd.Series:
 
    s.quantile(lo)
    upper = s.quantile(hi)

    return s.clip(lower=lower, upper=upper)
def add_outlier_flag(
    df: pd.DataFrame,
    col: str,
    *,
    k: float = 1.5
) -> pd.DataFrame:
   
    lower, upper = iqr_bounds(df[col], k=k)

    return df.assign(
        **{
            f"{col}_is_outlier":
                (df[col] < lower) | (df[col] > upper)
        }
    )