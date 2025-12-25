
import pandas as pd

def normalize1text(series: pd.Series) -> pd.Series:  #دالة عشان تفهم الكلمة بنفس المعنى مايفرق هي كابيتل او سمول ليترز
    s=series.astype(str)
    s=s.str.strip()
    s=s.str.casefold()
    return s
    

def into_number(series:pd.Series) -> pd.Series: #تحويل العامود لرقم واذا لقى قيمة مفقودة يحولها لNan بدون مايعلق البرنامج
   return pd.to_numeric(series, errors="coerce")
   

def into_datetime(series: pd.Series, *, utc: bool = True) -> pd.Series: #يحول العامود لتاريخ
    return pd.to_datetime(series, errors="coerce", utc=utc)


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=pd.to_numeric(df["user_id"], errors="coerce").astype("Int64"),
        amount=pd.to_numeric(df["amount"], errors="coerce"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce"),
        created_at=pd.to_datetime(df["created_at"], errors="coerce", utc=True),
        status=df["status"].astype("string")
    )

 
def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    total1 = len(df)
    missing1count = df.isna().sum()
    missing1pct = (missing1count / total1) * 100 if total1 else 0

    out = pd.DataFrame({
        "column": missing1count.index,
        "missing_count": missing1count.values,
        "missing_pct": missing1pct.values if hasattr(missing1pct, "values") else missing1pct,
    })

    return out.sort_values(["missing_count", "column"], ascending=[False, True]).reset_index(drop=True)

def add_missing_flags(df: pd.DataFrame, cols) -> pd.DataFrame:
     miising_flags = {f"{c}__isna": df[c].isna() for c in cols}
     return df.assign(**miising_flags)

def dedupe_keep_latest(df:pd.DataFrame, key_cols, ts_col) -> pd.DataFrame:
    dedupe = df.sort_values(ts_col)
    dedupe = dedupe.drop_duplicates(subset=key_cols, keep="last")
    dedupe = dedupe.reset_index(drop=True)
    return dedupe

#هنا نحول النص لتاريخ 
def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    datetime1 = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: datetime1})


def add_time_parts(df: pd.DataFrame, time1) -> pd.DataFrame:
    datetotime2 = df[time1]

    return df.assign(
        date=datetotime2.dt.date,
        year=datetotime2.dt.year,
        month=datetotime2.dt.month,
        dow=datetotime2.dt.dayofweek,
        hour=datetotime2.dt.hour
    )

def iqr_bounds(s: pd.Series, k: float = 1.5):
    x = s.dropna()
    x2 = x.quantile(0.25)
    x3 = x.quantile(0.75)
    iqr = (x3 - x2)
    low = (x2 - k) * iqr
    up = (x3 + k) * iqr
    return low, up


def winsorize(s: pd.Series, lo1: float = 0.01, hi1: float = 0.99) -> pd.Series:
    x = s.dropna()
    lo1 = x.quantile(lo1)
    hi1 = x.quantile(hi1)
    return s.clip(lower=lo1, upper=hi1)