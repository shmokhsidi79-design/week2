
import pandas as pd

def normalize1text(series: pd.Series) -> pd.Series:  #دالة عشان تفهم الكلمة بنفس المعنى مايفرق هي كابيتل او سمول ليترز
    s=series.astype(str)
    s=s.str.strip()
    s=s.str.lower()
    return s
    

def into_number(series:pd.Series) -> pd.Series: #تحويل العامود لرقم واذا لقى قيمة مفقودة يحولها لNan بدون مايعلق البرنامج
   return pd.to_numeric(series, errors="coerce")
   

def into_datetime(series: pd.Series, *, utc: bool = True) -> pd.Series: #يحول العامود لتاريخ
    return pd.to_datetime(series, errors="coerce", utc=utc)

def assert1range(series:pd.Series,*,lo=0,hi=None,name="value"):
    s = series.dropna()
    if lo is not None:
        not_good= s < lo
    if not_good.any():
         test = s[not_good].head(7).tolist()
         raise ValueError(f"{name} has values below {lo}")

    if hi is not None:
        not_good = s > hi
        if not_good.any():
             test = s[not_good].head(5).tolist()
             raise ValueError(f"{name} has values above {hi}")
        

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=pd.to_numeric(df["user_id"], errors="coerce").astype("Int64"),
        amount=pd.to_numeric(df["amount"], errors="coerce"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce"),
        created_at=pd.to_datetime(df["created_at"], errors="coerce", utc=True),
        status=df["status"].astype("string")
    )