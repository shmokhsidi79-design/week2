from pathlib import Path
import pandas as pd

missing1values = ["", " ", "na", "n/a", "null", "none", "nan"] #القيم المفقودة عشان نقدر ننظف البيانات بسكل ارتب


def read1orders(path)->pd.DataFrame: 
    return pd.read_csv(path)

def read1users(path)-> pd.DataFrame:
    return pd.read_csv(path)



def write1parquet(df,path):
    path1= path.parent
    path1.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path,index=False)

def read1parquet(path) -> pd.DataFrame:
  return pd.read_parquet(path)

