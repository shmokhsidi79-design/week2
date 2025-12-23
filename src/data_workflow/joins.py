import pandas as pd

def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on,
    validate: str,
    suffixes=("_x", "_y"),) -> pd.DataFrame:
    
    return pd.merge(
        left,
        right,
        how="left",
        on=on,
        validate=validate,
        suffixes=suffixes, )
