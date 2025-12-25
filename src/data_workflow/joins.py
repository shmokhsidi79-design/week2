from __future__ import annotations
import pandas as pd


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    *,
    validate: str,
    suffixes: tuple[str, str] = ("", "_r"),
) -> pd.DataFrame:
    

    before = len(left)

    joined = left.merge(
        right,
        how="left",
        on=on,
        validate=validate,
        suffixes=suffixes,
    )

    after = len(joined)

    if before != after:
        raise AssertionError(
            f"Row count changed after join! before={before}, after={after}"
        )

    return joined
