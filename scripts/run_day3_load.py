import pandas as pd
def main():
   
    orders = pd.read_parquet("data/orders_clean.parquet")
    users = pd.read_parquet("data/users.parquet")

   
    required_order_cols = {"order_id", "user_id", "created_at", "amount"}
    required_user_cols = {"user_id"}

    if not required_order_cols.issubset(orders.columns):
        raise ValueError("orders_clean missing required columns")

    if not required_user_cols.issubset(users.columns):
        raise ValueError("users missing required columns")

    if not users["user_id"].is_unique:
        raise ValueError("users.user_id must be unique")

    
    orders = parse_datetime(orders, "created_at", utc=True)
    orders = add_time_parts(orders, "created_at")

   
    analytics = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
    )

   
    analytics = analytics.assign(
        amount_winsorized=winsorize(analytics["amount"])
    )

    analytics = add_outlier_flag(
        analytics,
        col="amount"
    )

 
    analytics.to_parquet(
        "data/analytics_table.parquet",
        index=False
    )


if __name__ == "__main__":
    main()