# Week 2 Summary  ETL + EDA

## Key findings
- Revenue is unevenly distributed over time, with clear monthly variation.
- A small number of countries contribute most of the total revenue.
- Order values are right-skewed, with a few high-value orders influencing the distribution.

## Definitions
- Revenue = sum(amount) for completed orders.
- Refund rate = refunded orders / total orders, where status = "refunded".
- Time window = full available date range based on created_at.

## Data quality caveats
- Missingness: Some rows contain missing values in key fields such as created_at or amount.
- Duplicates: Duplicate users were detected and resolved by keeping the most recent record.
- Join coverage: A small number of orders did not match a user record.
- Outliers: Order values are skewed; winsorization was applied to reduce extreme effects.

## Next questions
- How does customer behavior change over time?
- Are high-value customers consistent or one-time buyers?
- How does refund behavior vary across regions?
- Can additional features improve revenue forecasting?
