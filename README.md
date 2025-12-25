# Week 2 – Data Workflow Project

## Project Overview
This project work with a simple data workflow using Python and pandas.
The main goal is to:
- Load raw CSV data
- Clean and validate it
- Save cleaned data in Parquet format
- Apply basic data quality checks

# Project Structure

week2project/
│
├── data/
│   ├── raw/                     # CSV files (input)
│   │   ├── orders.csv
│   │   └── users.csv
│   │
│   ├── processed/               # cleaned + derived data
│   │   ├── orders_clean.parquet
│   │   ├── users.parquet
│   │   └── analytics_table.parquet
│   │
│   ├── cache/                   # optional temp files
│   └── external/                # external datasets (if any)
│
├── reports/
│   ├── missingness_orders.csv
│   └── figures/                 # plots exported from notebook
│       ├── revenue_by_country.png
│       ├── amount_hist_winsor.png
│       └── ...
│
├── notebooks/
│   └── eda.ipynb
│
├── scripts/
│   ├── run_day1_load.py
│   ├── run_day2_clean.py
│   └── run_day3_build_analytics.py
│
├── src/
│   └── bootcamp_data/
│       ├── __init__.py
│       ├── etl.py
│   └── data_workflow/
│       ├── __init__.py
│       ├── config.py
│       ├── io.py
│       ├── transforms.py
│       ├── joins.py
│       └── quality.py
│
├── pyproject.toml
└── README.md


 Day 1 – Load Raw Data

- Reads raw CSV files (`orders.csv`, `users.csv`)
- Verifies that the files exist
- Saves them as Parquet files
- Uses structured paths via `config.py`

### Run this command:
```bash
python scripts/run_day1_load.py

Output:
-data/processed/orders.parquet
data/processed/users.parque
```


# day2
In this day , we clean and validate the data produced in Day 1.
The goal is to ensure data quality before any analysis or modeling.

🔹 What happens in Day 2?

we will 

Load processed data

Reads it

Validate structure

Ensures required columns exist.

Ensures datasets are not empty.

Apply schema enforcement

Converts data types ( numbers, strings, dates).

create missing values report

Saves the report as missingness_orders.csv.

Clean data

Adds flags for missing numeric values, and do a Quality checks

Ensures numeric values are within valid ranges.

Stops execution if invalid data is found.

Save cleaned data and Writes it cleaned dataset to:

data/processed/orders_clean.parquet


run:
python scripts/run_day2_clean.py

## Day3 

In this day, we prepare the dataset for analytics with:
- converting timestamp columns to  datetimes
- creating time-based features ( date, year, month, day of week, hour)
- handling numeric outliers using IQR bounds and winsorization

#we did this

    Load cleaned data
   - Reads from `orders_clean.parquet` from `data/processed/`.
    Parse datetime
   - Converts `created_at` into a pandas datetime type using safe parsing (`errors="coerce"`).
   - Invalid timestamps become `NaT` instead of crashing the script.
    Add time parts
    Creates additional time columns from `created_at`, such as:
     - `date`, `year`, `month`, `dow`, `hour`

 Outlier bounds (IQR)**
   Winsorize numeric values
   - Caps extreme values instead of dropping rows.
   - Uses quantile-based limits ( default: 1% and 99%).

---

run
python scripts/run_day3_load.py


## Day 4

This notebook focuses on exploratory data analysis.  
The goal is to understand the data structure, explore key patterns, and generate visual insights.


 1. Data Loading & Audit
- Loaded `analytics_table.parquet`
- Checked:
  - Row count
  - Column data types
  - Missing values per column



2. Key Questions

The following analytical questions were explored:

1. How does total revenue change over time?
2. How does revenue differ by country?
3. What is the distribution of order amounts (winsorized)?
4. Is the refund rate different across countries?

Each question is supported by:
- A table
- A visualization
- Short interpretation


- Aggregated revenue by month.
- Created a line chart showing revenue trends over time.
- Observed variations across months indicating changing activity levels.

📊 Output:
- `reports/figures/revenue_trend_monthly.png`

 4. Revenue by Country
- Grouped data by country.
- Computed total revenue and number of orders.
- Visualized results using a bar chart.

📊 Output:
- `reports/figures/revenue_by_country.png`

---
 5. Distribution of Order Amounts
- Used winsorized order values to reduce outlier impact.
- Plotted a histogram to visualize typical order behavior.

📊 Output:
- `reports/figures/amount_hist_winsor.png`


6. Refund Rate Comparison (Bootstrap)
- Compared refund rates between two countries using bootstrap sampling.
- Computed:
  - Mean difference
  - 95% confidence interval
- Result showed small differences with overlapping confidence intervals.

---

7. Key Findings
- Revenue varies across time and countries.
- A small number of countries contribute a large share of total revenue.
- Order values are right-skewed; winsorization helps stabilize analysis.
- Refund rate differences are small and not statistically significant.

---

8. Caveats
- Some countries have limited observations.
- Results are based on historical data only.
- Bootstrap confidence intervals do not imply causality.


## Day 5
this task focuses on building a complete ETL pipeline to prepare analytics-ready data.

1. Data Ingestion

Loaded raw CSV files:

orders.csv

users.csv

Validated required columns and basic structure.

2. Data Cleaning & Transformation

Enforced schema and data types.

Normalized text fields.

Parsed datetime columns.

Added time features (year, month, weekday, hour).

Handled missing values and added missing flags.

Deduplicated users.

Applied winsorization to numeric fields.

Validated join integrity.

3. Data Integration

Performed left join between orders and users.

Ensured one-to-many relationship integrity.

Preserved row counts after join.

4. Outputs Generated

orders_clean.parquet

users.parquet

analytics_table.parquet

run_meta.json

5. Metadata Recorded

Row counts per table

Missing timestamp counts

Join match rates

File paths used in the run

6. Result

The pipeline produces clean, analytics-ready data with consistent structure and traceable metadata