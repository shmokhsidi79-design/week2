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
│ ├── raw/ raw CSV files
│ ├── processed/ cleaned parquet files
│ ├── cache/
│ └── external/
│
├── reports/
│ └── missingness_orders.csv
│
├── scripts/
│ ├── run_day1_load.py
│ └── run_day2_clean.py
│
├── src/
│ └── data_workflow/
│ ├── config.py
│ ├── io.py
│ ├── transforms.py
│ └── quality.py
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


#--------------------------------------------------------------------

#day2
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

#Day3 

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


