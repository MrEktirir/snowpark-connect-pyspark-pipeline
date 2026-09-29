# Snowpark Connect PySpark Pipeline

A modular data engineering pipeline that uses the **PySpark DataFrame API** with **Snowpark Connect for Apache Spark** to process data directly in Snowflake.

The project demonstrates data ingestion, validation, business transformations, data quality checks, and analytical table creation using a Python application running in GitHub Codespaces.

## Project Overview

This project processes the **Tasty Bytes menu dataset** stored in an Amazon S3 bucket.

The pipeline loads raw menu records into Snowflake, validates the source data, calculates profitability metrics, aggregates the results by food truck brand and menu type, and writes the analytical output back to Snowflake.

Unlike a traditional Spark deployment, Snowpark Connect allows PySpark DataFrame operations to execute using Snowflake's compute infrastructure.

## Architecture

```text
Amazon S3 (Tasty Bytes Dataset)
          |
          v
Snowflake External Stage
          |
          v
MENU_RAW (100 records)
          |
          v
PySpark DataFrame
          |
          v
Data Validation
          |
          v
Data Transformation
  - Data cleaning
  - Profit calculations
  - Profit and price tiers
  - Brand-level aggregation
          |
          v
Data Quality Checks
          |
          v
MENU_BRAND_SUMMARY (15 records)
          |
          v
Pipeline Summary
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Pipeline development |
| PySpark DataFrame API | Data processing and transformations |
| Snowpark Connect | Execution of PySpark operations using Snowflake |
| Snowflake | Data storage, SQL processing, and compute |
| Amazon S3 | Source dataset |
| GitHub Codespaces | Development environment |

## Project Structure

```text
snowpark-connect-pyspark-pipeline/
├── main.py
├── src/
│   └── pipeline/
│       ├── connection.py
│       ├── ingestion.py
│       ├── validation.py
│       ├── transformation.py
│       ├── quality.py
│       └── output.py
├── config/
│   └── snowflake.yaml       # Local credentials; excluded from Git
├── requirements.txt
├── .gitignore
└── README.md
```

## Pipeline Workflow

### 1. Connection and Setup

Initializes a Snowpark Connect Spark session using connection parameters stored in a local YAML configuration file.

Creates a dedicated Snowflake schema and an external stage referencing the Tasty Bytes dataset.

### 2. Data Ingestion

Creates the `MENU_RAW` table and loads source records using Snowflake's `COPY INTO` command.

**Result:** 100 menu records ingested.

### 3. Data Validation

Validates the source dataset before applying business transformations.

Checks include:

- Expected row count and required columns.
- Null values in key fields.
- Duplicate menu item identifiers.
- Negative cost or sale prices.

**Result:** 100 records passed validation.

### 4. Data Transformation

Applies PySpark DataFrame operations to:

- Normalize brand names, menu types, and categories.
- Calculate profit per menu item.
- Calculate profit margin percentages.
- Assign profit and price tiers.
- Aggregate menu metrics by brand and menu type.

The aggregated output includes average costs, prices, profits, profit margins, and total potential profit.

**Result:** 15 aggregated records.

### 5. Data Quality Checks

Validates the transformed output against four conditions:

1. Output contains records.
2. Brand and menu-type combinations are unique.
3. Average profit margins are non-negative.
4. Brand names are not null.

**Result:** 4/4 checks passed — **100% quality score**.

### 6. Write Output

Writes the transformed DataFrame to a Snowflake table using:

```python
df.write.mode("overwrite").saveAsTable(table_name)
```

The pipeline reads the destination table again to verify the number of written records.

**Destination table:**

```text
SNOWFLAKE_LEARNING_DB
└── HIZIRREIS_INTRO_TO_SNOWPARK_CONNECT
    ├── MENU_RAW
    └── MENU_BRAND_SUMMARY
```

### 7. Cleanup and Summary

The pipeline reports execution metrics and closes the Spark session.

Example output from a successful execution:

```text
==================================================
PIPELINE SUMMARY
==================================================
Status: SUCCESS
Duration: 49.15 seconds
Rows In: 100
Rows Out: 15
Quality Score: 100%
==================================================
```

## Sample Analytical Results

| Food Truck Brand | Menu Type | Items | Avg. Profit (USD) | Avg. Margin |
|---|---|---:|---:|---:|
| Freezing Point | Ice Cream | 10 | 2.94 | 72.19% |
| The Mega Melt | Grilled Cheese | 6 | 2.79 | 70.00% |
| Revenge of the Curds | Poutine | 6 | 4.96 | 69.94% |
| Peking Truck | Chinese | 6 | 3.88 | 68.07% |
| Le Coin des Crêpes | Crepes | 6 | 4.54 | 67.87% |

These figures represent menu-level profitability calculations, not actual realized sales revenue or profit.

## Getting Started

### Prerequisites

- Python 3.11
- A Snowflake account with permission to create schemas, stages, and tables
- An available Snowflake warehouse
- Network access to the Tasty Bytes source dataset

### Install Dependencies

Create and activate a virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Install the required packages:

```bash
pip install -r requirements.txt
```

### Configure Snowflake Connection

Create a local configuration file at `config/snowflake.yaml`:

```yaml
snowflake:
  account: YOUR_ACCOUNT
  user: YOUR_USERNAME
  password: YOUR_PASSWORD
  role: YOUR_ROLE
  warehouse: YOUR_WAREHOUSE
  database: YOUR_DATABASE
  schema: PUBLIC
```

The configuration file is excluded from Git through `.gitignore`.

**Never commit actual credentials or access tokens.**

### Run the Pipeline

```bash
python main.py
```

The pipeline creates the required schema and tables, processes the menu dataset, and prints the execution summary.

## Key Learnings

This project provided hands-on experience with:

- Running PySpark DataFrame operations through Snowpark Connect.
- Combining Snowflake SQL operations with PySpark transformations.
- Building a modular ETL pipeline.
- Validating data before and after transformation.
- Persisting analytical results to Snowflake.
- Tracking pipeline executions using run identifiers and timestamps.
- Managing Snowflake credentials separately from version-controlled source code.

## Reference

Based on the official Snowflake quickstart:

[Comprehensive Guide to Snowpark Connect for Apache Spark](https://www.snowflake.com/en/developers/guides/intro-to-snowpark-connect-for-apache-spark/)

The implementation was adapted to run as a modular Python application in GitHub Codespaces rather than a Snowflake Notebook.