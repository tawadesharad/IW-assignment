# B2B Vendor Data Pipeline

## Overview
This project implements a robust data pipeline to ingest, clean, standardize, and aggregate sales data from multiple B2B vendors. Each vendor provides data in different formats and schemas, requiring transformation into a unified structure.

The pipeline processes raw vendor files and produces a standardized **daily sales dataset**.

---

## Tech Stack
- Python
- Pandas
- Google Colab
- Google Drive (for data storage)

---

## Input Data
Data is sourced from multiple vendors with inconsistent schemas:

- Zepto  
- Blinkit  
- Nykaa  
- Myntra  

Each file contains different column names, formats, and data quality issues.

---

## Pipeline Workflow

### 1. Ingestion
- Reads all CSV files from a Google Drive folder
- Automatically detects vendor using filename

---

### 2. Standardization
Maps vendor-specific columns into a unified schema

---

### 3. Data Cleaning
- Handles missing values
- Converts columns to correct data types
- Removes invalid records (null dates, missing product IDs)

---

### 4. Date Normalization
Different vendors use different formats:
- Zepto → DD-MM-YYYY  
- Myntra → YYYYMMDD  
- Others → standard date format  

All are converted into a consistent datetime format.

---

### 5. Aggregation
Data is grouped by:
- date  
- product_identifier  
- data_source  

Metrics calculated:
- total_units (sum)  
- total_revenue (sum)  

---

## Output
Final dataset is stored in final_daily_sales.csv file in output folder

---
## Assumptions
- Input files are in CSV format
- Vendor name is present in filename
- Column mappings are predefined
- Data volume is manageable in-memory (Pandas)

## Challenges Faced
- Handling inconsistent schemas across vendors
- Managing multiple date formats
- Dealing with missing and incorrect values
- Ensuring consistent aggregation logic

## Future Enhancements
- Use PySpark for large-scale data processing
- Automate pipeline using Airflow
- Store output in a data warehouse (Redshift / ClickHouse)
