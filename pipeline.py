import pandas as pd
import os
from config import COLUMN_MAPPINGS

# INPUT_FOLDER = r"C:\Users\MSI\Desktop\assignment\Assignment"
# OUTPUT_FILE = "output/final_daily_sales.csv"


INPUT_FOLDER = "/content/drive/MyDrive/inde_wild/input"
OUTPUT_FILE = "/content/drive/MyDrive/inde_wild/output/final_daily_sales.csv"

# -----------------------------
# Identify Vendor
# -----------------------------
def get_vendor(filename):
    filename = filename.lower()

    if "zepto" in filename:
        return "zepto"
    elif "blinkit" in filename:
        return "blinkit"
    elif "nykaa" in filename:
        return "nykaa"
    elif "myntra" in filename:
        return "myntra"
    else:
        return None


# -----------------------------
# Standardization
# -----------------------------
def standardize(df, vendor):
    mapping = COLUMN_MAPPINGS[vendor]

    df = df.rename(columns={
        mapping["date"]: "date",
        mapping["sku"]: "product_identifier",
        mapping["units"]: "total_units",
        mapping["revenue"]: "total_revenue"
    })

    df["data_source"] = vendor
    return df[["date", "product_identifier", "total_units", "total_revenue", "data_source"]]


# -----------------------------
# Date Handling
# -----------------------------
def fix_dates(df, vendor):
    if vendor == "zepto":
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    elif vendor == "myntra":
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    else:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df


# -----------------------------
# data Cleaning
# -----------------------------
def clean(df):
    df = df.dropna(subset=["date", "product_identifier"])

    df["total_units"] = pd.to_numeric(df["total_units"], errors="coerce").fillna(0)
    df["total_revenue"] = pd.to_numeric(df["total_revenue"], errors="coerce").fillna(0)

    return df


# -----------------------------
# Pipeline
# -----------------------------
def run_pipeline():
    all_data = []

    for file in os.listdir(INPUT_FOLDER):
        if file.endswith(".csv"):
            vendor = get_vendor(file)

            if not vendor:
                continue

            path = os.path.join(INPUT_FOLDER, file)
            df = pd.read_csv(path)

            df = standardize(df, vendor)
            df = fix_dates(df, vendor)
            df = clean(df)

            all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)

    # Aggregation
    final_df = final_df.groupby(["date", "product_identifier", "data_source"],
        as_index=False
    ).agg({
        "total_units": "sum",
        "total_revenue": "sum"
    })

    final_df.to_csv(OUTPUT_FILE, index=False)
    print("Pipeline completed and Output saved at:", OUTPUT_FILE)


if __name__ == "__main__":
    run_pipeline()
