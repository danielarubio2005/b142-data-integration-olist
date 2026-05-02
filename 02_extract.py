# Databricks notebook source
# Set up and paths

# Define paths (raw data)
RAW_PATH = "/Volumes/workspace/default/olist_raw"

# The files I'll load
files = {
    "customers":    f"{RAW_PATH}/olist_customers_dataset.csv",
    "geolocation":  f"{RAW_PATH}/olist_geolocation_dataset.csv",
    "order_items":  f"{RAW_PATH}/olist_order_items_dataset.csv",
    "payments":     f"{RAW_PATH}/olist_order_payments_dataset.csv",
    "reviews":      f"{RAW_PATH}/olist_order_reviews_dataset.csv",
    "orders":       f"{RAW_PATH}/olist_orders_dataset.csv",
    "products":     f"{RAW_PATH}/olist_products_dataset.csv",
    "sellers":      f"{RAW_PATH}/olist_sellers_dataset.csv",
    "category_translation": f"{RAW_PATH}/product_category_name_translation.csv",
}

print("Paths defined. Ready to load.")

# COMMAND ----------

# Load every CSV file with header inference (header=True) and schema inference (inferSchema=True)

dfs = {} # An empty dictionary to store DataFrames

for name, path in files.items():
    dfs[name] = spark.read.csv(path, header=True, inferSchema=True)
    print(f"Loaded '{name}': {dfs[name].count():,} rows, {len(dfs[name].columns)} columns")

# COMMAND ----------

# Inspect each schema and see 3 sample rows

for name, df in dfs.items():
    print(f"\n===== {name.upper()} =====")
    df.printSchema()
    display(df.limit(3))

# COMMAND ----------

#Initial data quality assesment to see nulls that will have to be handled when cleaning data

from pyspark.sql.functions import col, count, when, isnan

def null_summary(df, df_name):
    """Count nulls per column."""
    print(f"\n--- Nulls in {df_name} ---")
    total = df.count()
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    for column, n in null_counts.items():
        if n > 0:
            percentage = (n / total) * 100
            print(f"  {column}: {n:,} nulls ({percentage:.2f}%)")   #calculate percentage of the column that is null

for name, df in dfs.items():
    null_summary(df, name)