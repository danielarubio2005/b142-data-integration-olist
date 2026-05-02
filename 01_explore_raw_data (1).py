# Databricks notebook source
# Test to read one CSV to see if everything works
df_orders = spark.read.csv(
    "/Volumes/workspace/default/olist_raw/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

# See how many rows
print("Number of rows:", df_orders.count())

# Show the first 5 rows
df_orders.show(5)

# See the schema (columns and data types)
df_orders.printSchema()