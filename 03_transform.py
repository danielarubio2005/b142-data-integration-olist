# Databricks notebook source
#Reload the raw data

RAW_PATH = "/Volumes/workspace/default/olist_raw"

orders        = spark.read.csv(f"{RAW_PATH}/olist_orders_dataset.csv",        header=True, inferSchema=True)
customers     = spark.read.csv(f"{RAW_PATH}/olist_customers_dataset.csv",     header=True, inferSchema=True)
order_items   = spark.read.csv(f"{RAW_PATH}/olist_order_items_dataset.csv",   header=True, inferSchema=True)
payments      = spark.read.csv(f"{RAW_PATH}/olist_order_payments_dataset.csv",header=True, inferSchema=True)
reviews = spark.read.csv(
    f"{RAW_PATH}/olist_order_reviews_dataset.csv",
    header=True,
    inferSchema=True,
    multiLine=True,        # allow newlines inside quoted fields (so reviews with , "" or newline charracters don't break)
    escape='"',            # handle quotes inside quoted fields
    quote='"',
)
print(f"Reviews loaded: {reviews.count():,} rows")
products      = spark.read.csv(f"{RAW_PATH}/olist_products_dataset.csv",      header=True, inferSchema=True)
sellers       = spark.read.csv(f"{RAW_PATH}/olist_sellers_dataset.csv",       header=True, inferSchema=True)
geolocation   = spark.read.csv(f"{RAW_PATH}/olist_geolocation_dataset.csv",   header=True, inferSchema=True)
category_tr   = spark.read.csv(f"{RAW_PATH}/product_category_name_translation.csv", header=True, inferSchema=True)

print("All raw tables loaded.")

# COMMAND ----------

#Clean orders: convert timestamp strings to real timestamps, drop duplicates and filter to delivered orders (scope of my assginment for analysis purposes later)

from pyspark.sql.functions import to_timestamp, col

orders_clean = (
    orders
    .withColumn("order_purchase_timestamp",   to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at",          to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
    .dropDuplicates(["order_id"])  
    .filter(col("order_status") == "delivered")  # focus analytics on completed orders
    .filter(col("order_delivered_customer_date").isNotNull())
)

print(f"Orders before clean: {orders.count():,}")
print(f"Orders after clean:  {orders_clean.count():,}")
orders_clean.show(3)

# COMMAND ----------

#Clean order_items and payments:

# Cleam Order_items: just drop duplicates and ensure data types (it's already pretty clean)
order_items_clean = (
    order_items
    .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))
    .dropDuplicates(["order_id", "order_item_id"])
)

# Clean Payments: drop rows with null payment_value (rare but possible) and drop duplicates
payments_clean = (
    payments
    .filter(col("payment_value").isNotNull())
    .dropDuplicates(["order_id", "payment_sequential"])
)

print(f"Order items: {order_items_clean.count():,}")
print(f"Payments:    {payments_clean.count():,}")

# COMMAND ----------

#Clean reviews: data types and reviews can have duplicates per order_id from updates, so I'll keep latest

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

reviews_clean = (
    reviews
    .withColumn("review_creation_date",   to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp"))
)

# Keep only the latest review per order (.partitionBy("order_id") ensures to keep only the latest review for each order, not across all reviews, and descending so it's the last)
w = Window.partitionBy("order_id").orderBy(col("review_answer_timestamp").desc_nulls_last())
reviews_clean = (
    reviews_clean
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

print(f"Reviews after droping duplicates: {reviews_clean.count():,}")

# COMMAND ----------

#Clean products and translate categories to English: join two tables (products + translation) to enrich the data, and drop duplicates
    
from pyspark.sql.functions import lit, coalesce

# Join with translation to get English category names
products_clean = (
    products
    .join(category_tr, on="product_category_name", how="left")
    .withColumn(
        "product_category_name_english",
        coalesce(col("product_category_name_english"), lit("unknown"))
    )
    # Dimensions column: Fill missing dimensions with 0 (only 600 products aprox. affected)
    .fillna(0, subset=[
        "product_weight_g", "product_length_cm",
        "product_height_cm", "product_width_cm"
    ])
    .dropDuplicates(["product_id"])
)

print(f"Products after clean: {products_clean.count():,}")
products_clean.select("product_id", "product_category_name", "product_category_name_english").show(5) #check if the join was made correctly

# COMMAND ----------

#Clean customers and sellers: drop duplicates

customers_clean = customers.dropDuplicates(["customer_id"])
sellers_clean   = sellers.dropDuplicates(["seller_id"])

print(f"Customers: {customers_clean.count():,}")
print(f"Sellers:   {sellers_clean.count():,}")

# COMMAND ----------

#Clean geolocation: it has multiple rows per zip code, so I'll take the average

from pyspark.sql.functions import avg

geo_clean = (
    geolocation
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        avg("geolocation_lat").alias("lat"),
        avg("geolocation_lng").alias("lng"),
    )
)

print(f"Unique zip prefixes: {geo_clean.count():,}")

# COMMAND ----------

# All the data is clean and ready to integrate

#Path of the cleaned data
CLEAN_PATH = "/Volumes/workspace/default/olist_clean"

# Write each cleaned DataFrame to Parquet files in the CLEAN_PATH directory, overwriting any existing files

orders_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/orders")
order_items_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/order_items")
payments_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/payments")
reviews_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/reviews")
products_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/products")
customers_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/customers")
sellers_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/sellers")
geo_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/geolocation")

print("All cleaned tables written to Parquet")