# Databricks notebook source
RAW_PATH = "/Volumes/workspace/default/olist_raw"

orders        = spark.read.csv(f"{RAW_PATH}/olist_orders_dataset.csv",        header=True, inferSchema=True)
customers     = spark.read.csv(f"{RAW_PATH}/olist_customers_dataset.csv",     header=True, inferSchema=True)
order_items   = spark.read.csv(f"{RAW_PATH}/olist_order_items_dataset.csv",   header=True, inferSchema=True)
payments      = spark.read.csv(f"{RAW_PATH}/olist_order_payments_dataset.csv",header=True, inferSchema=True)
reviews = spark.read.csv(
    f"{RAW_PATH}/olist_order_reviews_dataset.csv",
    header=True,
    inferSchema=True,
    multiLine=True,        # allow newlines inside quoted fields
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

from pyspark.sql.functions import to_timestamp, col

orders_clean = (
    orders
    .withColumn("order_purchase_timestamp",   to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at",          to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
    .dropDuplicates(["order_id"])  # safety net
    .filter(col("order_status") == "delivered")  # focus analytics on completed orders
    .filter(col("order_delivered_customer_date").isNotNull())
)

print(f"Orders before clean: {orders.count():,}")
print(f"Orders after clean:  {orders_clean.count():,}")
orders_clean.show(3)

# COMMAND ----------

# order_items already pretty clean — just dedupe and ensure types
order_items_clean = (
    order_items
    .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))
    .dropDuplicates(["order_id", "order_item_id"])
)

# payments — drop rows with null payment_value (rare but possible), dedupe
payments_clean = (
    payments
    .filter(col("payment_value").isNotNull())
    .dropDuplicates(["order_id", "payment_sequential"])
)

print(f"Order items: {order_items_clean.count():,}")
print(f"Payments:    {payments_clean.count():,}")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

reviews_clean = (
    reviews
    .withColumn("review_creation_date",   to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp"))
)

# Keep only the latest review per order
w = Window.partitionBy("order_id").orderBy(col("review_answer_timestamp").desc_nulls_last())
reviews_clean = (
    reviews_clean
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

print(f"Reviews after dedup: {reviews_clean.count():,}")

# COMMAND ----------

from pyspark.sql.functions import lit, coalesce

# Join with translation to get English category names
products_clean = (
    products
    .join(category_tr, on="product_category_name", how="left")
    .withColumn(
        "product_category_name_english",
        coalesce(col("product_category_name_english"), lit("unknown"))
    )
    # Fill missing dimensions with 0 (only ~600 products affected)
    .fillna(0, subset=[
        "product_weight_g", "product_length_cm",
        "product_height_cm", "product_width_cm"
    ])
    .dropDuplicates(["product_id"])
)

print(f"Products after clean: {products_clean.count():,}")
products_clean.select("product_id", "product_category_name", "product_category_name_english").show(5)

# COMMAND ----------

customers_clean = customers.dropDuplicates(["customer_id"])
sellers_clean   = sellers.dropDuplicates(["seller_id"])

print(f"Customers: {customers_clean.count():,}")
print(f"Sellers:   {sellers_clean.count():,}")

# COMMAND ----------

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

# Add this as the final cell in notebook 03_transform
CLEAN_PATH = "/Volumes/workspace/default/olist_clean"

# Create the volume first via Catalog UI (same way you made olist_raw),
# OR run this once: spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.olist_clean")

orders_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/orders")
order_items_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/order_items")
payments_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/payments")
reviews_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/reviews")
products_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/products")
customers_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/customers")
sellers_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/sellers")
geo_clean.write.mode("overwrite").parquet(f"{CLEAN_PATH}/geolocation")

print("All cleaned tables written to Parquet")