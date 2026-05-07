# Databricks notebook source
# DBTITLE 1,CONFIGURATION
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "200")

sales_path = "abfss://silver@strretail360.dfs.core.windows.net/sales/sales_silver"

customer_path = "abfss://gold@strretail360.dfs.core.windows.net/delta/customer_gold"
product_path  = "abfss://gold@strretail360.dfs.core.windows.net/delta/products_gold"
store_path    = "abfss://gold@strretail360.dfs.core.windows.net/delta/stores_gold"

gold_path = "abfss://gold@strretail360.dfs.core.windows.net/delta/sales_gold"

sales_df = spark.read.format("delta").load(sales_path)

print("Setup done")

# COMMAND ----------

# DBTITLE 1,JOINS
# ensure dimension tables exist (prevents PATH errors)
def ensure_table(path, silver_fallback):
    if not DeltaTable.isDeltaTable(spark, path):
        (
            spark.read.format("delta")
            .load(silver_fallback)
            .limit(0)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )

ensure_table(customer_path, "abfss://silver@strretail360.dfs.core.windows.net/customers/customers_silver")
ensure_table(product_path,  "abfss://silver@strretail360.dfs.core.windows.net/products/products_silver")
ensure_table(store_path,    "abfss://silver@strretail360.dfs.core.windows.net/stores/stores_silver")

# read dims
customer_df = spark.read.format("delta").load(customer_path)
product_df  = spark.read.format("delta").load(product_path)
store_df    = spark.read.format("delta").load(store_path)

# build fact
fact_sales_df = (
    sales_df.alias("s")
    .join(customer_df.alias("c"), "customer_id", "left")
    .join(product_df.alias("p"), "product_id", "left")
    .join(store_df.alias("st"), "store_id", "left")
    .select(
        col("s.order_id"),
        col("s.customer_id"),
        col("s.product_id"),
        col("s.store_id"),
        col("s.order_timestamp"),
        col("s.order_amount"),
        col("s.channel_type")
    )
)

print("Fact ready")

# COMMAND ----------

# DBTITLE 1,CREATE fact_sales
# check available columns
print(sales_df.columns)

# build fact safely
fact_sales_df = (
    sales_df.alias("s")
    .join(customer_df.alias("c"), "customer_id", "left")
    .join(product_df.alias("p"), "product_id", "left")
    .join(store_df.alias("st"), "store_id", "left")
    .select(
        col("s.order_id"),
        col("s.customer_id"),
        col("s.product_id"),
        col("s.store_id"),
        col("s.order_timestamp"),
        col("s.order_amount"),
        # only include if exists
        *([col("s.channel_type")] if "channel_type" in sales_df.columns else [])
    )
)

# SAVE FACT GOLD AS PARQUET FOR POWER BI
fact_sales_df.write.mode("overwrite").parquet("abfss://gold@strretail360.dfs.core.windows.net/reporting/fact_sales_parquet")

# COMMAND ----------

fact_sales_df.display()