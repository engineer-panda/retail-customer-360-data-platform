# Databricks notebook source
# DBTITLE 1,CONFIGURATION
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

silver_path = "abfss://silver@strretail360.dfs.core.windows.net/products/products_silver"
gold_path   = "abfss://gold@strretail360.dfs.core.windows.net/delta/product_gold"

silver_df = spark.read.format("delta").load(silver_path)

if not DeltaTable.isDeltaTable(spark, gold_path):
    silver_df.limit(0).write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(gold_path)

print("done")

# COMMAND ----------

# DBTITLE 1,READ
from pyspark.sql.functions import col

dim_product_df = silver_df.select(
    col("product_id"),
    col("product_name"),
    col("product_category"),
    col("unit_price"),
    col("brand"),
    col("is_active_flag")
)

print("done")

# COMMAND ----------

# DBTITLE 1,CREATE dim_customer
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, gold_path)

delta_table.alias("t").merge(
    dim_product_df.alias("s"),
    "t.product_id = s.product_id"
).whenMatchedUpdate(set={
    "product_id": "s.product_id",
    "product_name": "s.product_name",
    "product_category": "s.product_category",
    "unit_price": "s.unit_price",
    "brand": "s.brand",
    "is_active_flag": "s.is_active_flag"
}).whenNotMatchedInsert(values={
    "product_id": "s.product_id",
    "product_name": "s.product_name",
    "product_category": "s.product_category",
    "unit_price": "s.unit_price",
    "brand": "s.brand",
    "is_active_flag": "s.is_active_flag"
}).execute()

#EXPORT TO PARQUET
parquet_output_path = "abfss://gold@strretail360.dfs.core.windows.net/reporting/products_parquet"

spark.read.format("delta").load(gold_path) \
    .write.mode("overwrite") \
    .parquet(parquet_output_path)

print("done")

# COMMAND ----------

dim_product_df.display()