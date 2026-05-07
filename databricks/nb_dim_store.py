# Databricks notebook source
# DBTITLE 1,CONFIGURATION
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

silver_path = "abfss://silver@strretail360.dfs.core.windows.net/stores/stores_silver"
gold_path   = "abfss://gold@strretail360.dfs.core.windows.net/delta/stores_gold"

silver_df = spark.read.format("delta").load(silver_path)

# create gold if not exists
if not DeltaTable.isDeltaTable(spark, gold_path):
    (
        silver_df.limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path)
    )

print("done")

# COMMAND ----------

# DBTITLE 1,READ
from pyspark.sql.functions import col

dim_store_df = silver_df.select(
    col("store_id"),
    col("store_name"),
    col("store_type"),
    col("city"),
    col("province"),
    col("store_manager_name")
)

print("done")

# COMMAND ----------

# DBTITLE 1,CREATE dim_store
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, gold_path)

delta_table.alias("t").merge(
    dim_store_df.alias("s"),
    "t.store_id = s.store_id"
).whenMatchedUpdate(set={
    "store_id": "s.store_id",
    "store_name": "s.store_name",
    "store_type": "s.store_type",
    "city": "s.city",
    "province": "s.province",
    "store_manager_name": "s.store_manager_name"
}).whenNotMatchedInsert(values={
    "store_id": "s.store_id",
    "store_name": "s.store_name",
    "store_type": "s.store_type",
    "city": "s.city",
    "province": "s.province",
    "store_manager_name": "s.store_manager_name"
}).execute()

#EXPORT TO PARQUET
parquet_output_path = "abfss://gold@strretail360.dfs.core.windows.net/reporting/stores_parquet"

spark.read.format("delta").load(gold_path) \
    .write.mode("overwrite") \
    .parquet(parquet_output_path)

print("done")

# COMMAND ----------

dim_store_df.display()