# Databricks notebook source
# DBTITLE 1,CONFIGURATION
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

silver_path = "abfss://silver@strretail360.dfs.core.windows.net/customers/customers_silver"
gold_path   = "abfss://gold@strretail360.dfs.core.windows.net/delta/customer_gold"

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

dim_customer_df = silver_df.select(
    col("customer_id"),
    col("full_name"),
    col("email_address"),
    col("city"),
    col("province"),
    col("registration_date"),
    col("is_loyalty_member")
)

print("done")

# COMMAND ----------

# DBTITLE 1,CREATE dim_customer
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, gold_path)

delta_table.alias("t").merge(
    dim_customer_df.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(set={
    "customer_id": "s.customer_id",
    "full_name": "s.full_name",
    "email_address": "s.email_address",
    "city": "s.city",
    "province": "s.province",
    "registration_date": "s.registration_date",
    "is_loyalty_member": "s.is_loyalty_member"
}).whenNotMatchedInsert(values={
    "customer_id": "s.customer_id",
    "full_name": "s.full_name",
    "email_address": "s.email_address",
    "city": "s.city",
    "province": "s.province",
    "registration_date": "s.registration_date",
    "is_loyalty_member": "s.is_loyalty_member"
}).execute()

#EXPORT TO PARQUET
parquet_output_path = "abfss://gold@strretail360.dfs.core.windows.net/reporting/customers_parquet"

spark.read.format("delta").load(gold_path) \
    .write.mode("overwrite") \
    .parquet(parquet_output_path)

print("done")

# COMMAND ----------

dim_customer_df.display()