# Databricks notebook source
# DBTITLE 1,READ CONTROL TABLE
jdbc_url = "jdbc:sqlserver://customerretaiilproject.database.windows.net:1433;database=dbretailcustomers"

properties = {
  "user": "username",
  "password": "Azure@123",
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

control_df = spark.read.jdbc(
  url=jdbc_url,
  table="(SELECT * FROM dbo.control_table WHERE source_name = 'sales') as t",
  properties=properties
)

control_df.display()
control_df.printSchema()

# COMMAND ----------

# DBTITLE 1,GETTING LAST WATERMARK
last_wm = control_df.select("last_watermark_value").first()[0]
print(last_wm)

# COMMAND ----------

# DBTITLE 1,READ BRONZE
bronze_df = spark.read.format("parquet").load(
  "abfss://bronze@strretail360.dfs.core.windows.net/sales"
)

incremental_df = bronze_df.filter(
  f"ingestion_time > '{last_wm}'"
)

incremental_df.display()


# COMMAND ----------

# DBTITLE 1,DEDUPLICATION
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy("order_id") \
    .orderBy(col("ingestion_time").desc())

dedup_df = incremental_df \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

dedup_df.display()

# COMMAND ----------

# DBTITLE 1,SILVER TRANSFORMATION
from pyspark.sql.functions import col, trim, lower, expr, coalesce, regexp_replace

clean_df = dedup_df \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("product_id", trim(col("product_id"))) \
    .withColumn("store_id", trim(col("store_id"))) \
    .withColumn("order_amount",
        expr("try_cast(regexp_replace(order_amount, ',', '') AS DOUBLE)")
    ) \
    .withColumn("sales_channel", lower(trim(col("sales_channel")))) \
    .withColumn("order_timestamp",
        coalesce(
            expr("timestamp_millis(try_cast(order_timestamp AS BIGINT))"),
            expr("try_to_timestamp(trim(order_timestamp))")
        )
    )

# COMMAND ----------

# DBTITLE 1,WRITING SILVER AND REJECTED
from pyspark.sql.functions import col

silver_df = clean_df.filter(
    col("order_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("product_id").isNotNull() &
    col("store_id").isNotNull() &
    col("order_amount").isNotNull() &
    (col("order_amount") > 0) &
    col("order_timestamp").isNotNull()
)

rejected_df = clean_df.filter(
    col("order_id").isNull() |
    col("customer_id").isNull() |
    col("product_id").isNull() |
    col("store_id").isNull() |
    col("order_amount").isNull() |
    (col("order_amount") <= 0) |
    col("order_timestamp").isNull()
)

# COMMAND ----------

# DBTITLE 1,COUNT
total_count = clean_df.count()
silver_count = silver_df.count()
rejected_count = rejected_df.count()

print(f"Total: {total_count}")
print(f"Inserted (Silver): {silver_count}")
print(f"Rejected: {rejected_count}")

# COMMAND ----------

# DBTITLE 1,WRITE IN ADLS
from delta.tables import DeltaTable

silver_path = "abfss://silver@strretail360.dfs.core.windows.net/sales/sales_silver"

if DeltaTable.isDeltaTable(spark, silver_path):

    delta_table = DeltaTable.forPath(spark, silver_path)

    delta_table.alias("t").merge(
        silver_df.alias("s"),
        "t.order_id  = s.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

else:
    
    silver_df.write.format("delta") \
        .mode("overwrite") \
        .save(silver_path)

# rejected
rejected_df.write.format("delta") \
    .mode("append") \
    .save("abfss://silver@strretail360.dfs.core.windows.net/sales/sales_rejected")

# COMMAND ----------

# DBTITLE 1,GENERATE NEW WATERMARK
# GENERATE NEW WATERMARK
new_wm = clean_df.agg({"ingestion_time": "max"}).collect()[0][0]

dbutils.notebook.exit(str(new_wm))

# COMMAND ----------

silver_df.display()