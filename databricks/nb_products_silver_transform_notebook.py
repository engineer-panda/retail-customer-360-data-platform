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
  table="(SELECT * FROM dbo.control_table WHERE source_name = 'products') as t",
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
bronze_df = spark.read.format("parquet").load("abfss://bronze@strretail360.dfs.core.windows.net/products")

incremental_df = bronze_df.filter(f"ingestion_time > '{last_wm}'")

incremental_df.display()


# COMMAND ----------

# DBTITLE 1,DEDUP
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy("product_id") \
    .orderBy(col("ingestion_time").desc())

dedup_df = incremental_df \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1)

# COMMAND ----------

# DBTITLE 1,SILVER TRANSFORMATION
from pyspark.sql.functions import col, trim, when, regexp_replace

clean_df = dedup_df \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("product_category", trim(col("product_category"))) \
    .withColumn("brand", trim(col("brand"))) \
    .withColumn("unit_price",
        when(regexp_replace(col("unit_price"), "CAD ", "").rlike("^[0-9.]+$"),
             regexp_replace(col("unit_price"), "CAD ", "").cast("double")
        ).otherwise(None)
    ) \
    .withColumn("is_active_flag",
        when(col("is_active_flag").isin(1, "1", 1.0, "1.0"), True)
        .when(col("is_active_flag").isin(0, "0", 0.0, "0.0"), False)
        .otherwise(None)
    )

clean_df.display()

# COMMAND ----------

# DBTITLE 1,CREATING SILVER AND REJECTED
from pyspark.sql.functions import col

silver_df = clean_df.filter(
    col("product_id").isNotNull() &
    col("product_name").isNotNull() &
    col("unit_price").isNotNull() &
    col("is_active_flag").isin(True, False)
)

rejected_df = clean_df.filter(
    col("product_id").isNull() |
    col("product_name").isNull() |
    col("unit_price").isNull() |
    ~col("is_active_flag").isin(True, False)
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

silver_path = "abfss://silver@strretail360.dfs.core.windows.net/products/products_silver"

if DeltaTable.isDeltaTable(spark, silver_path):

    delta_table = DeltaTable.forPath(spark, silver_path)

    delta_table.alias("t").merge(
        clean_df.alias("s"),
        "t.product_id = s.product_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

else:
    
    clean_df.write.format("delta") \
        .mode("overwrite") \
        .save(silver_path)

# rejected
rejected_df.write.format("delta") \
    .mode("append") \
    .save("abfss://silver@strretail360.dfs.core.windows.net/products/products_rejected")

# COMMAND ----------

# DBTITLE 1,GENERATE NEW WATERMARK
new_wm = silver_df.agg({"ingestion_time": "max"}).collect()[0][0]

dbutils.notebook.exit(str(new_wm))

# COMMAND ----------

silver_df.display()