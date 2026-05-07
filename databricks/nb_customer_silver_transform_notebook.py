# Databricks notebook source
# DBTITLE 1,READ CONTROL TABLE
jdbc_url = "jdbc:sqlserver://customerretaiilproject.database.windows.net:1433;database=dbretailcustomers"

properties = {
  "user": "username",
  "password": "#####",
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

control_df = spark.read.jdbc(
  url=jdbc_url,
  table="(SELECT * FROM dbo.control_table WHERE source_name = 'customers') as t",
  properties=properties
)

control_df.display()
control_df.printSchema()

# COMMAND ----------

# DBTITLE 1,PERMISSIONS
spark.conf.set("fs.azure.account.auth.type.strretail360.dfs.core.windows.net", "OAuth")

spark.conf.set("fs.azure.account.oauth.provider.type.strretail360.dfs.core.windows.net",
"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set("fs.azure.account.oauth2.client.id.strretail360.dfs.core.windows.net",
"")

spark.conf.set("fs.azure.account.oauth2.client.secret.strretail360.dfs.core.windows.net",
"")

spark.conf.set("fs.azure.account.oauth2.client.endpoint.strretail360.dfs.core.windows.net",
"https://login.microsoftonline.com/2022e591-a28c-4f78-bf86-066e6c7bd734/oauth2/token")

# COMMAND ----------

# DBTITLE 1,GETTING LAST WATERMARK
last_wm = control_df.select("last_watermark_value").first()[0]
print(last_wm)

# COMMAND ----------

# DBTITLE 1,READ BRONZE
bronze_df = spark.read.format("parquet").load(
  "abfss://bronze@strretail360.dfs.core.windows.net/customers"
)

incremental_df = bronze_df.filter(
  f"ingestion_time > '{last_wm}'"
)

incremental_df.show()


# COMMAND ----------

# DBTITLE 1,DEDUP
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy("customer_id") \
    .orderBy(col("ingestion_time").desc())

dedup_df = incremental_df \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1)

# COMMAND ----------

# DBTITLE 1,SILVER TRANSFORMATION
from pyspark.sql.functions import col, trim, lower, initcap, upper, when, regexp_replace, coalesce, expr

clean_df = (
    dedup_df
    .withColumn("full_name", initcap(trim(col("full_name"))))
    .withColumn("email_address", lower(trim(col("email_address"))))
    .withColumn("city", initcap(trim(regexp_replace(col("city"), "[0-9]", ""))))
    
    # Province standardization
    .withColumn("province", upper(trim(col("province"))))
    .withColumn(
        "province",
        when(col("province") == "ONTARIO", "ON")
        .when(col("province") == "??", None)
        .otherwise(col("province"))
    )
    
    # Date parsing
    .withColumn(
        "registration_date",
        coalesce(
            expr("try_to_date(trim(registration_date), 'yyyy-MM-dd')"),
            expr("try_to_date(trim(registration_date), 'yyyy/MM/dd')")
        )
    )
    
    # Loyalty flag normalization
    .withColumn(
        "is_loyalty_member",
        when(col("is_loyalty_member").cast("string").isin("1","true","Y","y"), True)
        .when(col("is_loyalty_member").cast("string").isin("0","false","N","n"), False)
        .otherwise(None)
    )
    
    # Filters
    .filter(col("email_address").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$"))
    .filter(col("registration_date").isNotNull())
)

clean_df.display()

# COMMAND ----------

# DBTITLE 1,WRITING SILVER AND REJECTED
from pyspark.sql.functions import col

silver_df = clean_df.filter(
    col("customer_id").isNotNull() &
    col("full_name").isNotNull()
)

rejected_df = clean_df.subtract(silver_df)

silver_df.display()
rejected_df.display()

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
silver_path = "abfss://silver@strretail360.dfs.core.windows.net/customers/customers_silver"

if DeltaTable.isDeltaTable(spark, silver_path):

    delta_table = DeltaTable.forPath(spark, silver_path)

    delta_table.alias("t").merge(
        silver_df.alias("s"),
        "t.customer_id = s.customer_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

else:
    
    silver_df.write.format("delta") \
        .mode("overwrite") \
        .save(silver_path)

# COMMAND ----------

# DBTITLE 1,READ SILVER
silver_df = spark.read.format("delta").load("abfss://silver@strretail360.dfs.core.windows.net/customers/customers_silver/")
silver_df.display()


# COMMAND ----------

# DBTITLE 1,GENERATE NEW WATERMARK
# GENERATE NEW WATERMARK
new_wm = clean_df.agg({"ingestion_time": "max"}).collect()[0][0]

dbutils.notebook.exit(str(new_wm))