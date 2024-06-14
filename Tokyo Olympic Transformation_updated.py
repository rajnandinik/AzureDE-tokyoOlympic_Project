# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": " ",
           "fs.azure.account.oauth2.client.secret": ' ',
           "fs.azure.account.oauth2.client.endpoint": " "}

dbutils.fs.mount(
    # contrainer@storageacc
    source="abfss://tokyo-olympic-data@tokyoolympicdatanandini.dfs.core.windows.net",
    mount_point="/mnt/tokyoolymic",
    extra_configs=configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

athelets_df = spark.read.format("csv").option("header", "True").option(
    "inferSchema", "True").load('/mnt/tokyoolymic/rawData/athletes.csv')
coaches_df = spark.read.format("csv").option("header", "True").option(
    "inferSchema", "True").load('/mnt/tokyoolymic/rawData/coaches.csv')
medal_df = spark.read.format("csv").option("header", "True").option(
    "inferSchema", "True").load('/mnt/tokyoolymic/rawData/medals.csv')
entriesGender_df = spark.read.format("csv").option("header", "True").option(
    "inferSchema", "True").load('/mnt/tokyoolymic/rawData/entriesGender.csv')
teams_df = spark.read.format("csv").option("header", "True").option("inferSchema", "True").load('/mnt/tokyoolymic/rawData/teams.csv'
                                                                                                )

# COMMAND ----------

entriesGender_df.show()
entriesGender_df.printSchema()

# COMMAND ----------

entriesGender_df = entriesGender_df.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
    .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

entriesGender_df.printSchema()

# COMMAND ----------

medal_df.show()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_country_df = medal_df.orderBy(
    "Gold", ascending=False).select("Team_Country", "Gold")
top_gold_medal_country_df.show()

# COMMAND ----------

athelets_df.write.mode("overwrite").option("header", "True").csv(
    "/mnt/tokyoolymic/transformedData/athletes")
coaches_df.write.mode("overwrite").option("header", "True").csv(
    "/mnt/tokyoolymic/transformedData/coaches")
medal_df.write.mode("overwrite").option("header", "True").csv(
    "/mnt/tokyoolymic/transformedData/medal")
entriesGender_df.write.mode("overwrite").option("header", "True").csv(
    "/mnt/tokyoolymic/transformedData/entriesGender")
teams_df.write.mode("overwrite").option("header", "True").csv(
    "/mnt/tokyoolymic/transformedData/teams")
