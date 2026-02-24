# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading , Writing and Creating tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxidatasetstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidatasetstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxidatasetstorage.dfs.core.windows.net", "appid")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxidatasetstorage.dfs.core.windows.net","secret")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxidatasetstorage.dfs.core.windows.net", "https://login.microsoftonline.com/9157991e-e378-4d3e-8476-cd4548687bb2/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Database creation

# COMMAND ----------

# MAGIC %sql
# MAGIC create database gold

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage Variables

# COMMAND ----------

silver = 'abfss://silver@nyctaxidatasetstorage.dfs.core.windows.net'
gold =   'abfss://gold@nyctaxidatasetstorage.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Zone Data from silver table 

# COMMAND ----------

df_zone = spark.read.format('parquet')\
                 .option('inferSchema',True)\
                 .option('header',True)\
                 .load(f'{silver}/trip_zone')

# COMMAND ----------

df_zone.display()  # Check data source or reload with supported scheme

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Delta table in gold database/schema

# COMMAND ----------

df_zone.write.format('delta')\
       .mode('append')\
        .option('path',f'{gold}/trip_zone')\
            .saveAsTable('gold.df_zone')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Accessing data from delta file (external table) using SQL queries

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.df_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping accidentally created table in default zone

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table df_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### listing all stroage credentials

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping wrongly created external location

# COMMAND ----------

# MAGIC %sql
# MAGIC drop external location nyclatestexternalloc
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read trip type data from silver table
# MAGIC

# COMMAND ----------

df_type = spark.read.format('parquet')\
                 .option('inferSchema',True)\
                 .option('header',True)\
                 .load(f'{silver}/trip_type')

# COMMAND ----------

df_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing data to gold and creating delta table for trip_type

# COMMAND ----------

df_type.write.format('delta')\
       .mode('append')\
        .option('path',f'{gold}/trip_type')\
            .saveAsTable('gold.df_type')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.df_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing data to gold and creating delta table for trip_data

# COMMAND ----------

df_tripdata = spark.read.format('parquet')\
                 .option('inferSchema',True)\
                 .option('header',True)\
                 .load(f'{silver}/trip2023data')

# COMMAND ----------

df_tripdata.display()

# COMMAND ----------

df_tripdata.write.format('delta')\
       .mode('append')\
        .option('path',f'{gold}/tripdata')\
            .saveAsTable('gold.tripdata')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from gold.tripdata

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.tripdata
