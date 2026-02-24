# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.nyctaxidatasetstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidatasetstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxidatasetstorage.dfs.core.windows.net", "appid")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxidatasetstorage.dfs.core.windows.net","secret")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxidatasetstorage.dfs.core.windows.net", "https://login.microsoftonline.com/9157991e-e378-4d3e-8476-cd4548687bb2/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@nyctaxidatasetstorage.dfs.core.windows.net/")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing libraries

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading csv data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading trip type data

# COMMAND ----------

df = spark.read.format("csv").\
    load( "abfss://bronze@nyctaxidatasetstorage.dfs.core.windows.net/trip_type/trip_type.csv",\
         header=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading trip Zone

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

df_tripzone = spark.read.format('csv')\
                .option('inferSchema',True)\
                .option('header',True).load('abfss://bronze@nyctaxidatasetstorage.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_tripzone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Parquet files

# COMMAND ----------

df_parquet = spark.read.format('parquet')\
    .schema(myschema)\
    .option('header',True)\
    .option('recursiveFileLookup','true')\
    .load('abfss://bronze@nyctaxidatasetstorage.dfs.core.windows.net/tripdata/')

# COMMAND ----------

myschema = '''
    VendorID BIGINT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    RatecodeID BIGINT,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type BIGINT,
    trip_type BIGINT,
    congestion_surcharge DOUBLE
'''

# COMMAND ----------

df_parquet.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transfomration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip Type data transformation

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumnRenamed('description','triptype_description')
display(df)

# COMMAND ----------

df.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@nyctaxidatasetstorage.dfs.core.windows.net/trip_type")\
            .save()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip Zone data file creation in silver layer

# COMMAND ----------

df_tripzone.display()

# COMMAND ----------

df_tripzone = df_tripzone.withColumn('zone1',split('zone','/')[0])\
                          .withColumn('zone2',split('zone','/')[1])
df_tripzone.display()
                          

# COMMAND ----------

df_tripzone.write.format('parquet')\
                         .mode('append')\
                             .option("path","abfss://silver@nyctaxidatasetstorage.dfs.core.windows.net/trip_zone")\
                                 .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trip Data Data Transormation into Silver Layer

# COMMAND ----------

df_parquet.display()

# COMMAND ----------

df_parquet = df_parquet.withColumn('trip_date',to_date('lpep_pickup_datetime'))\
    .withColumn('trip_year',year('lpep_pickup_datetime'))\
        .withColumn('trip_month',month('lpep_pickup_datetime'))\
            .withColumn('trip_day',dayofmonth('lpep_pickup_datetime'))
df_parquet.display()


# COMMAND ----------

df_parquet.display()

# COMMAND ----------

df_parquet = df_parquet.select("VendorID",'PUlocationID','DOLocationID','trip_distance','fare_amount','total_amount')
df_parquet.display()


# COMMAND ----------

df_parquet.display()

# COMMAND ----------

df_parquet.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@nyctaxidatasetstorage.dfs.core.windows.net/trip2023data')\
            .save()

# COMMAND ----------

display(df_parquet)
