# Databricks notebook source
# MAGIC %md
# MAGIC #### Set Spark properties to configure Azure credentials to access Azure storage
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.azstoragetraining.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azstoragetraining.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azstoragetraining.dfs.core.windows.net", "49bbd4cd-c36c-4eb5-96a4-e1ebad573555")
spark.conf.set("fs.azure.account.oauth2.client.secret.azstoragetraining.dfs.core.windows.net", "xSN8Q~1zr9rSHsDCnKSj7og~MDuZIJnugFMEzaLU")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azstoragetraining.dfs.core.windows.net", "https://login.microsoftonline.com/45da25de-8003-4abf-bea8-55aba615e5e7/oauth2/token")
 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import required functions & libraries

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp, to_date, date_format, col
from pyspark.sql import SparkSession


# COMMAND ----------

# MAGIC %md
# MAGIC #### Path to the data lake container

# COMMAND ----------

container_path = "abfss://student-marks@azstoragetraining.dfs.core.windows.net/"


# COMMAND ----------

# MAGIC %md
# MAGIC #### List all files in the container folder

# COMMAND ----------

files = dbutils.fs.ls(container_path)
display(files)

# COMMAND ----------

#Create an empty dataframe to store the final result
#All cleaned-up data from each file will be stored here  
result_df = None
#loop through each files individually  and read it one by one
for file in files:
# Read the file (without header) & removes completely empty rows that contains null values
    df = spark.read.option("header", False).csv(file.path).na.drop(how="all")
    display(df)
    # Extract metadata
    name = df.select("_c0").head()[0].split(":")[1].strip()
    display(name)
    date = df.select("_c1").head(2)[1][0].split(":")[1].strip()
    display(date)
    time = df.select("_c3").head()[0].split(":")[1].strip() + ":" + df.select("_c3").head()[0].split(":")[2].strip()
    display(time)

    # Clean and transform
    df = df.drop("_c3") \
        .withColumn("Name", lit(name)) \
        .withColumn("Time", lit(time)) \
        .withColumn("Date", lit(date)) \
        .withColumn("Time", date_format(to_timestamp("Time", "HH:mm"), "hh:mm a")) \
        .withColumn("Date", date_format(to_date("Date", "dd-MMM-yyyy"), "yyyy-MM-dd")) \
        .select("Name", "Time", "Date","_c0", "_c1", "_c2") \
        .fillna("Unknown")
    display(df)
    # Skip first 3 metadata rows
    #df = df.rdd.zipWithIndex().filter(lambda x: x[1] >= 3).map(lambda x: x[0]).toDF(df.columns)
    #df = df.exceptAll(df.limit(3))
    # Rename columns
    df = df.withColumnRenamed("_c0", "Subject") \
           .withColumnRenamed("_c1", "Attempt_1") \
           .withColumnRenamed("_c2", "Attempt_2")
    display(df)
    # Filter out Unknown rows
    df = df.filter(
        (col("Name") != "Unknown") &
        (col("Subject") != "Unknown") &
        (col("Time") != "Unknown") &
        (col("Date") != "Unknown") &
        (col("Attempt_1") != "Unknown") &
        (col("Attempt_2") != "Unknown") 
    )
    display(df)
    df = df.filter(col("Subject") != "Subject")
    display(df)
    # Append to final DataFrame
    if result_df is None:
        result_df = df
    else:
        result_df = result_df.union(df)

# Show the result
result_df.display()
