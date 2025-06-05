# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.azstoragetraining.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azstoragetraining.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azstoragetraining.dfs.core.windows.net", "49bbd4cd-c36c-4eb5-96a4-e1ebad573555")
spark.conf.set("fs.azure.account.oauth2.client.secret.azstoragetraining.dfs.core.windows.net", "xSN8Q~1zr9rSHsDCnKSj7og~MDuZIJnugFMEzaLU")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azstoragetraining.dfs.core.windows.net", "https://login.microsoftonline.com/45da25de-8003-4abf-bea8-55aba615e5e7/oauth2/token")
 

# COMMAND ----------

input_container_path = "abfss://autoloader-suman-input@azstoragetraining.dfs.core.windows.net/input_folder"
output_container_path = "abfss://autoloader-suman-output@azstoragetraining.dfs.core.windows.net/output_folder"


# COMMAND ----------

display(dbutils.fs.ls(input_container_path))
display(dbutils.fs.ls(output_container_path))



# COMMAND ----------

#from pyspark.sql.functions import input_file_name
image_df = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.maxFilesPerTrigger", "1")
            .load(input_container_path)
            #.withColumn("filename", input_file_name())
           )
image_df.printSchema()

# COMMAND ----------

display(image_df)

# COMMAND ----------

stream_query = image_df.writeStream \
         .format("delta") \
         .option("checkpointLocation", f"{output_container_path}/checkpoint/") \
         .option("path", f"{output_container_path}/images_binary") \
         .start()

# COMMAND ----------

stream_query.stop()