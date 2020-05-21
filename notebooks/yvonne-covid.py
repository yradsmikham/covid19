# Databricks notebook source
# Unmount container
dbutils.fs.unmount("/mnt/")

# COMMAND ----------

# Session Configuration
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "*****",
       "fs.azure.account.oauth2.client.secret": "*****",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/*****/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# Mount container
dbutils.fs.mount(
source = "abfss://fhir@yvonnesourcedev.dfs.core.windows.net/",
mount_point = "/mnt/",
extra_configs = configs)

# COMMAND ----------

# Display dbrix file system 
display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

# Explore data
patient_df = spark.read.json("mnt/fhir_patient_data/Patient.ndjson")
display(patient_df)

# COMMAND ----------

# Additional Data Exploration
condition_df = spark.read.json("mnt/fhir_patient_data/Condition.ndjson")
display(condition_df)

# COMMAND ----------

# Transform data
# Map conditions to patients

from pyspark.sql.functions import regexp_extract, col, split
condition_df = condition_df.withColumn('subject',condition_df['subject'].cast('string')).withColumn('code',condition_df['code'].cast('string'))
condition_df2 = condition_df.withColumn('id', regexp_extract(col('subject'), '\/(.*)\]', 1)).withColumn('conditions', split(col('code'), ',')[1])
condition_df2.select('id', 'conditions').show(100,truncate=False)

# COMMAND ----------

# Merge DFs

patient_cond_df = patient_df.join(condition_df2, on=['id'], how='left_outer').select('name', 'id', 'conditions').show(100, truncate=False)

# COMMAND ----------

# Export to Azure Data Lake 