# Databricks notebook source
# Session Configuration
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "<app_id>",
       "fs.azure.account.oauth2.client.secret": "<secret>",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant_id>/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# Mount container
dbutils.fs.mount(
source = "abfss://<container_name>@<storage_account>.dfs.core.windows.net/",
mount_point = "/mnt/",
extra_configs = configs)

# COMMAND ----------

# Display dbrix file system 
display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

# Explore patient data
patient_df = spark.read.json("mnt/fhir_patient_data/Patient.ndjson")
display(patient_df)

# COMMAND ----------

# Explore patient conditions data
condition_df = spark.read.json("mnt/fhir_patient_data/Condition.ndjson")
display(condition_df)

# COMMAND ----------

# Transform data

# Map conditions to patients
from pyspark.sql.functions import regexp_extract, col, split, collect_list, concat_ws, lit, concat

condition_df = condition_df.withColumn('subject',condition_df['subject'].cast('string')).withColumn('code',condition_df['code'].cast('string')).withColumn('id', regexp_extract(col('subject'), '\/(.*)\]', 1)).withColumn('conditions', split(col('code'), ',')[1])

# Aggregate data
cond_grouped_df = condition_df.groupby('id').agg(collect_list('conditions').alias("conditions"))

# Merge DFs
patient_cond_df = patient_df.join(cond_grouped_df, on=['id'], how='left_outer')
patient_cond_df.select('id', 'name', 'conditions').show(100)

# COMMAND ----------

# Analyze and filter data based on a specific condition 

positive_patients_df = condition_df.filter(condition_df.conditions.contains('Acute bronchitis (disorder)')).groupby('id').agg(collect_list('conditions').alias("conditions"))
positive_patients_df.select('id', 'conditions').show()

# COMMAND ----------

# Export to Azure Data Lake 
spark.conf.set("fs.azure.account.key.<storage_account>.dfs.core.windows.net", "<access_key>")
dbutils.fs.ls("abfss://<container_name>@<storage_account>.dfs.core.windows.net/")
positive_patients_df.write.format("json").save("abfss://<container_name>@<storage_account>.dfs.core.windows.net/positive_patients.json")