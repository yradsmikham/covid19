# Databricks notebook source
# Unmount container
dbutils.fs.unmount("/mnt/")

# COMMAND ----------

# Session Configuration
app_id = "cf586367-1ec1-4c0c-8cca-6c1446ff27a0"
secret = "1ebcb5a1-340b-4c5d-9a4d-68663f7fb6f3"
tenant_id = "72f988bf-86f1-41af-91ab-2d7cd011db47"

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "cf586367-1ec1-4c0c-8cca-6c1446ff27a0",
       "fs.azure.account.oauth2.client.secret": "1ebcb5a1-340b-4c5d-9a4d-68663f7fb6f3",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token",
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
patient = spark.read.json("mnt/fhir_patient_data/Patient.ndjson")
display(patient)

# COMMAND ----------

# Additional Data Exploration
condition = spark.read.json("mnt/fhir_patient_data/Condition.ndjson")
display(condition)

# COMMAND ----------

# Transform data
# Map conditions to patients

# COMMAND ----------

# Export to Azure Data Lake 