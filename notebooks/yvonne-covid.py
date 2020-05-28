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

# Explore patient data
patient_df = spark.read.json("mnt/fhir_patient_data/Patient_01.ndjson")
display(patient_df)

# COMMAND ----------

# Dataset size
print((patient_df.count(), len(patient_df.columns)))

# COMMAND ----------

# Explore patient conditions data
condition_df = spark.read.json("mnt/fhir_patient_data/Condition_01.ndjson")
display(condition_df)

# COMMAND ----------

# Data Transformation

# Map conditions to patients
from pyspark.sql.functions import regexp_extract, col, split, collect_list, concat_ws, lit, concat, regexp_replace, size, max

condition_df = condition_df.withColumn('subject',condition_df['subject'].cast('string')).withColumn('code',condition_df['code'].cast('string')).withColumn('id', regexp_extract(col('subject'), '\/(.*)\]', 1)).withColumn('conditions', split(col('code'), ',')[1])

# Aggregate data
cond_grouped_df = condition_df.groupby('id').agg(collect_list('conditions').alias("conditions"))

# Merge DFs
patient_cond_df = patient_df.join(cond_grouped_df, on=['id'], how='left_outer').select('id', 'name', 'conditions', 'address')

# Additional data manipulation (i.e. casting columns as string, regex, filtering)
patient_cond_df2 = patient_cond_df.withColumn('address', patient_cond_df['address'].cast('string')).withColumn('conditions', patient_cond_df['conditions'].cast('string')).withColumn('name', patient_cond_df['name'].cast('string')).withColumn("address", regexp_replace(regexp_replace(regexp_replace("address", "\\]\\[", ""), "\\[", ""), "\\]", "")).withColumn("conditions", regexp_replace(regexp_replace(regexp_replace("conditions", "\\]\\[", ""), "\\[", ""), "\\]", "")).select('id', 'name', 'conditions', 'address')

# Display dataframe
display(patient_cond_df2)

# COMMAND ----------

# Check data structure
patient_cond_df2.printSchema()

# COMMAND ----------

# Extract geolocation data from 'address'

# Filter
patient_cond_filtered_df = patient_cond_df2.select('id', 'name', 'conditions', split('address', ', ').alias('address'))

# Determine the number of columns
df_sizes = patient_cond_filtered_df.select(size('address').alias('address'))
df_max = df_sizes.agg(max('address'))
nb_columns = df_max.collect()[0][0]
df_result = df.select('id', 'name', 'conditions', *[df['address'][i] for i in range(nb_columns)])

# Rename columns
final_df = df_result.select(col("id").alias("id"), col("name").alias("name"), col("conditions").alias("conditions"), col('address[1]').alias('country'), col('address[3]').alias('latitude'), col('address[5]').alias('longitude'))
display(final_df)

# COMMAND ----------

# Analyze and filter data based on a specific condition  (i.e. Acute Bronchitis)

positive_patients_df = final_df.filter(final_df.conditions.contains('Acute bronchitis (disorder)'))
display(positive_patients_df)

# COMMAND ----------

positive_patients_df.count()

# COMMAND ----------

import folium
from folium import plugins
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Convert spark dataframe to pandas dataframe
pos_patients_pd_df = positive_patients_df.select("*").toPandas()

# Zoom in specific to Massachusetts
heatmap = folium.Map([42.4072, -71.3824], zoom_start=11)

# Convert to (n, 2) nd-array format for heatmap
patientArr = pos_patients_pd_df[['latitude', 'longitude']].as_matrix()

# Plot heatmap
heatmap.add_children(plugins.HeatMap(patientArr, radius=15))
heatmap