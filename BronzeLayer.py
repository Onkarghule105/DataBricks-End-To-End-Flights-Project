# Databricks notebook source
# MAGIC %md
# MAGIC  **Basic Connfiguration**

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE VOLUME workspace.bronze.bronzevolume
# MAGIC --CREATE VOLUME workspace.silver.silvervolume
# MAGIC --CREATE VOLUME workspace.gold.goldvolume
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE VOLUME workspace.raw.bronze
# MAGIC --CREATE VOLUME workspace.raw.silver
# MAGIC --CREATE VOLUME workspace.raw.gold

# COMMAND ----------

# MAGIC %md
# MAGIC **INCREMENTAL DATA INGESTION**

# COMMAND ----------

# Creating Parameters
dbutils.widgets.text("src", "")

# COMMAND ----------

src_value = dbutils.widgets.get("src")

# COMMAND ----------

# Read Data from location
df = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "CSV")\
            .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
            .option("cloudFiles.schemaEvolutionMode","rescue")\
            .load(f"/Volumes/workspace/raw/rawvolume/rawdata/{src_value}/")

# COMMAND ----------

# Write Data to location
df.writeStream.format("delta")\
            .outputMode("append")\
            .trigger(once=True)\
            .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
            .option("path", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data")\
            .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/customers/data/`