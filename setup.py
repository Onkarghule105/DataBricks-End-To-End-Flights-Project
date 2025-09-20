# Databricks notebook source
# MAGIC %sql
# MAGIC --CREATE VOLUME workspace.raw.rawvolume

# COMMAND ----------

#dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata")

# COMMAND ----------

# dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/bookings")

# COMMAND ----------

# dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/flights")

# COMMAND ----------

# dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/customers")

# COMMAND ----------

# dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE SCHEMA workspace.bronze
# MAGIC --CREATE SCHEMA workspace.silver
# MAGIC --CREATE SCHEMA workspace.gold