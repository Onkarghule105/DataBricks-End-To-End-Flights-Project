# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # **Parameters**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from workspace.silver.silver_passengers

# COMMAND ----------

## Catalog Name
# catalog = "workspace"

## Key Cols List
# key_cols = "['flight_id',]"
# key_col_list = eval(key_cols)

## CDC Column
# cdc_col = "modifiedDate" 

## Backdated Refresh
# backdated_refresh = ""

## Source Object
# source_object = "silver_flights"

## Source Schema
# source_schema = "silver"

## Target Schema
# target_schema = "gold"

## Target Object 
# target_object = "DimFlights"

## Surrogate key Col
# surrogate_key = "DimFlightsKey"


# COMMAND ----------

## Catalog Name
# catalog = "workspace"

## Key Cols List
# key_cols = "['airport_id',]"
# key_col_list = eval(key_cols)

## CDC Column
# cdc_col = "modifiedDate" 

## Backdated Refresh
# backdated_refresh = ""

## Source Object
# source_object = "silver_airports"

## Source Schema
# source_schema = "silver"

## Target Schema
# target_schema = "gold"

## Target Object 
# target_object = "DimAirports"

## Surrogate key Col
# surrogate_key = "DimAirportsKey"


# COMMAND ----------

# Catalog Name
catalog = "workspace"

# Key Cols List
key_cols = "['passenger_id',]"
key_col_list = eval(key_cols)

# CDC Column
cdc_col = "modifiedDate" 

# Backdated Refresh
backdated_refresh = ""

# Source Object
source_object = "silver_passengers"

# Source Schema
source_schema = "silver"

# Target Schema
target_schema = "gold"

# Target Object 
target_object = "DimPassengers"

# Surrogate key Col
surrogate_key = "DimPassengersKey"


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### **Incremental Data Ingestion**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Last Load Date**

# COMMAND ----------

# No BackDated Refresh
if  len(backdated_refresh) == 0:
    # If Table Exists in The Destination
   if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
        
        last_load = spark.sql(f"select max({cdc_col}) from {target_schema}.{target_object}").collect()[0][0]
   else:
        last_load = "1900-01-01 00:00:00"   
# Yes BackDated Refresh       
else:
    last_load = backdated_refresh       


#Test The Last Load
last_load

# COMMAND ----------

df_src = spark.sql(f"select * from {source_schema}.{source_object} where {cdc_col} > '{last_load}'")



# COMMAND ----------

# MAGIC %md
# MAGIC **# OLD vs NEW RECORDSS**

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    #Key Column String for Incremental Load
    key_col_string_incremental = ', '.join(key_col_list)

    df_trg = spark.sql(f"SELECT {key_col_string_incremental},{surrogate_key},create_date,update_date FROM {catalog}.{target_schema}.{target_object}")
else:
    # key column String for initial Load
    key_col_string_init = [f"'' AS {i}" for i in key_col_list]
    key_col_string_init = ", ".join(key_col_string_init)
   
    df_trg = spark.sql(f"""SELECT {key_col_string_init}, CAST('0' AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS timestamp) AS create_date,CAST('1900-01-01 00:00:00' AS timestamp) AS update_date where 1=0""")

# COMMAND ----------

df_trg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  **JOIN CONDITION**

# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = trg.{i}"for i in key_col_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
          SELECT src.*,
                 trg.{surrogate_key},
                 trg.create_date,
                 trg.update_date
          fROM src
          LEFT JOIN trg
          ON {join_condition}
          """)

# COMMAND ----------

df_join.display()

# COMMAND ----------

# OLD RECORDS
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())

# NEW RECORDS
df_new = df_join.filter(col(f'{surrogate_key}').isNull())


# COMMAND ----------

df_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # ## **ENRICHING DFS**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing DF_OLD**

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date',current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #### **Preparing DF_NEW**

# COMMAND ----------

df_new.display()

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"""
                         SELECT max({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}      
                        """).collect()[0][0]
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
        .withColumn('create_date',current_timestamp())\
        .withColumn('update_date',current_timestamp())
else:
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
        .withColumn('create_date',current_timestamp())\
        .withColumn('update_date',current_timestamp())



# COMMAND ----------

df_old_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Unioning OLD AND NEW RECORDS **

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)

# COMMAND ----------

df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_union.alias("src"),f"trg.{surrogate_key} = src.{surrogate_key}")\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()

else:

    df_union.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.dimpassengers where passenger_id ='P0049'