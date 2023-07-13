# Databricks notebook source
# MAGIC %run "../DB Workflows/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualifying - JSON files

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1datalakestorage1/raw/

# COMMAND ----------

# MAGIC %md
# MAGIC * STEP 1 - Reading the JSON files 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col

# COMMAND ----------

qualifying_schema = "constructorId INT,driverId INT,number INT,position INT,q1 STRING,q2 STRING,q3 STRING,qualifyId INT,raceId INT"

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).json(f"{raw_folder_path}/{v_file_date}/qualifying",multiLine=True)

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC STEP -2 Renaming the columns and ading ingestion date 

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 3 - Writing files to data lake storage

# COMMAND ----------

qualifying_renamed_df.write.mode("append").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id DESC;