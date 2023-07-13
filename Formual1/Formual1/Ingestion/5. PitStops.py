# Databricks notebook source
# MAGIC %run "../DB Workflows/configuration"

# COMMAND ----------

# MAGIC %run "../DB Workflows/common_functions"

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
# MAGIC ### pit_stops JSON file
# MAGIC * PIT_STOP.JSON is multiline JSON file

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/raw/

# COMMAND ----------

pitstop_schema = "raceId INT,driverId INT,stop STRING,lap INT,time STRING,duration STRING,milliseconds INT"

# COMMAND ----------

pitstop_df = spark.read.schema(pitstop_schema).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json",multiLine = True)

# COMMAND ----------

pitstop_df.printSchema()

# COMMAND ----------

display(pitstop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 2 - Renaming columns and adding additional Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
pitstop_final_df = pitstop_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(pitstop_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 3 - Writing the final df to blob Storage as parquet file

# COMMAND ----------

pitstop_final_df.write.mode("append").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

