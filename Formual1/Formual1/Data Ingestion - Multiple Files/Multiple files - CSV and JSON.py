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
# MAGIC ### LAP TIMES - CSV

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/raw/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/raw/lap_times/

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 1 - Reading the laptimes data from laptimes folder.
# MAGIC   * As we can see there are multiple CSV files in the folder. We can read entire folder or we can use wild card to read only specified files. 

# COMMAND ----------

laptimes_schema = "raceId INT,driverId INT,lap INT,position INT,time STRING,milliseconds INT"

# COMMAND ----------

# Reading Entire folder
spark.read.schema(laptimes_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times/")

# COMMAND ----------

# Reading only required files using the wild card
lap_times_df = spark.read.schema(laptimes_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 2 - Adding ingestion date to the lap_time_df

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit
lap_times_final_df = lap_times_df.withColumn("ingestion_date",current_timestamp())\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 3 - Write Output to the data storage processed folder

# COMMAND ----------

lap_times_final_df.write.options(mode = "overwrite").format("parquet").saveAsTable("f1_processed.laptimes")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/processed/

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/laptimes"))

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

qualifying_schema = "constructorId INT,driverId INT,number INT,position INT,q1 STRING,q2 STRING,q3 STRING,qualifyId INT,raceId INT"

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split*.json",multiLine=True)

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

qualifying_renamed_df.write.options(mode="overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying/"))

# COMMAND ----------

