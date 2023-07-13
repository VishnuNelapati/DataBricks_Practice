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
# MAGIC ls /mnt/formula1datalakestorage1/raw

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

lap_times_final_df.write.mode("append").format("parquet").saveAsTable("f1_processed.laptimes")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/processed/

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/laptimes"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.laptimes
# MAGIC group by race_id
# MAGIC order by race_id DESC;

# COMMAND ----------

