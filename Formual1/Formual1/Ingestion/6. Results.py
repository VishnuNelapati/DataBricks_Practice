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
# MAGIC ### Results JSON file

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/raw/

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Reading the data results from blob storage

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType,DateType,TimestampType,FloatType
from pyspark.sql.functions import lit,col,current_timestamp

# COMMAND ----------

results_schema = "resultId INT,raceId INT,driverId INT,constructorId INT,number INT,grid INT,position INT,positionText STRING,positionOrder INT,points FLOAT,laps INT,time STRING,milliseconds INT,fastestLap INT,fastestLapTime STRING,fastestLapSpeed STRING,statusId INT"

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC * STEP 2 - Rename the columns 

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId","result_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("positionText","position_text")\
.withColumnRenamed("positionOrder","position_order")\
.withColumnRenamed("fastestLap","fastest_lap")\
.withColumnRenamed("fastestLapTime","fastest_lap_time")\
.withColumnRenamed("FastestLapSpeed","fastest_lap_speed")\
.withColumnRenamed("statusId","status_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC * Step -3 Drop unwanted columns

# COMMAND ----------

results_final_df = results_renamed_df.drop("status_id")

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop Duplicates

# COMMAND ----------

# results_final_df.createTempView("results_view1")

# COMMAND ----------

# %sql
# select driver_id,race_id,count(1) from results_view1
# GROUP BY driver_id,race_id
# HAVING count(1) > 1;

# COMMAND ----------

results_final_df = results_final_df.drop_duplicates(subset=['driver_id','race_id'])

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step -4 Write final df to blob storage processed container by partioning with race id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Load - 1

# COMMAND ----------

# # Incremental Load - 1
# for race_res in results_final_df.select("race_id").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_res.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Load - 2

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

results_final_df = rearrange_data(results_final_df,"race_id")
display(results_final_df)

# COMMAND ----------

# write_data(results_final_df,"f1_processed.results","race_id")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
merge_delta_table(results_final_df,"f1_processed.results","race_id",processed_folder_path,merge_condition)

# COMMAND ----------

# # Incremental load - 2

# from delta.tables import DeltaTable

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#     deltaTable = DeltaTable.forPath(spark,"/mnt/formula1datalakestorage1/processed/results")
#     deltaTable.alias("tgt").merge(
#         results_final_df.alias("src"),
#         "tgt.result_id = src.result_id and tgt.race_id = src.race_id")\
#     .whenMatchedUpdateAll()\
#     .whenNotMatchedInsertAll().execute()
# else:
#     results_final_df.write.mode("append").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1datalakestorage1/raw/

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED f1_processed.drivers

# COMMAND ----------

