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
# MAGIC #### COnstructions file JSON

# COMMAND ----------

dbutils.fs.ls(f'{raw_folder_path}/{v_file_date}')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/raw/

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 1 - Reading the data from the mount

# COMMAND ----------

constructors_schema = "constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructors_df.show()

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 2 - Drop Unwanted columns/ Select Required columns

# COMMAND ----------

# Method 1
constructors_df.drop("url").show()

# COMMAND ----------

# Method 2
from pyspark.sql.functions import col
constructors_df.drop(col('url')).show()

# COMMAND ----------

# Method 3
constructors_df.drop(constructors_df.url).show()

# COMMAND ----------

# Method 4
constructors_df['constructorID','constructorRef','name','nationality'].show()

# COMMAND ----------

constructors_select_df = constructors_df.drop("url")

# COMMAND ----------

display(constructors_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 3 - Rename columns and add ingestion data as new column

# COMMAND ----------

constructors_renamed_df = constructors_select_df.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("constructorRef","constructor_ref")

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
constructors_final_df = constructors_renamed_df.withColumn("ingestion_date",current_timestamp())\
                                                          .withColumn("data_source",lit(v_data_source))\
                                                          .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC * Step -4 Writing files to output as Parquet files 

# COMMAND ----------

constructors_final_df.write.options(mode = "overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/processed/constructors/

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drivers File - JSON
# MAGIC 
# MAGIC * Nested JSON

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 1 - Reading the nested JSON from RAW container in BlobStorage

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType,NumericType,DateType
name_schema = StructType(fields = [StructField("forename",StringType(),True),
                                   StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema = StructType(fields = [StructField("driverId",StringType(),True),
                                     StructField("driverRef",StringType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("code",StringType(),True),
                                     StructField("name",name_schema),
                                     StructField("dob",DateType(),True),
                                     StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 2 - Renaming columns and transforming name column (name column is nested json)

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

driver_new_df = drivers_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("driverRef",'driver_ref')\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(driver_new_df)

# COMMAND ----------

# MAGIC  %md
# MAGIC  * Step 3 - Drop unwanted columns

# COMMAND ----------

driver_final_df = driver_new_df.drop("url")

# COMMAND ----------

display(driver_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - Writing files as parquest files

# COMMAND ----------

driver_final_df.write.options(mode = "overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))

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
# MAGIC Step -4 Write final df to blob storage processed container by partioning with race id

# COMMAND ----------

results_final_df.write.partitionBy("race_id").options(mode = "overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1datalakestorage1/processed/results

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

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

pitstop_final_df.write.options(mode = "overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

