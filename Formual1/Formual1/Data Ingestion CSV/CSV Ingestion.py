# Databricks notebook source
# MAGIC %md
# MAGIC #### 1)Read data stored in azure data lake using Spark dataframe
# MAGIC * https://spark.apache.org/docs/latest/api/python/reference/index.html

# COMMAND ----------

# MAGIC %run "../DB Workflows/configuration"

# COMMAND ----------

# MAGIC %run "../DB Workflows/common functions"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

dbutils.fs.ls(f"{raw_folder_path}")

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
# MAGIC * We have all the required datasets for our transofrmation in raw folder.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1datalakestorage1/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Circuits Files

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,DoubleType,StringType

# COMMAND ----------

circuit_schema = StructType(fields=[StructField("circuitId",IntegerType(),nullable=False),
                                   StructField("circuitRef",StringType(),nullable=True),
                                   StructField("name",StringType(),nullable=True),
                                   StructField("location",StringType(),nullable=True),
                                   StructField("country",StringType(),nullable=True),
                                   StructField("lat",DoubleType(),nullable=True),
                                   StructField("lng",DoubleType(),nullable=True),
                                   StructField("alt",IntegerType(),nullable=True),
                                   StructField("url",StringType(),nullable=True)])

# COMMAND ----------

circuits_df = spark.read.schema(circuit_schema).csv(path = f'{raw_folder_path}/{v_file_date}/circuits.csv',header = True)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

circuits_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Selecting required columns from the data frame

# COMMAND ----------

circuits_selected_df = circuits_df["circuitId","circuitRef","name","location","country","lat","lng","alt"]

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * Using COL function we can make alias names for the columns of a dataframe

# COMMAND ----------

# from pyspark.sql.functions import col

# circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat").alias("Latitude"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Rename Columns of the dataframe

# COMMAND ----------

from pyspark.sql.functions import col,lit

# circuits_renamed_df = circuits_selected_df.select(col("circuitId").alias("circuit_id"),
#                                           col("circuitRef").alias("circuitRef"),
#                                           col("name"),
#                                           col("location"),
#                                           col("country"),
#                                           col("lat").alias("latitude"),
#                                           col("lng").alias("longitude"),
#                                           col("alt").alias("altitude"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 4) Add a new column to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

display(circuits_renamed_df.withColumn("env",lit("production")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Write data to parquet file and store in data lake

# COMMAND ----------

circuits_final_df.write.options(mode = "overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1datalakestorage1/processed/circuits
# MAGIC  

# COMMAND ----------

df = spark.read.parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Race Data

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 1 Reading the data

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1datalakestorage1/raw/

# COMMAND ----------

from pyspark.sql.types import IntegerType,DateType,TimestampType,StringType,DoubleType,StructField,StructType

# COMMAND ----------

race_schema = StructType(fields = [StructField("raceId",IntegerType(),False),
                                   StructField("year",IntegerType(),True),
                                   StructField("round",IntegerType(),True),
                                   StructField("circuitId",IntegerType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("date",StringType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("url",StringType(),True)])

# COMMAND ----------

race_df = spark.read.schema(race_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv",header = True)

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

race_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 2 Selecting required columns

# COMMAND ----------

race_selected_df = race_df["raceId","year","round","circuitId","name","date","time"]

# COMMAND ----------

display(race_selected_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC * Step 3 Renaming the columns

# COMMAND ----------

race_renamed_df = race_selected_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("year","race_year")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(race_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC * Step 4 Add New column to data frame

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat
race_final_df = race_renamed_df.withColumn('race_timestamp',to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

race_final_df = race_final_df.withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

race_final_df = race_final_df['race_id','race_year','round','circuit_id','name','race_timestamp','ingestion_date','data_source','file_date']

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 5 Writing to data lake as parquet files

# COMMAND ----------

race_final_df.write.partitionBy("race_year").options(mode = "overwrite").format("parquet").saveAsTable("f1_processed.race")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls mnt/formula1datalakestorage1/processed/race/

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/race"))

# COMMAND ----------

