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

driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.format("delta").load(f"{processed_folder_path}/drivers"))