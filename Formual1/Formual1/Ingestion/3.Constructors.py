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

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/processed/constructors/

# COMMAND ----------

display(spark.read.format("delta").load(f"{processed_folder_path}/constructors/"))