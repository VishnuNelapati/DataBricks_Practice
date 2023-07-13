# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating the tables 
# MAGIC * Write data to delta lake (managed table)
# MAGIC * Write data to delta lake (external table)
# MAGIC * Read data from delta lake (table)
# MAGIC * Read data from delta lake (file)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/demo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC location "/mnt/formula1datalakestorage1/demo"

# COMMAND ----------

results_df = spark.read.option("inferSchema",True).json("/mnt/formula1datalakestorage1/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1datalakestorage1/demo/results_external")

# COMMAND ----------

spark.read.format("delta").load("/mnt/formula1datalakestorage1/demo/results_external").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING delta
# MAGIC LOCATION "/mnt/formula1datalakestorage1/demo/results_external"

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC ## Updates and Deletes on tables

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_external
# MAGIC SET driverId = 100
# MAGIC WHERE constructorId in (131)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark,path="/mnt/formula1datalakestorage1/demo/results_external")

deltaTable.update("position <=10",{"points":"11-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_external 
# MAGIC WHERE constructorId = 131;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark,"/mnt/formula1datalakestorage1/demo/results_external")

deltaTable.delete("constructorId = 9")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert and Merge 

# COMMAND ----------

# MAGIC %md
# MAGIC * Assume we have 3 Data frames(each for 3 different days) with data of drivers. 

# COMMAND ----------

driver_df1 = spark.read\
.option("inferSchema",True)\
.parquet("/mnt/formula1datalakestorage1/processed/drivers")\
.filter("driver_id <=10")\
.select("driver_id","name","nationality","dob")
display(driver_df1)

# COMMAND ----------

driver_df2 = spark.read\
.option("inferSchema",True)\
.parquet("/mnt/formula1datalakestorage1/processed/drivers")\
.filter("driver_id between 6 and 15")\
.select("driver_id","name","nationality","dob")
display(driver_df2)

# COMMAND ----------

driver_df3 = spark.read\
.option("inferSchema",True)\
.parquet("/mnt/formula1datalakestorage1/processed/drivers")\
.filter("driver_id between 1 and 5 OR driver_id between 16 and 20")\
.select("driver_id","name","nationality","dob")
display(driver_df3)

# COMMAND ----------

# MAGIC %md
# MAGIC * Now create a temporary view for each of the data frame

# COMMAND ----------

driver_df1.createTempView("driver_view1")

# COMMAND ----------

driver_df2.createTempView("driver_view2")

# COMMAND ----------

driver_df3.createTempView("driver_view3")

# COMMAND ----------

# MAGIC %md
# MAGIC * Creating a delta table to store all the info

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.drivers_merge(
# MAGIC driver_id INT,
# MAGIC name STRING,
# MAGIC nationality STRING,
# MAGIC dob DATE,
# MAGIC inserted_date DATE,
# MAGIC updated_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_view1 upd
# MAGIC ON tgt.driver_id = upd.driver_id
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET tgt.name = upd.name,
# MAGIC              tgt.nationality = upd.nationality,
# MAGIC              tgt.dob = upd.dob,
# MAGIC              tgt.updated_date = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT (driver_id,name,nationality,dob,inserted_date) VALUES (driver_id,name,nationality,dob,CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM F1_DEMO.DRIVERS_MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_view2 upd
# MAGIC ON tgt.driver_id = upd.driver_id
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET tgt.name = upd.name,
# MAGIC              tgt.nationality = upd.nationality,
# MAGIC              tgt.dob = upd.dob,
# MAGIC              tgt.updated_date = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT (driver_id,name,nationality,dob,inserted_date) VALUES (driver_id,name,nationality,dob,CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM F1_DEMO.DRIVERS_MERGE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert and merge using python

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark,"/mnt/formula1datalakestorage1/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    driver_df3.alias("upd"),
    "tgt.driver_id = upd.driver_id")\
.whenMatchedUpdate(set = {"name":"upd.name","nationality":"upd.nationality","dob":"upd.dob","updated_date":"current_timestamp()"})\
.whenNotMatchedInsert(values={
    "driver_id":"upd.driver_id",
    "name":"upd.name",
    "nationality":"upd.nationality",
    "dob":"upd.dob",
    "inserted_date":"current_timestamp()"
}).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM F1_DEMO.DRIVERS_MERGE

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %md
# MAGIC ### History Versioning , Time Travel and Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

display(spark.read.option("timestampAsOf","2022-01-25T05:59:14.000+0000").load("/mnt/formula1datalakestorage1/demo/drivers_merge"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM
# MAGIC * By default the retention is 7 days , we can change that to the number of our choice by setting this parameter as true
# MAGIC   * spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 1 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.drivers_txn(
# MAGIC driver_id INT,
# MAGIC name STRING,
# MAGIC nationality STRING,
# MAGIC dob DATE,
# MAGIC inserted_date DATE,
# MAGIC updated_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC where driver_id <=2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

for i in range(3,21):
    spark.sql(f"""INSERT INTO f1_demo.drivers_txn
SELECT * FROM f1_demo.drivers_merge
where driver_id = {i};""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.drivers_parquet_to_delta(
# MAGIC driver_id INT,
# MAGIC name STRING,
# MAGIC nationality STRING,
# MAGIC dob DATE,
# MAGIC inserted_date DATE,
# MAGIC updated_date DATE
# MAGIC )
# MAGIC USING PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_parquet_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE DRIVER_ID <=5;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_parquet_to_delta 