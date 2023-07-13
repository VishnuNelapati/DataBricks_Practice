-- Databricks notebook source
-- MAGIC %md
-- MAGIC * Create DataBase DEMO

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS DEMO COMMENT "CREATED FOR DEMO PURPOSE"

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

USE default;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC TABLE circuits_csv;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED DEMO;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managed Tables
-- MAGIC * CREATE MANAGED TABLE IN PYTHON
-- MAGIC * CREATE MANAGED TABLE IN SQL
-- MAGIC * EFFECT OF DROPPING A MANAGED TABLE
-- MAGIC   * When a managed table is dropped both metadata and data is deleted.But when external table is dropped only meta data is dropped.
-- MAGIC * DESCRIBE TABLE

-- COMMAND ----------

-- MAGIC %run "../DB Workflows/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Step 1 - Create managed tables in Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC display(race_results_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use database demo;
DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

select * 
from 
demo.race_results_python;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
select * 
from 
demo.race_results_python;

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External Tables
-- MAGIC * CREATE External TABLE IN PYTHON
-- MAGIC * CREATE External TABLE IN SQL
-- MAGIC * EFFECT OF DROPPING A External TABLE
-- MAGIC   * When a external table is dropped only metadata is deleted.
-- MAGIC * DESCRIBE TABLE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESCRIBE TABLE EXTENDED demo.race_results_ext_py;

-- COMMAND ----------

USE DEMO;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.printSchema()

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql1
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
) 
USING parquet
LOCATION "/mnt/formula1datalakestorage1/presentationlayer/race_results_ext_sql1";

-- COMMAND ----------

DESCRIBE TABLE EXTENDED demo.race_results_ext_sql1;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/mnt/formula1datalakestorage1/presentationlayer/race_results_ext_sql1

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql1
SELECT * from 
demo.race_results_ext_py;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Views on tables 
-- MAGIC   * Temp View 
-- MAGIC   * Global Temp View
-- MAGIC   * Permanent View

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results 
AS 
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

Select * from global_temp.gv_race_results


-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS 
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

Show tables in demo;