-- Databricks notebook source
-- By using this we are creating the data base in our storage account rather than in the default hive
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1datalakestorage1/processed"

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_processed;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_raw;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /mnt/formula1datalakestorage1/processed

-- COMMAND ----------

