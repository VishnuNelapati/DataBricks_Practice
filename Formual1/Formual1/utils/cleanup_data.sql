-- Databricks notebook source
DROP DATABASE f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1datalakestorage1/processed"

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /mnt/formula1datalakestorage1/processed

-- COMMAND ----------

SHOW TABLES IN f1_processed;

-- COMMAND ----------

-- DESCRIBE TABLE EXTENDED f1_processed.circuits;

-- COMMAND ----------

DROP DATABASE f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1datalakestorage1/presentationlayer"

-- COMMAND ----------

