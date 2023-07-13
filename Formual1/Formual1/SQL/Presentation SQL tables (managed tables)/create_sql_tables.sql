-- Databricks notebook source
DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1datalakestorage1/presentationlayer"

-- COMMAND ----------

SHOW TABLES IN f1_presentation; 

-- COMMAND ----------

