-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM DRIVERS;

-- COMMAND ----------

DESC DRIVERS;

-- COMMAND ----------

SELECT name,nationality from drivers 
where dob >= '1996-08-19'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL FUNCTIONS

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SELECT * from drivers;

-- COMMAND ----------

SELECT * , concat(driver_ref," ",code) as drivercode
from drivers;

-- COMMAND ----------

SELECT * ,split(name," ") [0],split(name," ")[1]
from 
drivers;

-- COMMAND ----------

SELECT * , current_timestamp()
from drivers;

-- COMMAND ----------

SELECT *, date_format(dob,'dd-mm-yyyy')
from drivers;

-- COMMAND ----------

SELECT *,date_add(dob, 365)
from drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Aggregate functions

-- COMMAND ----------

SELECT nationality,COUNT(name) 
from drivers
group by nationality;

-- COMMAND ----------

SELECT nationality,name,dob,RANK() OVER(PARTITION BY nationality ORDER BY dob DESC)
FROM DRIVERS
ORDER BY dob;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Check window function 

-- COMMAND ----------

