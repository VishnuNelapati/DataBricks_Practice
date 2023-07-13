-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

SHOW TABLES IN f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Circuits

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.csv("/mnt/formula1datalakestorage1/raw/circuits.csv",header= True).show(10)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits (
circuitId INT,
circuitsRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
location "/mnt/formula1datalakestorage1/raw/circuits.csv"
OPTIONS (header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Races

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.csv("/mnt/formula1datalakestorage1/raw/races.csv",header= True).show(10)

-- COMMAND ----------

CREATE TABLE f1_raw.races(raceId INT,
year INT,
round INT,
circuitID INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
location "/mnt/formula1datalakestorage1/raw/races.csv"
options (header true);

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Construuctors

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC spark.read.json("/mnt/formula1datalakestorage1/raw/constructors.json").show(10)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
using json
location "/mnt/formula1datalakestorage1/raw/constructors.json"

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drivers - JSON

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.json("/mnt/formula1datalakestorage1/raw/drivers.json").show(10)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(code STRING,
dob DATE,
driverId INT,
name STRUCT<forename: STRING,surname: STRING>,
nationality STRING,
number INT,
url STRING
)
USING json
location "/mnt/formula1datalakestorage1/raw/drivers.json"
options (header true);

-- COMMAND ----------

DESCRIBE TABLE EXTENDED f1_raw.drivers;

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.json("/mnt/formula1datalakestorage1/raw/results.json").show(5)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
constructorId INT,
driverId INT,
fastestLap INT,
fastestLapSpeed DOUBLE,
fastestLapTime STRING,
grid INT,
laps INT,
milliseconds INT,
number INT,
points DOUBLE,
position INT,
positionOrder INT,
positionText STRING,
raceId INT,
rank INT,
resultId INT,
statusId INT,
time STRING)
using JSON
location "/mnt/formula1datalakestorage1/raw/results.json"

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### pitstops

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.options(multiLine = True).json("/mnt/formula1datalakestorage1/raw/pit_stops.json").show(5)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
using json
location "/mnt/formula1datalakestorage1/raw/pit_stops.json"
options (multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Tables from multiple files
-- MAGIC * LAP TIMES CSV

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.csv("/mnt/formula1datalakestorage1/raw/lap_times").show(5)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT)
USING csv
options (path "/mnt/formula1datalakestorage1/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Tables from multiple files
-- MAGIC * Qualifying JSON

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.options(multiLine = True).json("/mnt/formula1datalakestorage1/raw/qualifying").show(5)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT
)
using json
options (path "/mnt/formula1datalakestorage1/raw/qualifying/",multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

SHOW TABLES in f1_raw;