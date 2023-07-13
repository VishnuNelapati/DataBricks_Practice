# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC drivers;

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS f1_presentation.calculated_results (

    race_year INT,
    team_name STRING,
    driver_id INT,
    driver_name STRING,
    race_id INT,
    position INT,
    points INT,
    normalized_points INT,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
    ) 
    USING DELTA
""")

# COMMAND ----------

spark.sql(sqlQuery= f"""CREATE OR REPLACE TEMP VIEW race_results_updates
            AS
            SELECT r.race_year,
                   c.name as team_name, 
                   res.driver_id as driver_id,
                   d.name as driver_name,
                   r.race_id,
                   res.position,
                   res.points,
                   CASE 
                       when 11-res.position <0 then 0
                       when res.position is NULL then 0
                       else 11 - res.position 
                   END
                   as normalized_points
            FROM f1_processed.drivers d 
            INNER JOIN f1_processed.results res ON res.driver_id = d.driver_id 
            INNER JOIN f1_processed.race r ON res.race_id = r.race_id
            INNER JOIN f1_processed.constructors c ON res.constructor_id = c.constructor_id
            WHERE res.file_date = '{v_file_date}'
""")

# COMMAND ----------

# %sql
# CREATE TABLE f1_presentation.calculated_results
# USING parquet 
# AS 
# SELECT r.race_year,
#        c.name as constructor_name , 
#        d.name as driver_name,
#        res.position,
#        res.points,
#        r.name as race_name,
#        CASE 
#            when 11-res.position <0 then 0
#            when res.position is NULL then 0
#            else 11 - res.position 
#        END
#        as normalized_points
# FROM f1_processed.drivers d 
# INNER JOIN f1_processed.results res ON res.driver_id = d.driver_id 
# INNER JOIN f1_processed.race r ON res.race_id = r.race_id
# INNER JOIN f1_processed.constructors c ON res.constructor_id = c.constructor_id 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_results tgt
# MAGIC   USING race_results_updates src
# MAGIC   ON (tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id)
# MAGIC   WHEN MATCHED THEN 
# MAGIC       UPDATE SET tgt.points = src.points,
# MAGIC                  tgt.position = src.position,
# MAGIC                  tgt.normalized_points = src.normalized_points,
# MAGIC                  tgt.updated_date = current_timestamp
# MAGIC   WHEN NOT MATCHED 
# MAGIC       THEN INSERT (race_year,team_name,driver_id,driver_name,race_id,position,points,normalized_points,created_date) VALUES (race_year,team_name,driver_id,driver_name,race_id,position,points,normalized_points,CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from  race_results_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from f1_presentation.calculated_results;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dominat Driver

# COMMAND ----------

# MAGIC %sql
# MAGIC Select driver_name,avg(normalized_points) as avg_points,sum(normalized_points) as total_points,count(1) as no_of_races from 
# MAGIC f1_presentation.calculated_results
# MAGIC group by driver_name
# MAGIC HAVING no_of_races >= 50
# MAGIC order by avg_points DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select driver_name,avg(normalized_points) as avg_points,sum(normalized_points) as total_points,count(1) as no_of_races from 
# MAGIC f1_presentation.calculated_results
# MAGIC where race_year between 2000 and 2010
# MAGIC group by driver_name
# MAGIC HAVING no_of_races >= 50
# MAGIC order by avg_points DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dominant Teams

# COMMAND ----------

# MAGIC %sql
# MAGIC Select team_name,avg(normalized_points) as avg_points,sum(normalized_points) as total_points,count(1) as no_of_races from 
# MAGIC f1_presentation.calculated_results
# MAGIC group by team_name
# MAGIC order by avg_points DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select team_name,avg(normalized_points) as avg_points,sum(normalized_points) as total_points,count(1) as no_of_races from 
# MAGIC f1_presentation.calculated_results
# MAGIC where race_year between 2000 and 2021
# MAGIC group by team_name
# MAGIC HAVING no_of_races >= 50
# MAGIC order by avg_points DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizing Dominant drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC Select driver_name,avg(normalized_points) as avg_points,sum(normalized_points) as total_points,count(1) as no_of_races from 
# MAGIC f1_presentation.calculated_results
# MAGIC group by driver_name
# MAGIC HAVING no_of_races >= 50
# MAGIC order by avg_points DESC;

# COMMAND ----------

