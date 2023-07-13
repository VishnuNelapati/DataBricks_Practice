# Databricks notebook source
# MAGIC %run "../DB Workflows/configuration"

# COMMAND ----------

presentation_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregate Functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("race_year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count,count_distinct,sum_distinct,sum

# COMMAND ----------

demo_df.count()

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(count_distinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

display(demo_df.filter(demo_df.driver_name == "Max Verstappen").select(sum("points")).withColumnRenamed("sum(points)","Total Points"))

# COMMAND ----------

# MAGIC %md
# MAGIC * Group BY

# COMMAND ----------

demo_df.groupBy("driver_name").sum("points").withColumnRenamed("sum(points)","Total Points").show()

# COMMAND ----------

demo_df\
.groupBy("driver_name")\
.agg(sum("points"),count_distinct("race_name"))\
.withColumnRenamed("sum(points)","Total Points")\
.withColumnRenamed("count(race_name)","Number_of_Races").show()

# COMMAND ----------

from pyspark.sql.functions import desc
demo_df.groupBy("race_name","driver_nationality").sum("points").withColumnRenamed("sum(points)","Total_points").orderBy(desc("Total_points")).show()

# COMMAND ----------

from pyspark.sql.functions import desc,asc
demo_df.groupBy("race_name","driver_nationality").agg(sum("points"),count("driver_name")).orderBy(desc("sum(points)"),desc("count(driver_name)")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

agg_df = demo_df.groupBy("race_year","driver_name").agg(sum("points").alias("Total_points"),count_distinct("race_name").alias("Total_races"))
agg_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

driver_window = Window.partitionBy("race_year").orderBy(desc("Total_points"))
display(agg_df.withColumn("rank",rank().over(driver_window)))

# COMMAND ----------



# COMMAND ----------

