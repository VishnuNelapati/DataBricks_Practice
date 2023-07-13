# Databricks notebook source
# MAGIC %run "../DB Workflows/configuration"

# COMMAND ----------

# MAGIC %run "../DB Workflows/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

from pyspark.sql.functions import col,lit,current_timestamp

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"final_file_date ='{v_file_date}'")\
.select("race_year")\
.distinct()\
.collect()

# COMMAND ----------

race_year_list = []
for i in race_results_list:
    race_year_list.append(i.race_year)
    
print(race_year_list)

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum_distinct,sum,count_distinct,count,when,col
driver_standing_df = race_results_df\
.groupBy("race_year","driver_name","driver_nationality")\
.agg(sum("points").alias("total_points"),
    count(when(col("position") == 1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,asc,rank

drivers_window = Window.partitionBy("race_year").orderBy(desc(col("total_points")),desc(col("wins")))
driver_standing_df = driver_standing_df.withColumn("rank",rank().over(drivers_window))

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

driver_standing_df = rearrange_data(driver_standing_df,"race_year")
display(driver_standing_df)

# COMMAND ----------

# driver_standing_df.write.options(mode = "overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

# write_data(driver_standing_df,data_table="f1_presentation.driver_standings",partition_col='race_year')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_table(driver_standing_df,"f1_presentation.driver_standings","race_year",presentation_folder_path,merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings
# MAGIC order by race_year desc;

# COMMAND ----------

