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

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import rank,sum_distinct,sum,count_distinct,count,when,col

constructors_df = race_results_df\
        .groupBy("race_year","team")\
        .agg(sum("points").alias("total_points"),
            count(when(col("position") == 1,True)).alias("wins"))

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,asc

constructors_window = Window.partitionBy("race_year").orderBy(desc(col("total_points")),desc(col("wins")))
final_df = constructors_df.withColumn("rank",rank().over(constructors_window))
display(final_df)

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

final_df = rearrange_data(final_df,"race_year")
display(final_df)

# COMMAND ----------

# final_df.write.options(mode = "overwrite").format("parquet").saveAsTable("f1_presentation.constructors_standings")

# COMMAND ----------

# write_data(final_df,data_table="f1_presentation.constructors_standings",partition_col='race_year')

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_table(final_df,"f1_presentation.constructors_standings","race_year",presentation_folder_path,merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team,race_year,sum(wins) as total_wins FROM f1_presentation.constructors_standings
# MAGIC group by team,race_year
# MAGIC order by race_year DESC;

# COMMAND ----------

