# Databricks notebook source
# MAGIC %run "../DB Workflows/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC * Creating SQL views for Dataframes

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year == 2018

# COMMAND ----------

rr_df = spark.sql("Select * from v_race_results")
display(rr_df)

# COMMAND ----------

dbutils.widgets.dropdown("Race_Year","2020",["2020","2019","2018","2017"])

# COMMAND ----------

year = dbutils.widgets.get("Race_Year")
print(year)

# COMMAND ----------

rr_df = spark.sql(f"Select * from v_race_results where race_year = {year}")
display(rr_df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("v_g_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.v_g_race_results;

# COMMAND ----------

