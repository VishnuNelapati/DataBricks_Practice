# Databricks notebook source
# MAGIC %run "../DB Workflows/configuration"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(results_df)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### JOINS EXAMPLE

# COMMAND ----------

joined_df = results_df.join(drivers_df,on=["driver_id"],how="inner")

# COMMAND ----------

display(joined_df)

# COMMAND ----------

display(joined_df.filter(joined_df.code == "HAM"))

# COMMAND ----------

# To view selected columns 
display(joined_df.filter(joined_df.code == "HAM").select(drivers_df.code,drivers_df.name,results_df.points,results_df.position))

# COMMAND ----------

# MAGIC %md
# MAGIC * Left Join , Right Join
# MAGIC * Semi Join / Left Semi Join ()
# MAGIC * Anti Join / Left Anti Join (opposite of semi join)
# MAGIC * Cross Join 

# COMMAND ----------

