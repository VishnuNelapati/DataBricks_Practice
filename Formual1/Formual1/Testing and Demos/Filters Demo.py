# Databricks notebook source
# MAGIC %run "../DB Workflows/configuration"

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1datalakestorage1/processed/

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/race/")

# COMMAND ----------

display(race_df)

# COMMAND ----------

display(race_df.filter("race_year == 2021"))

# COMMAND ----------

display(race_df.filter(race_df.name == "Brazilian Grand Prix"))

# COMMAND ----------

display(race_df.filter(race_df["name"] == "Brazilian Grand Prix"))

# COMMAND ----------

display(race_df.filter((race_df["race_year"] == 2019) & (race_df["round"] <= 5)))

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### FILL NA in the Data Frame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
display(race_df.filter(race_df.name == "Brazilian Grand Prix").fillna(current_timestamp(),subset=['race_timestamp']))

# COMMAND ----------

display(race_df.filter(race_df.name == "Brazilian Grand Prix").na.fill("000"))

# COMMAND ----------

