# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.dropdown("Team","Liverpool",['Liverpool',"Man City","Man United","Chelsea","Spurs"])

# COMMAND ----------

dbutils.widgets.get("Team")

# COMMAND ----------

