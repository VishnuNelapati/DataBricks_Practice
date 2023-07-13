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

# from pyspark.sql.functions import lit,current_timestamp,col,count
# display(spark.read.parquet(f"{processed_folder_path}/results").groupBy("file_date").agg(count(col("time"))))

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results").filter(f"file_date = '{v_file_date}'").withColumnRenamed("time","race_time").withColumnRenamed("file_date","final_file_date")

# COMMAND ----------

results_df.show()

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/race").withColumnRenamed("year","race_year")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits").withColumnRenamed("location","circuit_location")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnRenamed("name","driver_name")\
.withColumnRenamed("nationality","driver_nationality")\
.withColumnRenamed("number","driver_number")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnRenamed("name","team")

# COMMAND ----------

df1 = races_df.join(circuits_df,on="circuit_id",how="inner").select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

df2 = df1.join(results_df,on = "race_id",how="inner").select(df1.race_id,df1.race_year,df1.race_name,df1.race_date,df1.circuit_location,results_df.grid,results_df.fastest_lap,results_df.race_time,results_df.points,results_df.driver_id,results_df.constructor_id,results_df.position,results_df.final_file_date)

# COMMAND ----------

df3 = df2.join(drivers_df,on = "driver_id",how = "inner").select(df2.race_id,df2.race_year,df2.race_name,df2.race_date,df2.circuit_location,df2.grid,df2.fastest_lap,df2.race_time,df2.points,df2.constructor_id,df2.position,drivers_df.driver_name,drivers_df.driver_number,drivers_df.driver_nationality,df2.final_file_date)

# COMMAND ----------

df4 = df3.join(constructors_df,on = "constructor_id",how = "inner")

# COMMAND ----------

df_final = df4.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","final_file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_final = df_final.withColumn("created_date",current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import to_date
df_final = df_final.withColumn("race_date",to_date("race_date"))

# COMMAND ----------

df_final = rearrange_data(df_final,"race_id")
display(df_final)

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# df_final.write.options(mode = "overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# write_data(df_final,data_table="f1_presentation.race_results",partition_col='race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_table(df_final,"f1_presentation.race_results","race_id",presentation_folder_path,merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_presentation.race_results
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC f1_presentation.race_results

# COMMAND ----------

