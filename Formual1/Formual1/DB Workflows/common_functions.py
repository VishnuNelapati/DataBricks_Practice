# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(df):
    output_df = df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_data(df,partition_col):
    columns = df.schema.names.copy()
    columns.remove(partition_col)
    columns.append(partition_col)
    return df[columns]

# COMMAND ----------

def write_data(df,data_table,partition_col):
    if spark._jsparkSession.catalog().tableExists(data_table):
        df.write.mode("overwrite").insertInto(data_table)
        print("Overwriting existing partitions")
    else:
        df.write.mode("append").partitionBy(partition_col).format("parquet").saveAsTable(data_table)
        print("appending data")

# COMMAND ----------

def merge_delta_table(input_df,data_table,partition_col,folder_path,merge_condition):
    # Incremental load - 2

    from delta.tables import DeltaTable

    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    t = data_table.split(".")[1]
    if spark._jsparkSession.catalog().tableExists(data_table):
        deltaTable = DeltaTable.forPath(spark,f"{folder_path}/{t}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition)\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll().execute()
    else:
        input_df.write.mode("append").partitionBy(partition_col).format("delta").saveAsTable(data_table)

# COMMAND ----------

