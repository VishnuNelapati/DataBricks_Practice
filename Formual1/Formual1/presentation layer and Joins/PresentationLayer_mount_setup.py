# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

storageaccount = "formula1datalakestorage1"
container = "presentationlayer"
access_key = "mCNhR0+PLIscDF8R3LtQAj9RCPfOCPFyp+hMH6ujzm9sjzloTt3cnesYw1FNsRwz765KmkGKmsetWViuXjmzhQ=="

# COMMAND ----------

dbutils.fs.mount(
  source = f"wasbs://{container}@{storageaccount}.blob.core.windows.net",
  mount_point = f"/mnt/{storageaccount}/{container}",
  extra_configs = {"fs.azure.account.key.formula1datalakestorage1.blob.core.windows.net":"mCNhR0+PLIscDF8R3LtQAj9RCPfOCPFyp+hMH6ujzm9sjzloTt3cnesYw1FNsRwz765KmkGKmsetWViuXjmzhQ=="})

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1datalakestorage1/presentationlayer

# COMMAND ----------

