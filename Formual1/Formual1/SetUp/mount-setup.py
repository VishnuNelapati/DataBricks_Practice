# Databricks notebook source
# MAGIC %md
# MAGIC ### Mounting Using Manual insertion of keys 
# MAGIC 
# MAGIC - For mounting we require few details of the storage account.
# MAGIC   * Stotage account name 
# MAGIC   * Container (container exist inside storage account)
# MAGIC   * Access Key
# MAGIC   
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC   

# COMMAND ----------

# dbutils.fs.mount(
# source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
# mount_point = "/mnt/<mount-name>",
# extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

storageaccount = 'formula1datalakestorage1'
container = 'raw'
accesskey = 'mCNhR0+PLIscDF8R3LtQAj9RCPfOCPFyp+hMH6ujzm9sjzloTt3cnesYw1FNsRwz765KmkGKmsetWViuXjmzhQ=='

# COMMAND ----------

dbutils.fs.mount(
  source = f"wasbs://{container}@{storageaccount}.blob.core.windows.net",
  mount_point = f"/mnt/{storageaccount}/{container}",
  extra_configs = {"fs.azure.account.key.formula1datalakestorage1.blob.core.windows.net":"mCNhR0+PLIscDF8R3LtQAj9RCPfOCPFyp+hMH6ujzm9sjzloTt3cnesYw1FNsRwz765KmkGKmsetWViuXjmzhQ=="})

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1datalakestorage1/raw")

# COMMAND ----------

def createmount(container):
    dbutils.fs.mount(
      source = f"wasbs://{container}@{storageaccount}.blob.core.windows.net",
      mount_point = f"/mnt/{storageaccount}/{container}",
      extra_configs = {"fs.azure.account.key.formula1datalakestorage1.blob.core.windows.net":"mCNhR0+PLIscDF8R3LtQAj9RCPfOCPFyp+hMH6ujzm9sjzloTt3cnesYw1FNsRwz765KmkGKmsetWViuXjmzhQ=="})
    

# COMMAND ----------

createmount('processed')

# COMMAND ----------

createmount('demo')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting Using Secret Vaults
# MAGIC 
# MAGIC * Formual1Scope is sceret scope we have created in data bricks, which accesses the azure key vault to get the access keys.

# COMMAND ----------

dbutils.secrets.list('Formual1Scope')

# COMMAND ----------

dbutils.secrets.get('Formual1Scope','databricks-mount-accesskey')

# COMMAND ----------

# MAGIC %fs 
# MAGIC head dbfs:/mnt/formula1datalakestorage1/raw/circuits.csv

# COMMAND ----------

