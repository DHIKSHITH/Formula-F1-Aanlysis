# Databricks notebook source
storage_account_name ="formula107dl"
client_id=dbutils.secrets.get(scope="formula2-scope",key="databricks-clientid")
tenant_id=dbutils.secrets.get(scope="formula2-scope",key="databricks-tenantid")
client_secret=dbutils.secrets.get(scope="formula2-scope",key="databricks-clientsecret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": f"{client_id}",
"fs.azure.account.oauth2.client.secret": f"{client_secret}",
"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula107dl/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula107dl/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula107dl/presentation")

# COMMAND ----------

