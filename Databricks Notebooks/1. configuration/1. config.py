# Databricks notebook source
client_secret = dbutils.secrets.get(scope="secret-scope-covid",key="client-secret")
storage_account='covid19deprojectdl'
client_id=dbutils.secrets.get(scope="secret-scope-covid",key="client-id")
tenant_id=dbutils.secrets.get(scope="secret-scope-covid",key="tenant-id")


configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", 
        "fs.azure.account.oauth2.client.id": f"{client_id}",
        "fs.azure.account.oauth2.client.secret": f"{client_secret}",
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source= f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
        mount_point=f"/mnt/{storage_account}/{container_name}",
        extra_configs=configs
        
        )

# COMMAND ----------

mount_adls('lookup')
mount_adls('raw')
mount_adls('processed')


# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

