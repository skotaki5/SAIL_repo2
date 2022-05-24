# Databricks notebook source
evnType=dbutils.secrets.get(scope=scopeName,key=f"env-type")
scopeName=f"akv-sail-eastus2-{envType}"
directoryId=dbutils.secrets.get(scope=scopeName,key=f"directory-id")
# change as per new naming convention
storageAaccountName=f"sasaileastus2{envType}01"
spClientId=dbutils.secrets.get(scope=scopeName,key=f"sp-client-id")
spClientSecret=dbutils.secrets.get(scope=scopeName,key=f"sp-client-secret")
endPoint=f"https://login.microsoftonline.com/{directoryId}/oauth2/token"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": spClientId,
          "fs.azure.account.oauth2.client.secret": spClientSecret,
          "fs.azure.account.oauth2.client.endpoint": endPoint}

# COMMAND ----------

blob_containers = ["metadata", "logs", "bronze", "silver", "gold"]
try:
    for container in blob_containers:
        result = dbutils.fs.mount(
        source = f"abfss://{container}@{storageAaccountName}.dfs.core.windows.net/",
        mount_point = f"/mnt/sail/{container}",
        extra_configs = configs)
    
    if result:
        print("!! mount point:/mnt/sail/{0} is created ".format(container)) 
except:
    raise