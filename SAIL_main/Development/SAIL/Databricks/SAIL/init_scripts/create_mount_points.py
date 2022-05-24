# Databricks notebook source
storage_account_name='sasaildeveastus2'
sas='?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx&se=2030-03-02T15:32:05Z&st=2022-03-02T07:32:05Z&spr=https&sig=SZS%2FnIy0%2BNK2y%2B8%2B0xUscKokdBy%2BbiaY97Ersy7RK%2Bo%3D'

# COMMAND ----------

def mount_adls(container_name):
  
    result = dbutils.fs.mount(
          source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name,storage_account_name),
          mount_point = "/mnt/sail/{0}".format(container_name),
          extra_configs = {"fs.azure.sas.{0}.{1}.blob.core.windows.net".format(container_name,storage_account_name):sas}
                        )
    if result:
      print("!! mount point:/mnt/sail/{0} is created ".format(container_name))

# COMMAND ----------

blob_containers = ["metadata", "logs", "bronze", "silver", "gold"]
try:
  for container in blob_containers:
    mount_adls(container_name=container)
except:
  raise

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/sail"