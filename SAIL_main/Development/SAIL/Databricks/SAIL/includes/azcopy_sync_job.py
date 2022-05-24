# Databricks notebook source
time_zone = 'UTC' # Check for which timezone to be used

# COMMAND ----------

# DBTITLE 1,Installing azcopy package
# MAGIC %sh
# MAGIC # install azure cli cmds
# MAGIC curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
# MAGIC 
# MAGIC # install azcopy cmds
# MAGIC wget https://aka.ms/downloadazcopy-v10-linux
# MAGIC tar -xvf downloadazcopy-v10-linux
# MAGIC sudo rm /usr/bin/azcopy
# MAGIC sudo cp ./azcopy_linux_amd64_*/azcopy /usr/bin/

# COMMAND ----------

# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------


import os

# COMMAND ----------

# DBTITLE 1,logger function
dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
    logger = _get_logger(time_zone,logging.DEBUG)
else:
    logger = _get_logger(time_zone,logging.INFO)

# COMMAND ----------

# DBTITLE 1,Reading parameters from widgets

dbutils.widgets.text("REGION", "","")
REGION = getArgument("REGION").strip()
logger.debug("REGION: " + REGION)

dbutils.widgets.text("PROJECT", "","")
PROJECT = getArgument("PROJECT").strip()
logger.debug("PROJECT: " + PROJECT)

dbutils.widgets.text("DES_STR_ACCT", "","")
DES_STR_ACCT = getArgument("DES_STR_ACCT").strip()
logger.debug("DES_STR_ACCT: " + DES_STR_ACCT)

dbutils.widgets.text("SRC_STR_ACCT", "","")
SRC_STR_ACCT = getArgument("SRC_STR_ACCT").strip()
logger.debug("SRC_STR_ACCT: " + SRC_STR_ACCT)


dbutils.widgets.text("SRC_CONT", "","")
SRC_CONT = getArgument("SRC_CONT").strip()
logger.debug("SRC_CONT: " + SRC_CONT)

dbutils.widgets.text("DES_CONT", "","")
DES_CONT = getArgument("DES_CONT").strip()
logger.debug("DES_CONT: " + DES_CONT)

dbutils.widgets.text("SRC_PATH", "","")
SRC_PATH = getArgument("SRC_PATH").strip()
logger.debug("SRC_PATH: " + SRC_PATH)

dbutils.widgets.text("DES_PATH", "","")
DES_PATH = getArgument("DES_PATH").strip()
logger.debug("DES_PATH: " + DES_PATH)

#scope = f'akv-{PROJECT}-{REGION}-{ENV}'
#print(scope)


# COMMAND ----------

# DBTITLE 1,Fetching secrets from Azure Key Vault
scope = 'key-vault-secrets'
CLIENT_ID =  dbutils.secrets.get(scope,"sp-client-id")
CLIENT_SECRET =  dbutils.secrets.get(scope,"sp-client-secret")
TENANT_ID =  dbutils.secrets.get(scope,"directory-id")
SRC_ACCT_KEY= dbutils.secrets.get(scope,f"{SRC_STR_ACCT}-acct-key") 
DES_ACCT_KEY= dbutils.secrets.get(scope,f"{DES_STR_ACCT}-acct-key") 

# COMMAND ----------


LOG_FILE_NAME="azCopy_"+datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S%f")+".log"


# COMMAND ----------

# DBTITLE 1,Setting environment variables
os.environ['sh_param_region']=REGION
os.environ['sh_param_project']=PROJECT
os.environ['sh_param_client_id']=CLIENT_ID
os.environ['sh_param_client_secret']=CLIENT_SECRET
os.environ['sh_param_tenant_id']=TENANT_ID
os.environ['sh_param_des_cont']=DES_CONT
os.environ['sh_param_src_cont']=SRC_CONT
os.environ['sh_param_des_path']=DES_PATH
os.environ['sh_param_src_path']=SRC_PATH
os.environ['sh_param_des_str_acct']=DES_STR_ACCT
os.environ['sh_param_src_str_acct']=SRC_STR_ACCT
os.environ['sh_param_des_acct_key']=DES_ACCT_KEY
os.environ['sh_param_src_acct_key']=SRC_ACCT_KEY
os.environ['sh_param_log_file_name']=LOG_FILE_NAME


# COMMAND ----------

# MAGIC %sh
# MAGIC echo " !!! azcopy sync process started  !!!"
# MAGIC  
# MAGIC REGION=$sh_param_region
# MAGIC PROJECT=$sh_param_project
# MAGIC CLIENT_ID=$sh_param_client_id
# MAGIC CLIENT_SECRET=$sh_param_client_secret
# MAGIC TENANT_ID=$sh_param_tenant_id
# MAGIC SRC_STR_ACCT=$sh_param_src_str_acct
# MAGIC SRC_ACCT_KEY=$sh_param_src_acct_key
# MAGIC DES_STR_ACCT=$sh_param_des_str_acct
# MAGIC DES_ACCT_KEY=$sh_param_des_acct_key
# MAGIC SRC_CONT=$sh_param_src_cont
# MAGIC DES_CONT=$sh_param_des_cont
# MAGIC DES_PATH=$sh_param_des_path
# MAGIC SRC_PATH=$sh_param_src_path
# MAGIC LOG_FILE_NAME=$sh_param_log_file_name
# MAGIC LOG_PATH="/dbfs/mnt/sail/logs/az_sync_logs"
# MAGIC 
# MAGIC if [ ! -d "${LOG_PATH}" ]; then
# MAGIC   echo "creating directory $LOG_PATH"
# MAGIC   mkdir -p $LOG_PATH
# MAGIC fi
# MAGIC 
# MAGIC az login --service-principal -u ${CLIENT_ID} -p ${CLIENT_SECRET} --tenant ${TENANT_ID}
# MAGIC echo ""################################################################""
# MAGIC LOG_TIME=`date -u '+%Y-%m-%dT%H:%M:%SZ'`
# MAGIC echo "$LOG_TIME -||- az login successfull"
# MAGIC 
# MAGIC START=`date -u '+%Y-%m-%dT%H:%M:%SZ'`
# MAGIC END=`date -u -d "2 hours" '+%Y-%m-%dT%H:%M:%SZ'`
# MAGIC 
# MAGIC SRC_SAS=`az storage container generate-sas --account-key ${SRC_ACCT_KEY} --account-name ${SRC_STR_ACCT} --expiry $END --start $START -n ${SRC_CONT} --permissions acdlrw -o tsv | sed "s/%3A/:/g"`
# MAGIC DES_SAS=`az storage container generate-sas --account-key ${DES_ACCT_KEY} --account-name ${DES_STR_ACCT} --expiry $END --start $START -n ${DES_CONT} --permissions acdlrw -o tsv | sed "s/%3A/:/g"`
# MAGIC 
# MAGIC SRC_URI="https://${SRC_STR_ACCT}.blob.core.windows.net/${SRC_PATH}?${SRC_SAS}"
# MAGIC DES_URI="https://${DES_STR_ACCT}.blob.core.windows.net/${DES_PATH}?${DES_SAS}"
# MAGIC 
# MAGIC echo ""################################################################""
# MAGIC LOG_TIME=`date -u '+%Y-%m-%dT%H:%M:%SZ'`
# MAGIC echo "$LOG_TIME -||- Executing Azcopy sync command"
# MAGIC echo azcopy sync \""${SRC_URI}"\" \""${DES_URI}"\" --recursive
# MAGIC 
# MAGIC azcopy sync "${SRC_URI}" "${DES_URI}" --recursive > /dbfs/mnt/sail/logs/az_sync_logs/$LOG_FILE_NAME
# MAGIC 
# MAGIC echo ""################################################################""
# MAGIC LOG_TIME=`date -u '+%Y-%m-%dT%H:%M:%SZ'`
# MAGIC echo "$LOG_TIME -||- azcopy sync finished succesfully "

# COMMAND ----------

# MAGIC %sh 
# MAGIC cat /dbfs/mnt/sail/logs/az_sync_logs/$sh_param_log_file_name

# COMMAND ----------

audit_dict=dict(map(lambda line : line.replace("\n","").split(":"),
           filter(lambda line : (':' in line ) and ("INFO:" not in line) and ("Log file is located at:" not in line) ,
                  open(f"/dbfs/mnt/sail/logs/az_sync_logs/{LOG_FILE_NAME}")
                 )
          )
      )
audit_dict

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -ltr  /dbfs/mnt/sail/logs/az_sync_logs/

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/mnt/sail/logs/az_sync_logs/azCopy_20220316145354625832.log