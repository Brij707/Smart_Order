# Databricks notebook source
# MAGIC %md
# MAGIC ## Changelog
# MAGIC * 10/03/2020 - cmd2: "data_path" variable was substituted by "environment" variable in order to make the code more agile;
# MAGIC * 13/04/2020 - cmd7, 8 and 18: New Library and commands added due to package "Azure" shut down (US: 8103).

# COMMAND ----------

# MAGIC %md
# MAGIC %sh
# MAGIC 
# MAGIC #!/bin/bash
# MAGIC pip install --upgrade pip
# MAGIC echo "Removing pyOpenSSL package"
# MAGIC rm -rf /databricks/python2/lib/python2.7/site-packages/OpenSSL
# MAGIC rm -rf /databricks/python2/lib/python2.7/site-packages/pyOpenSSL-16.0.0-*.egg-info
# MAGIC rm -rf /databricks/python2/lib/python2.7/site-packages/cryptography
# MAGIC rm -rf /databricks/python2/lib/python2.7/site-packages/cryptography-1.9*.egg-info
# MAGIC rm -rf /databricks/python3/lib/python3.5/site-packages/OpenSSL
# MAGIC rm -rf /databricks/python3/lib/python3.5/site-packages/pyOpenSSL-16.0.0*.egg-info
# MAGIC rm -rf /databricks/python3/lib/python3.5/site-packages/cryptography
# MAGIC rm -rf /databricks/python3/lib/python3.5/site-packages/cryptography-1.9*.egg-info
# MAGIC /databricks/python3/bin/pip3 install cryptography==2.3
# MAGIC /databricks/python2/bin/pip install cryptography==2.3
# MAGIC /databricks/python2/bin/pip install pyOpenSSL==19.0.0
# MAGIC /databricks/python3/bin/pip3 install pyOpenSSL==19.0.0

# COMMAND ----------

# DBTITLE 1,Define variables and paths
try:
  country_loop = getArgument('VAR_COUNTRY')
  environment = getArgument('VAR_ENVIRONMENT')
  
except:
  country_loop = 'INDIA_HEALTH' ## INDIA_HOME, PAKISTAN_HOME, INDO_HOME
  environment = 'dev' ## prod, dev ## Creation of the variable "Environment" 09/03/2020 - MS


## Business and Data path definition
if environment == 'dev':
  if country_loop == 'INDIA_HEALTH':
    business_path = 'wasbs://sobusinessdev@devrbainesasmartorderhc.blob.core.windows.net/SmartOrder_Files' ## Testing for New DEV Env business files path
    data_path = 'wasbs://smartorder@devrbainesasmartorderhc.blob.core.windows.net/dev' ## New DEV Env
  else:    
    business_path = 'wasbs://sobusinessdev@devrbainesasmartorder.blob.core.windows.net/SmartOrder_Files' ## Testing for New DEV Env business files path
    data_path = 'wasbs://smartorder@devrbainesasmartorder.blob.core.windows.net/dev' ## New DEV Env
elif environment == 'prod':
  if country_loop == 'INDIA_HEALTH':
    data_path = 'wasbs://smartorder@prdrbainesasmartorderhc.blob.core.windows.net/prod'
    business_path = 'wasbs://sobusinessinput@prdrbainesasmartorderhc.blob.core.windows.net/SmartOrder_Files'
  else:
    business_path = 'wasbs://sobusinessinput@prdrbainesasmartorder.blob.core.windows.net/SmartOrder_Files' ## New Prod Env business files path
    data_path = 'wasbs://smartorder@prdrbainesasmartorder.blob.core.windows.net/prod' ## New Prod Env
else:
  raise ValueError('Undefined Environment! Please select "dev" or "prod" as the environment.')


bis_config_path = business_path + '/' + country_loop + '/INPUTS/CONFIG/'
bis_increment_path = business_path + '/' + country_loop + '/INPUTS/INCREMENTAL/'
bis_control_f_path = business_path + '/' + country_loop + '/Control_File/'

inter_path = data_path + '/' + country_loop + '/INTERMEDIATE/'
config_path = data_path + '/' + country_loop + '/INPUTS/CONFIG/'
increment_path = data_path + '/' + country_loop + '/INPUTS/INCREMENTAL/'
output_path = data_path + '/' + country_loop + '/OUTPUTS'
control_f_path = data_path + '/' + country_loop + '/Control_File/'

## json file location:
json_file = '/dbfs/mnt/Config_Files_Validation/file_config_val.json'

if environment == 'dev':
  ## DEV Files writing path: (18/12/2019) MS, JV
  container_name = "smartorder"
  w_incremental_path = 'dev/' + country_loop + '/INPUTS/INCREMENTAL/'
  w_config_path = 'dev/' + country_loop + '/INPUTS/CONFIG/'
  w_control_f_path = 'dev/' + country_loop + '/Control_File/'

else:
  ## PROD Files writing path: (18/12/2019) MS, JV
  container_name = "smartorder"
  w_incremental_path = 'prod/' + country_loop + '/INPUTS/INCREMENTAL/'
  w_config_path = 'prod/' + country_loop + '/INPUTS/CONFIG/'
  w_control_f_path = 'prod/' + country_loop + '/Control_File/'
print(bis_config_path)
print(w_incremental_path)

# COMMAND ----------

# DBTITLE 1,AI Model Automation DEV or PROD blob storage access
if environment == 'dev':
  ## DEV Storage Account
  if country_loop == 'INDIA_HEALTH':
    storage_account_name = "devrbainesasmartorderhc"
    spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'devrbainekvsmartorderhc', key = 'Blob-devrbainesasmartorderhc-key'))
  else: 
    storage_account_name = "devrbainesasmartorder"
    spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'devrbainekvsmartorder', key = 'Blob-devrbainesasmartorder-key'))

else:
  if country_loop == 'INDIA_HEALTH':
    ## PROD Storage Account
    storage_account_name = "prdrbainesasmartorderhc"
    spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorderhc', key = 'storageaccountsohcpulse'))
  else:
    ## PROD Storage Account
    storage_account_name = "prdrbainesasmartorder"
    spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorder', key = 'blob-prdrbainesasmartorder-key'))

# COMMAND ----------

# DBTITLE 1,Mount an Azure Blob Storage container
if environment == 'dev':
  if country_loop == 'INDIA_HEALTH':
    try:
      dbutils.fs.mount(
        source = data_path,
        mount_point = "/mnt/Config_Files_Validation",
        extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "devrbainekvsmartorderhc", key = "Blob-devrbainesasmartorderhc-key")}) ## Testing
    except:
      dbutils.fs.unmount("/mnt/Config_Files_Validation")
      dbutils.fs.mount(source = data_path, mount_point = "/mnt/Config_Files_Validation", extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "devrbainekvsmartorderhc", key = "Blob-devrbainesasmartorderhc-key")}) ## Testing
  else:
    try:
      dbutils.fs.mount(
        source = data_path,
        mount_point = "/mnt/Config_Files_Validation",
        extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "devrbainekvsmartorder", key = "Blob-devrbainesasmartorder-key")}) ## Testing
    except:
      dbutils.fs.unmount("/mnt/Config_Files_Validation")
      dbutils.fs.mount(source = data_path, mount_point = "/mnt/Config_Files_Validation", extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "devrbainekvsmartorder", key = "Blob-devrbainesasmartorder-key")}) ## Testing

else:
  if country_loop == 'INDIA_HEALTH':
    try:
      dbutils.fs.mount(
         source = data_path,
         mount_point = "/mnt/Config_Files_Validation", 
         extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "prdrbainekvsmartorderhc", key = "storageaccountsohcpulse")}) ## Production
    except:
      dbutils.fs.unmount("/mnt/Config_Files_Validation")
      dbutils.fs.mount(source = data_path, mount_point = "/mnt/Config_Files_Validation", extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "prdrbainekvsmartorderhc", key = "storageaccountsohcpulse")}) ## Production
  else:
    try:
      dbutils.fs.mount(
         source = data_path,
         mount_point = "/mnt/Config_Files_Validation", 
         extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "prdrbainekvsmartorder", key = "blob-prdrbainesasmartorder-key")}) ## Production
    except:
      dbutils.fs.unmount("/mnt/Config_Files_Validation")
      dbutils.fs.mount(source = data_path, mount_point = "/mnt/Config_Files_Validation", extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "prdrbainekvsmartorder", key = "blob-prdrbainesasmartorder-key")}) ## Production

# COMMAND ----------

# CHECKS

import pkg_resources

print("pip: ", pkg_resources.get_distribution("pip").version) ## Check pip version (28/05/2020) - MS
print("pyOpenSSL: ", pkg_resources.get_distribution("pyOpenSSL").version)
print("cryptography: ", pkg_resources.get_distribution("cryptography").version)

# COMMAND ----------

# DBTITLE 1,Libraries Import
from pyspark.sql.functions import count, col
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from re import sub
import io

## US: 8103 - New Library and commands added due to package "Azure" shut down (13/04/2020) - MS
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

# DBTITLE 1,Create the BlobService
## Preparing to write in the blob storage (18/12/2019) - JV

account_name = storage_account_name

if environment == 'dev':
  if country_loop == 'INDIA_HEALTH':
    account_key = dbutils.secrets.get(scope = 'devrbainekvsmartorderhc', key = 'Blob-devrbainesasmartorderhc-key') ## Testing
  else:
    account_key = dbutils.secrets.get(scope = 'devrbainekvsmartorder', key = 'Blob-devrbainesasmartorder-key') ## Testing

else:
  if country_loop == 'INDIA_HEALTH':
    account_key = dbutils.secrets.get(scope = 'prdrbainekvsmartorderhc', key = 'storageaccountsohcpulse') ## Production
  else:
    account_key = dbutils.secrets.get(scope = 'prdrbainekvsmartorder', key = 'blob-prdrbainesasmartorder-key') ## Production


## US: 8103 - New Library and commands added due to package "Azure" shut down (13/04/2020) - MS
BlobService = BlobServiceClient(account_url = "https://" + account_name + ".blob.core.windows.net", credential = account_key)
# call the class get_container_client to get te container:
BlobService.get_container_client(container_name)

# COMMAND ----------

# DBTITLE 1,Get Regions Control File to run
## Clear Control File folder
dbutils.fs.rm(control_f_path, True)

## Update the Control_File with the list of Regions to run from "SmartOrder_Files"
df = (dbutils.fs.ls(bis_control_f_path))
locf = []
for name in df:
  a = name[1].rsplit('.', 1)[0]
  locf.append(a)

## write Config files in the blob
for ls in locf:
  print(ls)
  cf = spark.read.csv(path = bis_control_f_path + '/' + ls + '.csv', header = True, inferSchema = True)
  ## convert spark df to a pandas df
#   exec('dt = {}'.format(ls))
  dt = cf.toPandas()

  ## Write the csv file in the blob storage where the empty df can keep their headers and columns naming (18/12/2019) - JV
  df_string = io.StringIO()
    
  df_string = dt.to_csv(encoding = "utf-8", index = False)

## US: 8103 - New Library and commands added due to package "Azure" shut down (13/04/2020) - MS
  BlobService.get_container_client(container_name).upload_blob(data = df_string, name = w_control_f_path + ls + '.csv', overwrite = True)

# COMMAND ----------

# DBTITLE 1,Create functions to interact with JSON
# Function to get a value from the json file: 
def get_value_from_file(file, key):
  with open(json_file, 'r') as f:
    data = json.load(f)
    
  try:
    return(data[file][0][key])
  except:
    return('error')


# Function to get a value from the column of a dataset in the json file: 
def get_value_from_column(file, column, key):
  with open(json_file, 'r') as f:
    data = json.load(f)
    
  return(data[file][0]['columns'][0][column][0][key])


# Function to get the list of columns of a dataset in the json file: 
def get_list_of_columns(dt):
  with open(json_file, 'r') as f:
    data = json.load(f)
  l = []
  for i in data[dt][0]['columns'][0]:
    l.append(i)
      
  return (l)


# Function to get the list of column names of a dataset in the json file: 
def get_list_of_columns_name(File):
  with open(json_file, 'r') as f:
    data = json.load(f)
    l = list()
    for Column in data[File][0]['columns'][0]:
      try:
        ColumnName = data[File][0]['columns'][0][Column][0]['col_name']
        l.append(ColumnName)
      except:
        print("Element has no col_name.")
  return (l)

# COMMAND ----------

# DBTITLE 1,Create the data-sets list, errors list, Regions list and variable "Issues"
if country_loop == 'INDIA_HEALTH':
  
  Regions_List = ['ASM_BENGALURU', 'ASM_DELHI', 'ASM_KOLKATA', 'ASM_HARYANA', 'ASM_MUMBAI_METRO']
  
  ## List of datasets that will be evaluated
  list_of_config_datasets = ['DIST_MAPPING_OLD']
  list_of_inc_datasets = ['DB_List', 'Smart_Category_Mapping','ND_INPUT']

elif country_loop == 'INDIA_HOME':
  
  Regions_List = ['ASM_APC-HOME', 'ASM_BANGALORE-HOME', 'ASM_BIH-HOME', 'ASM_CG-HOME', 'ASM_CHENNAI-HOME', 'ASM_DELHI-HOME', 'ASM_EUP-HOME', 'ASM_GUJ-HOME', 'ASM_HAR-HOME', 'ASM_HYD_UPC-HOME', 'ASM_JHK-HOME', 'ASM_KOL-HOME', 'ASM_KTK-HOME', 'ASM_MP-HOME', 'ASM_MUM Metro-HOME', 'ASM_MUPC-HOME', 'ASM_NM-HOME', 'ASM_NUP-HOME', 'ASM_PUN-HOME', 'ASM_RAJ-HOME', 'ASM_SM-HOME', 'ASM_SUP-HOME', 'ASM_TNN-HOME', 'ASM_TNS-HOME', 'ASM_WUP-HOME', 'ASM_KERALA-HOME', 'ASM_HAR-HOME', 'ASM_OR-HOME', 'ASM_NBUPC-HOME', 'ASM_NESA-HOME', 'ASM_UPCSB-HOME', 'ASM_DEL1-HOME', 'ASM_DEL2-HOME']
  
  ## List of datasets that will be evaluated
  list_of_config_datasets = ['DIST_MAPPING_OLD', 'CLUSTER_SIZE', 'SEASONAL_MONTHS']
  list_of_inc_datasets = ['ND_INPUT', 'DB_List', 'SKU_EXCLUDE', 'Smart_Category_Mapping', 'TARGET']

elif country_loop == 'PAKISTAN_HOME':
  
  Regions_List = ['Central', 'North', 'PC ISB', 'PC LHR', 'South Punjab', 'South', 'PC KHI']
  
  ## List of datasets that will be evaluated
  list_of_config_datasets = ['DIST_MAPPING', 'CLUSTER_SIZE', 'SEASONAL_MONTHS']
  list_of_inc_datasets = ['ND_INPUT', 'TARGET']

elif country_loop == 'INDO_HOME':
  
  Regions_List = ['BANDUNG', 'CIREBON', 'GRESIK', 'JAKARTA - TM 1', 'JOGJA', 'KEDIRI', 'MAKASSAR', 'MALANG', 'MANADO', 'MEDAN', 'PEKANBARU', 'SEMARANG', 'SIDOARJO', 'SURABAYA', 'TASIK']
  
  ## List of datasets that will be evaluated
  list_of_config_datasets = ['DIST_MAPPING', 'CLUSTER_SIZE', 'SEASONAL_MONTHS']
  list_of_inc_datasets = ['ND_INPUT', 'TARGET']
  

name_form1 = []
name_form2 = []
efiles = []
wregs = []
dupfiles_reg = []
list_of_datasets = ['list_of_config_datasets', 'list_of_inc_datasets']
Issues = 0

# COMMAND ----------

# DBTITLE 1,Check naming and format of files columns
for l in list_of_datasets:
  exec('lod = {}'.format(l))
  for i in lod:
    if l == 'list_of_config_datasets':
      input_f = spark.read.csv(path = bis_config_path + '/' + i + '.csv', header = True, inferSchema = True)
    elif l == 'list_of_inc_datasets':
      input_f = spark.read.csv(path = bis_increment_path + '/' + i + '.csv', header = True, inferSchema = True)
    a = [x for x in input_f.columns if x not in get_list_of_columns_name(i)]
    b = [x for x in get_list_of_columns_name(i) if x not in input_f.columns]
    if (a == b):
      print('{} is OK!'.format(i))
    elif (b > a):
      print("Warning:")
      print('File {} column(s) {} have a naming issue or column(s) are missing!'.format(i,b))
      print('Please correct {} file!'.format(i))
      Issues = Issues +1
      name_form1.append('- File {} column {} has a naming issue or a missing column!'.format(i,b))
    elif (a > b and (country_loop != 'INDIA_HEALTH' and input_f != 'ND_INPUT')) :
      print("Warning:")
      print('File {} column(s) {} have a naming issue or column(s) are missing!'.format(i,a))
      print('Please correct {} file!'.format(i))
      Issues = Issues +1
      name_form2.append('- File {} column {} has a naming issue or a missing column!'.format(i,b))

# COMMAND ----------

# DBTITLE 1,Check for empty files
if country_loop == 'PAKISTAN_HOME': ## Because for Pakistan IN_INPUT file can be empty we won't consider it for this check (29/11/2019) - MS
  list_of_inc_datasets = ['TARGET'] ## Because for Pakistan IN_INPUT file can be empty we won't consider it for this check 


for l in list_of_datasets:
  exec('lod = {}'.format(l))
  for i in lod:
    if l == 'list_of_config_datasets':
      input_f = spark.read.csv(path = bis_config_path + '/' + i + '.csv', header = True, inferSchema = True)
    elif l == 'list_of_inc_datasets':
      input_f = spark.read.csv(path = bis_increment_path + '/' + i + '.csv', header = True, inferSchema = True)
    check_empty = get_value_from_file(i, 'Is_Empty')
    if (input_f.count() == 0) & (check_empty == True):
        print("NOTE:")
        print('Mandatory {} file is empty, but it is allowed!'.format(i))
    else:
      if (input_f.count() == 0) & (check_empty == False):
        print("WARNING:")
        print('Mandatory {} file is empty!'.format(i))
        Issues = Issues +1
        efiles.append('- Mandatory {} file is empty!'.format(i))
      else:
        print('{} file is OK!'.format(i))
  
if country_loop == 'PAKISTAN_HOME': ## Because for Pakistan IN_INPUT file can be empty we won't consider it for this check (29/11/2019) - MS
  list_of_inc_datasets = ['ND_INPUT', 'TARGET'] ## Reinsert IN_INPUT file in the list_of_datasets for the next checks (29/11/2019) - MS

# COMMAND ----------

# DBTITLE 1,Check in Region\ ASM\ State column any misspelling region
reg = ['Region', 'REGION', 'State', 'state', 'ASM', 'asm']

final_list = []

for file_in_blob in dbutils.fs.ls(data_path + '/' + country_loop + '/Control_File/'):
  final_list.append(sub('.csv', '', file_in_blob[1]))


for l in list_of_datasets:
  exec('lod = {}'.format(l))
  for i in lod:
    if l == 'list_of_config_datasets':
      input_f = spark.read.csv(path = bis_config_path + '/' + i + '.csv', header = True, inferSchema = True)
    elif l == 'list_of_inc_datasets':
      input_f = spark.read.csv(path = bis_increment_path + '/' + i + '.csv', header = True, inferSchema = True)
    a = [x for x in input_f.columns if x in reg]
    if len(a) > 0:
      for fl in final_list:
        input_f = input_f.filter(~col(a[0]).contains(fl))
      if input_f.count() > 0: 
        c = input_f.select(a[0]).distinct().rdd.flatMap(lambda x: x).collect()
        if len([x for x in c if x in Regions_List]) == len(c):
          print("Note:")
          print('Although in the {} file the regions: {} do not match the Control_File, there is no problem with it!'.format(i, c))
        else:
          c = [x for x in c if x not in Regions_List]
          regs = ''
          for d in c: 
            regs = regs + '<i>' + str(d) + ', ' + '</i>'
          regs = regs.replace("[","").replace("]","").replace("'","").replace('"','')
          print("WARNING:")
          print('For the {} file the regions: {} do not match the Control_File!'.format(i, c))
          Issues = Issues +1
          wregs.append('- For the {} file the regions: {} do not match the Control_File!'.format(i, regs))
      else:
        print('The regions of {} file are in order!'.format(i))

# COMMAND ----------

# DBTITLE 1,Check for duplicate rows in files
for l in list_of_datasets:
  exec('lod = {}'.format(l))
  for i in lod:
    if l == 'list_of_config_datasets':
      input_f = spark.read.csv(path = bis_config_path + '/' + i + '.csv', header = True, inferSchema = True)
    elif l == 'list_of_inc_datasets':
      input_f = spark.read.csv(path = bis_increment_path + '/' + i + '.csv', header = True, inferSchema = True)
    check_duplic = get_value_from_file(i, 'Has_Duplicates')
    if (input_f.count() > input_f.dropDuplicates().count()) & (check_duplic == True):
      print("NOTE:")
      print('Mandatory {} file contains duplicate data, but it is allowed!'.format(i))
    else:
      if (input_f.count() > input_f.dropDuplicates().count()) & (check_duplic == False):
        print("WARNING:")
        print('{} file contains duplicate data!'.format(i))
        Issues = Issues +1
        dupfiles_reg.append('- {} file contains duplicate data!'.format(i))
      else:
        print('{} file is OK!'.format(i))

# COMMAND ----------

# DBTITLE 1,Gather the errors and create an email with the report
if country_loop == 'INDIA_HOME' or country_loop == 'PAKISTAN_HOME' or country_loop == 'INDIA_HEALTH':

  error_description = [name_form1, name_form2, efiles, wregs, dupfiles_reg]

  sample =''

  for a in error_description:
    for c in a: 
      sample = sample + '<p>' + str(c) + '</p>'

  
  sample = sample.replace("[","").replace("]","").replace("'","").replace('"','')

  mailserver = smtplib.SMTP('smtp.office365.com',587)
  mailserver.ehlo()
  mailserver.starttls()
  
  if environment == 'dev':
    if country_loop == 'INDIA_HEALTH':
      RBOneMailPass = dbutils.secrets.get(scope = "devrbainekvsmartorderhc", key = "Password-RBOneManagement")
    else:
      RBOneMailPass = dbutils.secrets.get(scope = "devrbainekvsmartorder", key = "Password-RBOneManagement")
  else:
    if country_loop == 'INDIA_HEALTH':
      RBOneMailPass = dbutils.secrets.get(scope = "prdrbainekvsmartorderhc", key = "Password-RBOneManagement")
    else:
      RBOneMailPass = dbutils.secrets.get(scope = "prdrbainekvsmartorder", key = "Password-RBOneManagement")
  

  mailserver.login("RBONE.management@rb.com", RBOneMailPass)

  subject = "Smart Order - Pipeline run STOPPED for " + country_loop + " due to business file error"
  
  if environment == 'dev':
    toaddr = ['Kapil.Mathur@rb.com', 'Rohit.Kumar6@rb.com']## 'Rohit.Kumar6@rb.com', 'Abhigyan.Shivam@rb.com', 'Vatul.Parakh@rb.com', 'Chandana.Mysore@rb.com', 'onedatascience.support@rb.com' (Ticket)
    cc = ['Martim.Santos@rb.com'] ## 'DLRBOneSupportTeamPT@rb.com', 'Martim.Santos@rb.com', 'Bruno.Cardoso@rb.com', 'Jose.Viegas@rb.com'
  else:
    toaddr = ['Kapil.Mathur@rb.com', 'Rohit.Kumar6@rb.com', 'Chandana.Mysore@rb.com', 'onedatascience.support@rb.com'] ## 'Kapil.Mathur@rb.com', 'Rohit.Kumar6@rb.com', 'Chandana.Mysore@rb.com', 'onedatascience.support@rb.com' (Ticket)
    cc = ['DLRBOneSupportTeamPT@rb.com', 'Martim.Santos@rb.com', 'Bruno.Cardoso@rb.com', 'Jose.Viegas@rb.com'] ## 'DLRBOneSupportTeamPT@rb.com', 'Martim.Santos@rb.com', 'Bruno.Cardoso@rb.com', 'Jose.Viegas@rb.com'
    

  msg = MIMEMultipart('alternative')
  msg['Subject'] = subject
  msg['From'] = "RBONE.management@rb.com"
  msg['To'] = ', '.join(toaddr) ## If this is commented the mail receivers will not be able to check who received the email too!
  msg['Cc'] = ', '.join(cc) ## If this is commented the mail receivers will not be able to check who received the email too!

  msgTEXT = "<p><h1><b><font color='red'><i>WARNING</i></font></b></h1></p><p>Smart Order run stopped due to an issue in the <i><b>Input Files</b></i> from the business! Below follows the " + str(Issues) + " issue(s) description:</p>" "<b>"+ sample + "</b><p></p><p>Please, address this issue as soon as possible and, once it's solved, reply to all in this email for our support team be able to restart the pipeline!</p>"


  msg.attach(MIMEText(msgTEXT, 'html'))

# COMMAND ----------

if country_loop == 'INDIA_HOME' or country_loop == 'PAKISTAN_HOME' or country_loop == 'INDIA_HEALTH':

  if Issues > 0:
    mailserver.sendmail(msg['From'], (toaddr+cc), msg.as_string())
    mailserver.quit()
    print("Email sent.")
    if Issues == 1:
      ToBe = 'is'
    else:
      ToBe = 'are'
    raise ValueError('Input files are NOT in order: There ' + ToBe + ' ' + str(Issues) + ' issue(s) to solve. Please check email sent!')
  else:
    print('All files are OK!')

# COMMAND ----------

# DBTITLE 1,Write Business input csv files in Smart Order's blob
## List of Config files to be saved
df_bcp = (dbutils.fs.ls(bis_config_path))
config_files_to_save_list = []
for name in df_bcp:
  a = name[1].rsplit('.', 1)[0]
  config_files_to_save_list.append(a)
   

## List of Incremental files to be saved
df_bip = (dbutils.fs.ls(bis_increment_path))
inc_files_to_save_list = []
for name in df_bip:
  a = name[1].rsplit('.', 1)[0]
  inc_files_to_save_list.append(a)

  
list_of_input_files = ['config_files_to_save_list', 'inc_files_to_save_list']  


for l in list_of_input_files:
  exec('lif = {}'.format(l))
  ## write Config files in the blob
  for ls in lif:
    print(ls)
    if l == 'config_files_to_save_list':
      if ls == 'BUCKETS_CUTOFF': ## Added due error raised because there was a Column with 1,0E+29 (18/12/2019) - JV, MS
        conf_f = spark.read.csv(path = bis_config_path + '/' + ls + '.csv', header = True, inferSchema = False)
      else:
        conf_f = spark.read.csv(path = bis_config_path + '/' + ls + '.csv', header = True, inferSchema = True)
      ## convert spark df to a pandas df
      dt = conf_f.toPandas()
      ## Write the csv file in the blob storage where the empty df can keep their headers and columns naming (18/12/2019) - JV
      df_string = io.StringIO()
      df_string = dt.to_csv(encoding = "utf-8", index = False)
      
## US: 8103 - New Library and commands added due to package "Azure" shut down (13/04/2020) - MS
      BlobService.get_container_client(container_name).upload_blob(data = df_string, name = w_config_path + ls + '.csv', overwrite = True)

      
    elif l == 'inc_files_to_save_list':
      incf = spark.read.csv(path = bis_increment_path + '/' + ls + '.csv', header = True, inferSchema = True)
      ## convert spark df to a pandas df
      dt = incf.toPandas()
      ## Write the csv file in the blob storage where the empty df can keep their headers and columns naming (18/12/2019) - JV
      df_string = io.StringIO()
      df_string = dt.to_csv(encoding = "utf-8", index = False)
      
## US: 8103 - New Library and commands added due to package "Azure" shut down (13/04/2020) - MS
      BlobService.get_container_client(container_name).upload_blob(data = df_string, name = w_incremental_path + ls + '.csv', overwrite = True)
