# Databricks notebook source
# MAGIC %md
# MAGIC ## Changelog
# MAGIC * 10/03/2020 - cmd2: "data_path" variable was substituted by "environment" variable in order to make the code more agile.

# COMMAND ----------

# DBTITLE 1,Define variables and paths
try:
  country_loop = getArgument('VAR_COUNTRY')
  environment = getArgument('VAR_ENVIRONMENT')

except:
  country_loop = 'INDIA_HEALTH'
  environment = 'dev' ## prod, dev ## Creation of the variable "Environment" 09/03/2020 - MS


## Data path definition
if environment == 'dev':
  if country_loop == 'INDIA_HEALTH':
    data_path = 'wasbs://smartorder@devrbainesasmartorderhc.blob.core.windows.net/dev' ## New DEV Env
  else:    
    data_path = 'wasbs://smartorder@devrbainesasmartorder.blob.core.windows.net/dev' ## New DEV Env
elif environment == 'prod':
  if country_loop == 'INDIA_HEALTH':
    data_path = 'wasbs://smartorder@prdrbainesasmartorderhc.blob.core.windows.net/prod'
  else:
    data_path = 'wasbs://smartorder@prdrbainesasmartorder.blob.core.windows.net/prod' ## New Prod Env
else:
  raise ValueError('Undefined Environment! Please select "dev" or "prod" as the environment.')
  
inter_path = data_path + '/' + country_loop + '/INTERMEDIATE/'
config_path = data_path + '/' + country_loop + '/INPUTS/CONFIG/'
increment_path = data_path + '/' + country_loop + '/INPUTS/INCREMENTAL/'
output_path = data_path + '/' + country_loop + '/OUTPUTS'

json_file = '/dbfs/mnt/Config_Files_Validation/file_config_val.json'

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
      print("Directory already mounted!")
  else:
    try:
      dbutils.fs.mount(
        source = data_path,
        mount_point = "/mnt/Config_Files_Validation",
        extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "devrbainekvsmartorder", key = "Blob-devrbainesasmartorder-key")}) ## Testing
    except:
      print("Directory already mounted!")

else:
  if country_loop == 'INDIA_HEALTH':
    try:
      dbutils.fs.mount(
         source = data_path,
         mount_point = "/mnt/Config_Files_Validation", 
         extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": storage_account_access_key}) ## Production
    except:
      print("Directory already mounted!")
    else:
      try:
        dbutils.fs.mount(
           source = data_path,
           mount_point = "/mnt/Config_Files_Validation", 
           extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": dbutils.secrets.get(scope = "prdrbainekvsmartorder", key = "blob-prdrbainesasmartorder-key")}) ## Production
      except:
        print("Directory already mounted!")

# COMMAND ----------

# DBTITLE 1,Libraries import
import json

# COMMAND ----------

# DBTITLE 1,Functions to create Json file
# Function to create a json file:
def create_json():
  data = {}
  with open(json_file, 'w') as f:
    json.dump(data, f)


# Function to add a dataset to the json file:   
def add_dataset_to_json(file, key, value):
  with open(json_file, 'r') as f:
    data = json.load(f)
    
  try:
    data[file][0][key] = value
  except:
    data[file] = [{}]
    data[file][0][key] = value
    
  with open(json_file, 'w') as f:
    json.dump(data, f)


# Function to add a column to the dataset in the json file:    
def add_column_to_file(file, column, key, value):
  with open(json_file, 'r') as f:
    data = json.load(f)
    
  try:
    data[file][0]['columns'][0][column][0][key] = value
  except:
    try:
      data[file][0]['columns'][0][column] = [{}]
      data[file][0]['columns'][0][column][0][key] = value
    except:
      try:
        data[file][0]['columns'] = [{}]
        data[file][0]['columns'][0][column] = [{}]
        data[file][0]['columns'][0][column][0][key] = value
      except:
        data[file] = [{}]
        data[file][0]['columns'] = [{}]
        data[file][0]['columns'][0][column] = [{}]
        data[file][0]['columns'][0][column][0][key] = value
      
      
    
  with open(json_file, 'w') as f:
    json.dump(data, f)

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

# COMMAND ----------

# DBTITLE 1,Json file creation
create_json()

add_dataset_to_json('DIST_MAPPING_OLD', 'Is_Empty', False)
add_dataset_to_json('DIST_MAPPING_OLD', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING_OLD', 'Region', 'col_name', 'Region')
add_column_to_file('DIST_MAPPING_OLD', 'Region', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING_OLD', 'Region', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING_OLD', 'Focus', 'col_name', 'Focus')
add_column_to_file('DIST_MAPPING_OLD', 'Focus', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING_OLD', 'Focus', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING_OLD', 'Old_DB', 'col_name', 'Old_DB')
add_column_to_file('DIST_MAPPING_OLD', 'Old_DB', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING_OLD', 'Old_DB', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING_OLD', 'New_DB', 'col_name', 'New_DB')
add_column_to_file('DIST_MAPPING_OLD', 'New_DB', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING_OLD', 'New_DB', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING_OLD', 'Old_StoreCode', 'col_name', 'Old_StoreCode')
add_column_to_file('DIST_MAPPING_OLD', 'Old_StoreCode', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING_OLD', 'Old_StoreCode', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING_OLD', 'New_StoreCode', 'col_name', 'New_StoreCode')
add_column_to_file('DIST_MAPPING_OLD', 'New_StoreCode', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING_OLD', 'New_StoreCode', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING_OLD', 'Weekly', 'col_name', 'Weekly')
add_column_to_file('DIST_MAPPING_OLD', 'Weekly', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING_OLD', 'Weekly', 'Has_Duplicates', False)

add_dataset_to_json('CLUSTER_SIZE', 'Is_Empty', False)
add_dataset_to_json('CLUSTER_SIZE', 'Has_Duplicates', False)
add_column_to_file('CLUSTER_SIZE', 'REGION', 'col_name', 'REGION')
add_column_to_file('CLUSTER_SIZE', 'REGION', 'Has_NaN_Null', False)
add_column_to_file('CLUSTER_SIZE', 'REGION', 'Has_Duplicates', False)
add_column_to_file('CLUSTER_SIZE', 'FOCUS', 'col_name', 'FOCUS')
add_column_to_file('CLUSTER_SIZE', 'FOCUS', 'Has_NaN_Null', False)
add_column_to_file('CLUSTER_SIZE', 'FOCUS', 'Has_Duplicates', False)
add_column_to_file('CLUSTER_SIZE', 'Classification', 'col_name', 'Classification')
add_column_to_file('CLUSTER_SIZE', 'Classification', 'Has_NaN_Null', False)
add_column_to_file('CLUSTER_SIZE', 'Classification', 'Has_Duplicates', False)
add_column_to_file('CLUSTER_SIZE', 'csize', 'col_name', 'csize')
add_column_to_file('CLUSTER_SIZE', 'csize', 'Has_NaN_Null', False)
add_column_to_file('CLUSTER_SIZE', 'csize', 'Has_Duplicates', False)

add_dataset_to_json('SEASONAL_MONTHS', 'Is_Empty', False)
add_dataset_to_json('SEASONAL_MONTHS', 'Has_Duplicates', False)
add_column_to_file('SEASONAL_MONTHS', 'Region', 'col_name', 'Region')
add_column_to_file('SEASONAL_MONTHS', 'Region', 'Has_NaN_Null', False)
add_column_to_file('SEASONAL_MONTHS', 'Region', 'Has_Duplicates', False)
add_column_to_file('SEASONAL_MONTHS', 'Brand', 'col_name', 'Brand')
add_column_to_file('SEASONAL_MONTHS', 'Brand', 'Has_NaN_Null', False)
add_column_to_file('SEASONAL_MONTHS', 'Brand', 'Has_Duplicates', False)
add_column_to_file('SEASONAL_MONTHS', 'Months', 'col_name', 'Months')
add_column_to_file('SEASONAL_MONTHS', 'Months', 'Has_NaN_Null', False)
add_column_to_file('SEASONAL_MONTHS', 'Months', 'Has_Duplicates', False)
add_column_to_file('SEASONAL_MONTHS', 'Group', 'col_name', 'Group')
add_column_to_file('SEASONAL_MONTHS', 'Group', 'Has_NaN_Null', False)
add_column_to_file('SEASONAL_MONTHS', 'Group', 'Has_Duplicates', False)

add_dataset_to_json('ND_INPUT', 'Is_Empty', False)
add_dataset_to_json('ND_INPUT', 'Has_Duplicates', False)
add_column_to_file('ND_INPUT', 'State', 'col_name', 'State')
add_column_to_file('ND_INPUT', 'State', 'Has_NaN_Null', False)
add_column_to_file('ND_INPUT', 'State', 'Has_Duplicates', False)
add_column_to_file('ND_INPUT', 'PR_CODE', 'col_name', 'PR_CODE')
add_column_to_file('ND_INPUT', 'PR_CODE', 'Has_NaN_Null', False)
add_column_to_file('ND_INPUT', 'PR_CODE', 'Has_Duplicates', False)
add_column_to_file('ND_INPUT', 'Classification', 'col_name', 'Classification')
add_column_to_file('ND_INPUT', 'Classification', 'Has_NaN_Null', False)
add_column_to_file('ND_INPUT', 'Classification', 'Has_Duplicates', False)
add_column_to_file('ND_INPUT', 'STRATERGIC_COUNT', 'col_name', 'STRATERGIC_COUNT')
add_column_to_file('ND_INPUT', 'STRATERGIC_COUNT', 'Has_NaN_Null', False)
add_column_to_file('ND_INPUT', 'STRATERGIC_COUNT', 'Has_Duplicates', False)
add_column_to_file('ND_INPUT', 'Category.Desc', 'col_name', 'Category.Desc')
add_column_to_file('ND_INPUT', 'Category.Desc', 'Has_NaN_Null', False)
add_column_to_file('ND_INPUT', 'Category.Desc', 'Has_Duplicates', False)

add_dataset_to_json('DB_List', 'Is_Empty', False)
add_dataset_to_json('DB_List', 'Has_Duplicates', False)
add_column_to_file('DB_List', 'DB CODE0', 'col_name', 'DB CODE0')
add_column_to_file('DB_List', 'DB CODE0', 'Has_NaN_Null', False)
add_column_to_file('DB_List', 'DB CODE0', 'Has_Duplicates', False)
add_column_to_file('DB_List', 'ZSM', 'col_name', 'ZSM')
add_column_to_file('DB_List', 'ZSM', 'Has_NaN_Null', False)
add_column_to_file('DB_List', 'ZSM', 'Has_Duplicates', False)
add_column_to_file('DB_List', 'ASM', 'col_name', 'ASM')
add_column_to_file('DB_List', 'ASM', 'Has_NaN_Null', False)
add_column_to_file('DB_List', 'ASM', 'Has_Duplicates', False)
add_column_to_file('DB_List', 'TSI', 'col_name', 'TSI')
add_column_to_file('DB_List', 'TSI', 'Has_NaN_Null', False)
add_column_to_file('DB_List', 'TSI', 'Has_Duplicates', False)
add_column_to_file('DB_List', 'DB CODE4', 'col_name', 'DB CODE4')
add_column_to_file('DB_List', 'DB CODE4', 'Has_NaN_Null', False)
add_column_to_file('DB_List', 'DB CODE4', 'Has_Duplicates', False)
add_column_to_file('DB_List', 'DB NAME', 'col_name', 'DB NAME')
add_column_to_file('DB_List', 'DB NAME', 'Has_NaN_Null', False)
add_column_to_file('DB_List', 'DB NAME', 'Has_Duplicates', False)

add_dataset_to_json('TARGET', 'Is_Empty', False)
add_dataset_to_json('TARGET', 'Has_Duplicates', False)
add_column_to_file('TARGET', 'Region', 'col_name', 'Region')
add_column_to_file('TARGET', 'Region', 'Has_NaN_Null', False)
add_column_to_file('TARGET', 'Region', 'Has_Duplicates', False)
add_column_to_file('TARGET', 'DB', 'col_name', 'DB')
add_column_to_file('TARGET', 'DB', 'Has_NaN_Null', True)
add_column_to_file('TARGET', 'DB', 'Has_Duplicates', False)
add_column_to_file('TARGET', 'Channel', 'col_name', 'Channel')
add_column_to_file('TARGET', 'Channel', 'Has_NaN_Null', True)
add_column_to_file('TARGET', 'Channel', 'Has_Duplicates', False)
add_column_to_file('TARGET', 'Sales_Rep', 'col_name', 'Sales_Rep')
add_column_to_file('TARGET', 'Sales_Rep', 'Has_NaN_Null', True)
add_column_to_file('TARGET', 'Sales_Rep', 'Has_Duplicates', False)
add_column_to_file('TARGET', 'Target', 'col_name', 'Target')
add_column_to_file('TARGET', 'Target', 'Has_NaN_Null', False)
add_column_to_file('TARGET', 'Target', 'Has_Duplicates', False)

add_dataset_to_json('SKU_EXCLUDE', 'Is_Empty', False)
add_dataset_to_json('SKU_EXCLUDE', 'Has_Duplicates', False)
add_column_to_file('SKU_EXCLUDE', 'Product SKU Code', 'col_name', 'Product SKU Code')
add_column_to_file('SKU_EXCLUDE', 'Product SKU Code', 'Has_NaN_Null', False)
add_column_to_file('SKU_EXCLUDE', 'Product SKU Code', 'Has_Duplicates', False)
add_column_to_file('SKU_EXCLUDE', 'Supergroup', 'col_name', 'Supergroup')
add_column_to_file('SKU_EXCLUDE', 'Supergroup', 'Has_NaN_Null', False)
add_column_to_file('SKU_EXCLUDE', 'Supergroup', 'Has_Duplicates', False)

add_dataset_to_json('Smart_Category_Mapping', 'Is_Empty', False)
add_dataset_to_json('Smart_Category_Mapping', 'Has_Duplicates', False)
add_column_to_file('Smart_Category_Mapping', 'Product Code', 'col_name', 'Product Code')
add_column_to_file('Smart_Category_Mapping', 'Product Code', 'Has_NaN_Null', False)
add_column_to_file('Smart_Category_Mapping', 'Product Code', 'Has_Duplicates', False)
add_column_to_file('Smart_Category_Mapping', 'Group Code', 'col_name', 'Group Code')
add_column_to_file('Smart_Category_Mapping', 'Group Code', 'Has_NaN_Null', False)
add_column_to_file('Smart_Category_Mapping', 'Group Code', 'Has_Duplicates', False)
add_column_to_file('Smart_Category_Mapping', 'Group Name', 'col_name', 'Group Name')
add_column_to_file('Smart_Category_Mapping', 'Group Name', 'Has_NaN_Null', False)
add_column_to_file('Smart_Category_Mapping', 'Group Name', 'Has_Duplicates', False)
add_column_to_file('Smart_Category_Mapping', 'New MicroSegment', 'col_name', 'New MicroSegment')
add_column_to_file('Smart_Category_Mapping', 'New MicroSegment', 'Has_NaN_Null', False)
add_column_to_file('Smart_Category_Mapping', 'New MicroSegment', 'Has_Duplicates', False)

add_dataset_to_json('Price', 'Is_Empty', False)
add_dataset_to_json('Price', 'Has_Duplicates', False)
add_column_to_file('Price', 'Supergroup', 'col_name', 'Supergroup')
add_column_to_file('Price', 'Supergroup', 'Has_NaN_Null', False)
add_column_to_file('Price', 'Supergroup', 'Has_Duplicates', False)
add_column_to_file('Price', 'Price_Shared', 'col_name', 'Price_Shared')
add_column_to_file('Price', 'Price_Shared', 'Has_NaN_Null', False)
add_column_to_file('Price', 'Price_Shared', 'Has_Duplicates', False)

add_dataset_to_json('UPLIFT_NUMERIC', 'Is_Empty', False)
add_dataset_to_json('UPLIFT_NUMERIC', 'Has_Duplicates', False)
add_column_to_file('UPLIFT_NUMERIC', 'Region', 'col_name', 'Region')
add_column_to_file('UPLIFT_NUMERIC', 'Region', 'Has_NaN_Null', False)
add_column_to_file('UPLIFT_NUMERIC', 'Region', 'Has_Duplicates', False)
add_column_to_file('UPLIFT_NUMERIC', 'Outlet category name', 'col_name', 'Outlet category name')
add_column_to_file('UPLIFT_NUMERIC', 'Outlet category name', 'Has_NaN_Null', False)
add_column_to_file('UPLIFT_NUMERIC', 'Outlet category name', 'Has_Duplicates', False)
add_column_to_file('UPLIFT_NUMERIC', 'Store.Code', 'col_name', 'Store.Code')
add_column_to_file('UPLIFT_NUMERIC', 'Store.Code', 'Has_NaN_Null', False)
add_column_to_file('UPLIFT_NUMERIC', 'Store.Code', 'Has_Duplicates', False)
add_column_to_file('UPLIFT_NUMERIC', 'Brand', 'col_name', 'Brand')
add_column_to_file('UPLIFT_NUMERIC', 'Brand', 'Has_NaN_Null', False)
add_column_to_file('UPLIFT_NUMERIC', 'Brand', 'Has_Duplicates', False)
add_column_to_file('UPLIFT_NUMERIC', 'Category Desc', 'col_name', 'Category Desc')
add_column_to_file('UPLIFT_NUMERIC', 'Category Desc', 'Has_NaN_Null', False)
add_column_to_file('UPLIFT_NUMERIC', 'Category Desc', 'Has_Duplicates', False)
add_column_to_file('UPLIFT_NUMERIC', 'Product SKU Code', 'col_name', 'Product SKU Code')
add_column_to_file('UPLIFT_NUMERIC', 'Product SKU Code', 'Has_NaN_Null', False)
add_column_to_file('UPLIFT_NUMERIC', 'Product SKU Code', 'Has_Duplicates', False)
add_column_to_file('UPLIFT_NUMERIC', 'DB', 'col_name', 'DB')
add_column_to_file('UPLIFT_NUMERIC', 'DB', 'Has_NaN_Null', False)
add_column_to_file('UPLIFT_NUMERIC', 'DB', 'Has_Duplicates', False)

## New table added for PAKISTAN and INDONESIA (28/11/2019) - MS
add_dataset_to_json('DIST_MAPPING', 'Is_Empty', False)
add_dataset_to_json('DIST_MAPPING', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING', 'Region', 'col_name', 'Region')
add_column_to_file('DIST_MAPPING', 'Region', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING', 'Region', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING', 'Focus', 'col_name', 'Focus')
add_column_to_file('DIST_MAPPING', 'Focus', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING', 'Focus', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING', 'Old_DB', 'col_name', 'Old_DB')
add_column_to_file('DIST_MAPPING', 'Old_DB', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING', 'Old_DB', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING', 'New_DB', 'col_name', 'New_DB')
add_column_to_file('DIST_MAPPING', 'New_DB', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING', 'New_DB', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING', 'Old_StoreCode', 'col_name', 'Old_StoreCode')
add_column_to_file('DIST_MAPPING', 'Old_StoreCode', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING', 'Old_StoreCode', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING', 'New_StoreCode', 'col_name', 'New_StoreCode')
add_column_to_file('DIST_MAPPING', 'New_StoreCode', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING', 'New_StoreCode', 'Has_Duplicates', False)
add_column_to_file('DIST_MAPPING', 'Weekly', 'col_name', 'Weekly')
add_column_to_file('DIST_MAPPING', 'Weekly', 'Has_NaN_Null', False)
add_column_to_file('DIST_MAPPING', 'Weekly', 'Has_Duplicates', False)

## New table added for INDIA_HEALTH (25/02/2021) - Rohit
add_dataset_to_json('REGIONAL_DRIVE_SKU', 'Is_Empty', True)
add_dataset_to_json('REGIONAL_DRIVE_SKU', 'Has_Duplicates', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'ASM', 'col_name', 'ASM')
add_column_to_file('REGIONAL_DRIVE_SKU', 'ASM', 'Has_NaN_Null', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'ASM', 'Has_Duplicates', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'PRD_CD', 'col_name', 'PRD_CD')
add_column_to_file('REGIONAL_DRIVE_SKU', 'PRD_CD', 'Has_NaN_Null', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'PRD_CD', 'Has_Duplicates', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Channel', 'col_name', 'Channel')
add_column_to_file('REGIONAL_DRIVE_SKU', 'Channel', 'Has_NaN_Null', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Channel', 'Has_Duplicates', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Class', 'col_name', 'Class')
add_column_to_file('REGIONAL_DRIVE_SKU', 'Class', 'Has_NaN_Null', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Class', 'Has_Duplicates', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Period', 'col_name', 'Period')
add_column_to_file('REGIONAL_DRIVE_SKU', 'Period', 'Has_NaN_Null', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Period', 'Has_Duplicates', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Category_Desc', 'col_name', 'Category_Desc')
add_column_to_file('REGIONAL_DRIVE_SKU', 'Category_Desc', 'Has_NaN_Null', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Category_Desc', 'Has_Duplicates', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Strategic_Count', 'col_name', 'Strategic_Count')
add_column_to_file('REGIONAL_DRIVE_SKU', 'Strategic_Count', 'Has_NaN_Null', False)
add_column_to_file('REGIONAL_DRIVE_SKU', 'Strategic_Count', 'Has_Duplicates', False)

# COMMAND ----------

# DBTITLE 1,View json file created
with open(json_file, 'r') as f:
  print(json.load(f))
