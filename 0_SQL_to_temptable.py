# Databricks notebook source
# MAGIC %md
# MAGIC ## Changelog
# MAGIC * 24/06/2019 - Change table path on `cmd 10`from `smart_order.' + region_home + '_vwDS_SO_' + country_home + '_2YR'` to `smart_order_' + region_home + '_' + country_home + '.vwDS_SO_' + country_home + '_2YR'`. This method will prevent conflict when running multiple regions and countries at the same time on different pipelines.  
# MAGIC * 28/06/2019 - Remmoved join with dblist `cmd 9` to avoid any manipulation of Region column coming from SQL extract. This change is done becuase SQL data has been updated and now we dont have to update the Region column after extraction  
# MAGIC * 25/07/2019 - Change the arguments for `try except`approach;
# MAGIC * 18/09/2019 - US 5794 - Automated start and end dates treatment for scheduled executions without manual intervention;
# MAGIC * 20/09/2019 - cmd4: Added improvement to get the last day of each month dynamically;
# MAGIC * 07/10/2019 - cmd12: Added improvement due to upper/lower case string issue in sql data in region CHENNAI, ASM_CHENNAI-HOME and ASM_CHENNAI-Home;
# MAGIC * 10/10/2019 - cmd3: Added change to get start_date dynamically. 3 years back from end date;
# MAGIC * 11/10/2019 - cmd12: Added to dataframe dt the column "restated_store_code" in order to have no empty values in this column;
# MAGIC * 24/10/2019 - cmd2: Added a key vault to prevent from having passwords on display.
# MAGIC * 10/03/2020 - cmd2: "data_path" variable was substituted by "environment" variable in order to make the code more agile.

# COMMAND ----------

# DBTITLE 1,Define Variables
try:
  region_home = getArgument('VAR_SCHEMA_NAME')
  country_home = getArgument('VAR_COUNTRY_HOME')
  country_loop = getArgument('VAR_COUNTRY')
  start_date = getArgument('VAR_START_DATE')
  end_date = getArgument('VAR_END_DATE')
  database_name = getArgument('VAR_DATABASE_NAME')
  environment = getArgument('VAR_ENVIRONMENT')
#   flag_overwrite_dates = getArgument('VAR_FLAG_OVERWRITE_DATES')

except:
  region_home = 'ASEAN' ## SOA, NAMET, ASEAN
  country_home = 'MY' ## INDIA, PK, INDONESIA,MY
  country_loop = 'MY_HEALTH' ## INDIA_HOME, PAKISTAN_HOME, INDO_HOME
  start_date = 201701
  end_date = 202010
#   database_name = 'smart_order' ## Production
  database_name = 'smart_order_dev_hc' ## Testing
  environment = 'dev' ## prod, dev ## Creation of the variable "Environment" 09/03/2020 - MS
flag_overwrite_dates = 'FALSE' ## added to run cmd 3 - 18/09/2019 - JA


## Data path defenition - Py
if environment == 'dev':
  if country_loop == 'INDIA_HEALTH' or country_loop=='PAK_HEALTH' or country_loop=='MY_HEALTH':
    data_path = 'wasbs://smartorder@devrbainesasmartorderhc.blob.core.windows.net/dev' ## New DEV Env
  else:    
    data_path = 'wasbs://smartorder@devrbainesasmartorder.blob.core.windows.net/dev'
elif environment == 'prod':
  if country_loop == 'INDIA_HEALTH' or country_loop=='PAK_HEALTH' or country_loop=='MY_HEALTH':
    data_path = 'wasbs://smartorder@prdrbainesasmartorderhc.blob.core.windows.net/prod'
  else:
    data_path = 'wasbs://smartorder@prdrbainesasmartorder.blob.core.windows.net/prod' ## New Prod Env
else:
  raise ValueError('Undefined Environment! Please select "dev" or "prod" as the environment.')


overwrite_dates = True if flag_overwrite_dates == 'TRUE' else False ## added to run cmd 3 - 18/09/2019 - JA

database_name = database_name + '_' + region_home
  
inter_path = data_path + '/' + country_loop + '/INTERMEDIATE/'
config_path = data_path + '/' + country_loop + '/INPUTS/CONFIG/'
increment_path = data_path + '/' + country_loop + '/INPUTS/INCREMENTAL/'
output_path = data_path + '/' + country_loop + '/OUTPUTS'

if country_loop == 'INDIA_HEALTH':
  blob_path = "wasbs://newspageinputindiahealth@prdrbainesasmartorderhc.blob.core.windows.net"
elif country_loop == 'PAK_HEALTH':
  blob_path = "wasbs://newspageinputpakhealth@prdrbainesasmartorderhc.blob.core.windows.net"
elif country_loop == 'MY_HEALTH':
  blob_path = "wasbs://newspageinputmy@prdrbainesasmartorderhc.blob.core.windows.net"
else:
  blob_path = "wasbs://newspageinputindiahealth@prdrbainesasmartorderhc.blob.core.windows.net"

#Obtained Config file ; To establish connection with SQL and retrive data
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
url = "jdbc:sqlserver://prdrbnesql02.database.windows.net:1433;database=rboneDWH"
table = "["+region_home+"].[vwDS_SO_"+country_home+"_2YR]"
user = "RBOne_DS_Reader"
password = "Qwerty123!"

# COMMAND ----------

# DBTITLE 1,AI Model Automation DEV or PROD blob storage access
if environment == 'dev':
  ## DEV Storage Account
  if country_loop == 'INDIA_HEALTH' or country_loop=='PAK_HEALTH' or country_loop=='MY_HEALTH' :
    storage_account_name = "devrbainesasmartorderhc"
    spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'devrbainekvsmartorderhc', key = 'Blob-devrbainesasmartorderhc-key'))
  else: 
    storage_account_name = "devrbainesasmartorder"
    spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'devrbainekvsmartorder', key = 'Blob-devrbainesasmartorder-key'))

else:
  if country_loop == 'INDIA_HEALTH' or country_loop=='PAK_HEALTH' or country_loop=='MY_HEALTH':
    ## PROD Storage Account
    storage_account_name = "prdrbainesasmartorderhc"
    spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorderhc', key = 'storageaccountsohcpulse'))
  else:
    ## PROD Storage Account
    storage_account_name = "prdrbainesasmartorder"
    spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorder', key = 'blob-prdrbainesasmartorder-key'))


# COMMAND ----------

# DBTITLE 1,NewsPage Blob container - only for DEV
if environment == 'dev':
  if country_loop == 'INDIA_HEALTH' or country_loop=='PAK_HEALTH' or country_loop=='MY_HEALTH':
    ## PROD Storage Account
    storage_account_name_2 = "prdrbainesasmartorderhc"
    spark.conf.set("fs.azure.account.key." + storage_account_name_2 + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorderhc', key = 'storageaccountsohcpulse'))

# COMMAND ----------

# DBTITLE 1,Libraries Import
from functools import reduce
from pyspark.sql.functions import col, when, round, expr, countDistinct, sum, count, concat, lit, min, max, UserDefinedFunction, datediff, explode, year, month, dayofmonth, rank, split, to_date, add_months, regexp_replace, size, collect_set, greatest, least, round, bround, upper, lower, dense_rank, row_number, mean, ceil, udf, date_add, last_day,trim,date_format
from pyspark.sql.window import Window
from re import sub
import pandas as pd
import datetime as Dt
from pyspark.sql.types import *
import pandas as pd
import datetime 
import dateutil as du
from datetime import datetime
import calendar

# COMMAND ----------

if country_loop == 'INDIA_HEALTH':
  pulseData_Path = "wasbs://pulseinputindiahealth@prdrbainesasmartorderhc.blob.core.windows.net"
elif country_loop == 'PAK_HEALTH':
  pulseData_Path = "wasbs://pulseinputpakhealth@prdrbainesasmartorderhc.blob.core.windows.net"
elif country_loop == 'MY_HEALTH':
  pulseData_Path = "wasbs://pulseinputmy@prdrbainesasmartorderhc.blob.core.windows.net"

if country_loop == 'INDIA_HEALTH' or country_loop=='PAK_HEALTH' or country_loop=='MY_HEALTH':
## Automation for picking Latest input file
  fPaths = dbutils.fs.ls(pulseData_Path)
  fPaths = spark.createDataFrame(fPaths)
  fPaths = fPaths.filter(fPaths['name'].contains("RB_"))
  fPaths = fPaths.orderBy('name',ascending=False)
  fPaths_0 = fPaths.collect()[0].path
  print(fPaths_0)
  
  remote_table1 = spark.read.csv(path = fPaths_0, header = True, inferSchema = True, sep = '|')
else:
  remote_table = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", table)\
  .option("user", user)\
  .option("password", password)\
  .load()

# COMMAND ----------

remote_table1=remote_table1.withColumn('Year Month',to_date(col('Year Month').cast('string'),'yyyyMM'))\
                           .withColumn('Year Month',date_format(col('Year Month'),'yyyy/MM'))

# COMMAND ----------

# DBTITLE 1,Remove Leading and Trailing Whitespace
if country_loop=='PAK_HEALTH':
  remote_table1= remote_table1.withColumn('Channel Local', trim(col('Channel Local')))

# COMMAND ----------

if country_loop == 'INDIA_HEALTH' or country_loop=='PAK_HEALTH' or country_loop=='MY_HEALTH':
  remote_table = remote_table1\
  .withColumn("DB Channel", when(col("Distributor Code")=="1000061620", lit("DISTRIBUTOR")).otherwise(col("DB Channel")))


# COMMAND ----------

# DBTITLE 1,Automated Start and End Dates for scheduled executions
# ###################################################################
#                                                                   #
# US 5794 - Automated Start and End Dates for scheduled executions. #
# 2019-Sept-18 - João Arienti.                                      #
#                                                                   #
# ###################################################################


def createDates(start_date, end_date):
  start_day = Dt.datetime.strptime(str(start_date), '%Y%m')
  end_day = Dt.datetime.strptime(str(end_date), '%Y%m')
  final = list(set([str(x.strftime('%Y') + "/" + x.strftime('%m')) for x in pd.date_range(start = start_day, end = end_day)]))
  final.sort()
  return("'" + "','".join(final) + "'")

## If it is not to overwrite start and end dates with the ones from arguments, it should run using dates as below.
## Usually, pipeline executions should use dates as below because they reflect required dates accordingly to scheduling.
if not overwrite_dates:

  # ###########################
  #   Pakistan (Ex.: Set-19)  #
  # ###########################
  # Start Date: 201801        #
  # End Date:   201908        #
  # ###########################

  if country_loop == 'PAKISTAN_HOME':
    start_date = 201701 ## date before was 2018/01 (27/11/2019) - MS
    ## end_date should be the same month for days 30 and 31.
    if Dt.date.today().day == calendar.monthrange(datetime.now().year,datetime.now().month)[1]: ## Added to get a last day of each month (20/09/2019) - MS
      end_date = Dt.date.today().strftime('%Y%m')
    ## end_date should be the month before any other day.  
    else:
      end_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months=-1)
      end_date = end_date_temp.strftime('%Y%m')
      #print(end_date)


  # ###########################
  #  Indonesia (Ex.: Set-19)  #
  # ###########################
  # Start Date: 201601        #
  # End Date:   201908        #
  # ###########################

  if country_loop == 'INDO_HOME': ## removed " or country_loop == 'INDO_HEALTH" (05/03/2020) - MS
#     start_date = 201601
    start_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months = -38) ## Added to get start_date dynamically. 3 years back from end date (27/11/2019) - MS
    start_date = start_date_temp.strftime('%Y%m') ## Added to get start_date dynamically. 3 years back from end date (27/11/2019) - MS
    ## end_date should be the same month for days 30 and 31.
    if Dt.date.today().day == calendar.monthrange(datetime.now().year,datetime.now().month)[1]: ## Added to get a last day of each month (20/09/2019) - MS
      end_date = Dt.date.today().strftime('%Y%m')
    ## end_date should be the month before any other day.  
    else:
      end_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months=-1)
      end_date = end_date_temp.strftime('%Y%m')
      #print(end_date)


  # ###########################
  #    India (Ex.: Set-19)    #
  # ###########################
  # Start Date: 201603        #
  # End Date:   201907        # --> 2 months before.
  # ###########################

  if country_loop == 'INDIA_HOME'  or country_loop=='PAK_HEALTH': 
#     start_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months = -26)  ## ONLY FOR TESTING
    start_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months = -36) ## Added to get start_date dynamically. 3 years back from end date (10/10/2019) - MS..changed start date to -35..KM (8Jan2021)
    start_date = start_date_temp.strftime('%Y%m') ## Added to get start_date dynamically. 3 years back from end date (10/10/2019) - MS
    ## end_date should always be the month before because India runs on  31st, so, in practice, it would be two months before the execution reference month. 
    end_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months = -1) ## Kapil request to change the end date month from -2 to -1 (31/12/2020) - MS
#     end_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months = -3)  ## ONLY FOR TESTING
    end_date = end_date_temp.strftime('%Y%m')
  
if country_loop == 'INDIA_HEALTH' or country_loop=='MY_HEALTH': ##Don;
#     start_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months = -26)  ## ONLY FOR TESTING
    start_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months = -25) ## Added to get start_date dynamically. 3 years back from end date (10/10/2019) - MS..changed start date to -35..KM (8Jan2021)
    start_date = start_date_temp.strftime('%Y%m') ## Added to get start_date dynamically. 3 years back from end date (10/10/2019) - MS
    ## end_date should always be the month before because India runs on  31st, so, in practice, it would be two months before the execution reference month. 
    end_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months = -1) ## Kapil request to change the end date month from -2 to -1 (31/12/2020) - MS
#     end_date_temp = Dt.date.today() + du.relativedelta.relativedelta(months = -3)  ## ONLY FOR TESTING
    end_date = end_date_temp.strftime('%Y%m')


date_value = createDates(start_date, end_date)

# COMMAND ----------

## CHECKS
date_value

# COMMAND ----------

# DBTITLE 1,Function Redefine Names
def redefineNames(dataset, columns = None):
  if (columns != None):
    data = reduce(lambda data, idx: data.withColumnRenamed(data.columns[idx], columns[idx]), range(len(dataset.columns)), dataset)
  else:
    data = dataset
  data = reduce(lambda data, idx: data.withColumnRenamed(data.columns[idx], sub('[\\W]', '_', data.columns[idx].lower())), range(len(data.columns)), data)
  return data

# COMMAND ----------

remote_table = redefineNames(remote_table)
# DB to Region Mapping File
if country_loop == 'PAKISTAN_HOME' or country_loop == 'INDO_HOME' or country_loop == 'INDO_HEALTH':
  DB_List = spark.read.option("header", "true").option("inferschema", "true").csv(config_path + '/DIST_LIST.csv')
## List of all the unique values of DB in DB_List dataframe
  dbList = DB_List.select("DB").dropDuplicates().rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# This filter was on notebook 1. Now, we are filtering on notebook 0 because we need this before notebook 0.1 for India. And it works fine for every other country.
remote_table = remote_table.filter(col('year_month').isin( date_value.replace('\'', '').split(',') ))

# COMMAND ----------

 if country_loop == 'PAKISTAN_HOME':
  ## Beginning of change for December run:  Temporary change on DB code due to data reinstatement on line 5 (25/11/2019) - Anshul
  remote_table = remote_table.withColumn('distributor_code', regexp_replace(remote_table['distributor_code'].cast(StringType()), '341', '161'))
  dt = remote_table.withColumnRenamed("distributor_code","DB")
  dt1 = dt.filter(col('DB').isin(dbList))
  dt1 = dt1.withColumn('store_code',concat(dt1['DB'],lit('_'),dt1['store_code']))
  dt1 = dt1.drop("DB","distributor_name")
  ## End of change for December run: Temporary change on DB code due to data reinstatement on line 5 (25/11/2019) - Anshul

if country_loop == 'INDO_HOME': ## or country_loop == 'INDO_HEALTH'
  dt = remote_table
  dt1 = dt.filter(col('distributor_code').isin(dbList))

if country_loop == 'INDIA_HEALTH':
  dt = remote_table
  dt = dt.withColumn("asm", when(dt["asm"] == 'ASM_CHENNAI-Home', 'ASM_CHENNAI-HOME')\
                           .when(dt["asm"] == 'ASM_HP JK/CHD', 'ASM_HP JKCHD').otherwise(dt["asm"]))\
          .withColumn("asm",upper("asm"))
        ## Added improvement because Fix above was afecting other regions ex: ASM_NUM Metro-HOME (07/10/2019) MS/JV
### Changed on 11/10/2019 Kapil/Rakesh

#   dt = dt.withColumn('restated_store_code',when(dt['restated_store_code'].isin('NA'),dt['outlet_unique_code']).otherwise(dt['restated_store_code'])) - commented as order will run on outlet code-brij(29/09/2022)

  ## Added to run order on outlet code -brij(29/09/2022)
  dt = dt.withColumn('outlet_unique_code',when(dt['outlet_unique_code'].isin('NA'),dt['restated_store_code']).otherwise(dt['outlet_unique_code']))

  dt1 = dt.filter((upper(dt["db_channel"]) == 'DISTRIBUTOR')&(~dt["restated_store_code"].isin(['NA', 'Not Available', 'Pareto']))&(~dt["outlet_unique_code"].isin(['NA', 'Not Available', 'Pareto','Unknown']))) ## Changed for India 2022 both BU combined data

#   dt1 = dt.filter((upper(dt['lob']) == "HEALTH")&(upper(dt["db_channel"]) == 'DISTRIBUTOR')&(~dt["restated_store_code"].isin(['NA', 'Not Available', 'Pareto']))&(~dt["outlet_unique_code"].isin(['NA', 'Not Available', 'Pareto','Unknown']))) 
  
## filter removed in order to run all region in India (18/09/2019) - MS  
### End of changes 11/10/2019
## KM Dec8

# if country_loop == 'INDIA_HEALTH':
#   dt = remote_table
#   dt1 = dt.filter((upper(dt['lob']) == "HEALTH")&(upper(dt["db_channel"]) == 'DISTRIBUTOR')&(~dt["outlet_unique_code"].isin(['NA', 'Not Available', 'Pareto','Unknown'])))

if country_loop == 'PAK_HEALTH':
  dt1 = remote_table
  
if country_loop == 'MY_HEALTH':
  dt1 = remote_table

dt3 = dt1.select("*")

# COMMAND ----------

# DBTITLE 1,getDataFromBlob
## Get the latest file in the blob.
def getDataFromBlob(blob_address, pattern, tableName, separator):
  
  if Dt.date.today().day == calendar.monthrange(Dt.datetime.now().year,Dt.datetime.now().month)[1]: ## Added to get a last day of each month (20/09/2019) - MS
    file_date = Dt.date.today() + du.relativedelta.relativedelta(months=1)
    file_date = file_date.strftime('%Y%m01')
    pattern = pattern + file_date
#     pattern = pattern + Dt.date.today().strftime('%Y%m%d')
#     pattern = pattern + '20221001' ## ONLY for TESTING Purpose (2020/01/03) MS
  else:
#     pattern = pattern + Dt.date.today().strftime('%Y%m01')
    pattern = pattern + Dt.date.today().strftime('%Y%m%d') ### Changed to get the files for same day-Brij
#     pattern = pattern + '20221001' ## ONLY for TESTING Purpose (2020/01/03) MS


  print(pattern) 
  list_of_files = dbutils.fs.ls(blob_path)
  lof = []
  for name in list_of_files:
    if pattern in name[1]:
      lof.append(name[1])
  lof.sort()
  latest_file = lof[len(lof) - 1]
  spark.read.csv(blob_address + '/' + latest_file, sep = separator, header = True).createOrReplaceTempView(tableName)
  return(spark.read.csv(blob_address + '/' + latest_file, sep = separator, header = True))

# COMMAND ----------

# DBTITLE 1,Map Correct ASM and DB Mapping
if country_loop=='INDIA_HEALTH':
  ## For Updating the Distributor_code from POS_data- Brij (2022/11/18)
  POS_data = getDataFromBlob(blob_address = blob_path, pattern = 'RB01_PARTNERPOS_', tableName ='RB01_PARTNERPOS', separator = '|')
  POS_data = POS_data.filter((col('SalesRepCode').isNotNull()) & (col('SalesRepCode')!="") & (col('PartnerCode').isNotNull()) & (col('POSCode').isNotNull()))
  POS_data =POS_data.filter(col('POSStatus')==1) ## Added on request of kapil-11/06/2021
  POS_data=POS_data.select('POSCode','PartnerCode').distinct().withColumnRenamed('POSCode','outlet_unique_code')


  # For Updating ASM name -Brij (2022/11/18)
  asm_map = spark.read.csv(path = data_path + "/INDIA_HEALTH/TEMP/DB ASM Mapping.csv", header = True, inferSchema = True, sep = ',')
  asm_map=asm_map.withColumnRenamed('RB_CUST_CODE','PartnerCode')\
                  .withColumnRenamed('CUSTOM_ATTR14','New_ASM')\
                  .withColumn('New_ASM',upper(col('New_ASM')))\
                  .withColumn('New_ASM',when(col('New_ASM') == 'ASM_HP JK/CHD', 'ASM_HP JKCHD').otherwise(col('New_ASM')))\
                  .select('PartnerCode','New_ASM').distinct()\
                  .drop_duplicates(subset=['PartnerCode'])

  ## cols order kept to be same for no error in notebook 0.1
  req_cols_order=dt3.columns

  ## Updating the 'distributor_code' and 'asm' as it is not correct in pulse data
  dt4 = dt3.withColumn('outlet_unique_code',regexp_replace(col('outlet_unique_code'),'SF',""))\
                      .join(POS_data,on=['outlet_unique_code'],how='left')\
                      .withColumn('distributor_code',when(col('PartnerCode').isNull(),col('distributor_code')).otherwise(col('PartnerCode')))\
                      .join(asm_map.withColumnRenamed('PartnerCode','distributor_code'),on=['distributor_code'],how='left')\
                      .withColumn('asm',when(col('New_ASM').isNotNull(),col('New_ASM')).otherwise(col('asm')))\
                      .select(req_cols_order).distinct()

  ## Take all db from data and db_asm_mapping files 
  df_asm_map=dt4.select('distributor_code','asm').distinct()\
                .withColumnRenamed('distributor_code','PartnerCode')\
                .withColumnRenamed('asm','New_ASM')\

  db_asm_map=asm_map.unionByName(df_asm_map)\
                    .filter(col('New_ASM').isNotNull())\
                    .drop_duplicates(subset=['PartnerCode'])
  
else:
  dt4 = dt3
  db_asm_map=dt4.select('distributor_code','asm').distinct()\
                .withColumnRenamed('distributor_code','PartnerCode')\
                .withColumnRenamed('asm','New_ASM')\

  # db_asm_map.display()

# COMMAND ----------

## Relevant
spark.sql('create database if not exists ' + database_name)
spark.sql('drop table if exists ' + database_name + '.vwDS_SO_' + country_home + '_2YR')
dt4.write.mode('overwrite').saveAsTable(database_name + '.vwDS_SO_' + country_home + '_2YR')

#rt2.write.mode('overwrite').saveAsTable(database_name + '.vwDS_SO_' + country_home + '_2YR')
# Took almost 4 min. to write near 6 million rows.

###  Save Region_Mapping File
spark.sql('drop table if exists ' + database_name + '.ASM_MAP_' + country_loop)
db_asm_map.write.mode('overwrite').saveAsTable(database_name + '.ASM_MAP_' + country_loop)

# COMMAND ----------

# India needs a second copy of raw data. The first one will be transformed on notebook 0.1 (specific to India) and this data will be used on the following notebooks.
# But, India has a last notebook, Notebook 9.1 - India, that needs also a replica of raw data. That's why we are doing a copy of the data only for India.
# Instead of writing a new table from Spark DataFrame, it is faster to just copy the table.

if country_loop == 'INDIA_HOME' or country_loop == 'INDIA_HEALTH' or country_loop == 'PAK_HEALTH' or country_loop == 'MY_HEALTH':
  spark.sql('drop table if exists ' + database_name + '.vwDS_SO_' + country_home + '_2YR_raw_data')
  spark.sql('create table ' + database_name + '.vwDS_SO_' + country_home + '_2YR_raw_data' + ' as select * from ' + database_name + '.vwDS_SO_' + country_home + '_2YR')
# Took 1 min. to write near 6 millions rows.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC -Data Format
# MAGIC 
# MAGIC -col outlet_category_name is not there , new col is there i.e 'channel_local'
# MAGIC 
# MAGIC -