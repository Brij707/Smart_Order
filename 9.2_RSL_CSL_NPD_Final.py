# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Changelog

# COMMAND ----------

# MAGIC %md ## Notes
# MAGIC 
# MAGIC - Combination means any unique set of [State, Class, Store_type, Category_Desc]
# MAGIC - Any intermediate dataframe named with suffix '_s' contains seasonal data, similary '_ns' means non seasonal

# COMMAND ----------

# DBTITLE 1,Define Variables and paths - R
# MAGIC %r
# MAGIC region_loop = tryCatch(getArgument("VAR_REGION"), error = function(e) {return('ASM_SOUTH_UP')}) ## ASM_CHENNAI-HOME
# MAGIC focus_loop = tryCatch(getArgument("VAR_FOCUS"), error = function(e) {return('HEALTH')})
# MAGIC country_loop = tryCatch(getArgument("VAR_COUNTRY"), error = function(e) {return('INDIA_HEALTH')})
# MAGIC database_name = tryCatch(getArgument("VAR_DATABASE_NAME"), error = function(e) {return('smart_order_dev_hc')}) ## Testing
# MAGIC environment = tryCatch(getArgument('VAR_ENVIRONMENT'), error = function(e) {return('dev')}) ## prod, dev ## Creation of the variable "Environment" 09/03/2020 - MS
# MAGIC 
# MAGIC ## Data path definition
# MAGIC if (environment == 'dev'){
# MAGIC 	if (country_loop == 'INDIA_HEALTH'){
# MAGIC 		data_path <- 'wasbs://smartorder@devrbainesasmartorderhc.blob.core.windows.net/dev'
# MAGIC 	}else{
# MAGIC 		data_path <- 'wasbs://smartorder@devrbainesasmartorder.blob.core.windows.net/dev'}
# MAGIC } else{ if (environment == 'prod'){
# MAGIC   if (country_loop == 'INDIA_HEALTH'){
# MAGIC     data_path <- 'wasbs://smartorder@prdrbainesasmartorderhc.blob.core.windows.net/prod'
# MAGIC     } else{
# MAGIC   data_path <- 'wasbs://smartorder@prdrbainesasmartorder.blob.core.windows.net/prod' ## New Prod Env
# MAGIC     }
# MAGIC } else{
# MAGIC   stop('Undefined Environment! Please select "dev" or "prod" as the environment.')
# MAGIC }
# MAGIC }
# MAGIC inter_path <- paste0(data_path,"/",country_loop,"/INTERMEDIATE")
# MAGIC config_path <- paste0(data_path,"/",country_loop,"/INPUTS/CONFIG")
# MAGIC increment_path <- paste0(data_path,"/",country_loop,"/INPUTS/INCREMENTAL")
# MAGIC output_path <- paste0(data_path,"/",country_loop,"/OUTPUTS")

# COMMAND ----------

# DBTITLE 1,Define variables, paths-Python
try:
  country_loop   = getArgument('VAR_COUNTRY')
  region_loop = getArgument('VAR_REGION')
  focus_loop = getArgument("VAR_FOCUS")
  database_name = getArgument('VAR_DATABASE_NAME')
  environment = getArgument('VAR_ENVIRONMENT')
  
except:
  country_loop = 'INDIA_HEALTH' 
  region_loop = 'ASM_SOUTH_UP' 
  focus_loop = 'HEALTH'
#   database_name = 'smart_order' ## Production
  database_name = 'smart_order_dev_hc' ## Testing
  environment = 'dev' 

## Data path definition - Py
if environment == 'dev':
  if country_loop == 'INDIA_HEALTH':
    data_path = 'wasbs://smartorder@devrbainesasmartorderhc.blob.core.windows.net/dev'
  else:    
    data_path = 'wasbs://smartorder@devrbainesasmartorder.blob.core.windows.net/dev'
elif environment == 'prod':
  if country_loop == 'INDIA_HEALTH':
    data_path = 'wasbs://smartorder@prdrbainesasmartorderhc.blob.core.windows.net/prod'
  else:
    data_path = 'wasbs://smartorder@prdrbainesasmartorder.blob.core.windows.net/prod'
else:
  raise ValueError('Undefined Environment! Please select "dev" or "prod" as the environment.')
  

database_name = database_name + '_' + country_loop + '_' + region_loop.replace('-', '_').replace(' ', '_') + '_' + focus_loop ## added to fix issue " " and "-"

inter_path = data_path + '/' + country_loop + '/INTERMEDIATE'
print(inter_path)
config_path = data_path + '/' + country_loop + '/INPUTS/CONFIG'
print(config_path)
increment_path = data_path + '/' + country_loop + '/INPUTS/INCREMENTAL'
print(increment_path)
output_path = data_path + '/' + country_loop + '/OUTPUTS'
print(output_path)

if country_loop != 'INDIA_HEALTH':
  blob_path = "wasbs://newspageinputindiahealth@prdrbainesasmartorder.blob.core.windows.net"
else:
  blob_path = "wasbs://newspageinputindiahealth@prdrbainesasmartorderhc.blob.core.windows.net"

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

# DBTITLE 1,NewsPage Blob container - only for DEV
if environment == 'dev':
  if country_loop == 'INDIA_HEALTH':
    ## PROD Storage Account
    storage_account_name_2 = "prdrbainesasmartorderhc"
    spark.conf.set("fs.azure.account.key." + storage_account_name_2 + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorderhc', key = 'storageaccountsohcpulse'))
  else:
    ## PROD Storage Account
    storage_account_name_2 = "prdrbainesasmartorder"

    spark.conf.set("fs.azure.account.key." + storage_account_name_2 + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorder', key = 'blob-prdrbainesasmartorder-key'))

# COMMAND ----------

# DBTITLE 1,R libraries Import
# MAGIC %r
# MAGIC library(SparkR)
# MAGIC library(data.table)
# MAGIC library(lubridate)
# MAGIC library(dplyr)

# COMMAND ----------

# DBTITLE 1,Python libraries Import
import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.functions import upper, col, regexp_replace, countDistinct, ceil, col, substring, months_between, to_date
from pyspark.sql.types import DateType
import pyspark.sql.functions as F
from re import sub
import datetime
import dateutil.relativedelta
import datetime as dt
import dateutil as du
import calendar

from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Create getDataFromBlob function to get data from blob
## Get the latest file in the blob.
  
def getDataFromBlob(blob_address, pattern, tableName, separator):
  
  if dt.date.today().day == calendar.monthrange(dt.datetime.now().year,dt.datetime.now().month)[1]: ## Added to get a last day of each month (20/09/2019) - MS
    file_date = dt.date.today() + du.relativedelta.relativedelta(months=1)
    file_date = file_date.strftime('%Y%m01')
    pattern = pattern + file_date
#    pattern = pattern + '20201125' ## ONLY for TESTING Purpose (2020/01/03) MS
  else:
    pattern = pattern + dt.date.today().strftime('%Y%m01')
#    pattern = pattern + '20201125' ## ONLY for TESTING Purpose (2020/01/03) MS


    
  list_of_files = dbutils.fs.ls(blob_path)
  lof = []
  for name in list_of_files:
    if pattern in name[1]:
      lof.append(name[1])
  lof.sort()
  latest_file = lof[len(lof) - 1]
  spark.read.csv(blob_address + '/' + latest_file, sep = separator, header = True).registerTempTable(tableName)
  return(spark.read.csv(blob_address + '/' + latest_file, sep = separator, header = True))

# COMMAND ----------

## To avoid SparkUpgradeException, due to the upgrading of Spark 3.0
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY") 

# COMMAND ----------

# DBTITLE 1,Files import from India's Blob
POS_data = getDataFromBlob(blob_address = blob_path, pattern = 'RB01_PARTNERPOS_', tableName ='RB01_PARTNERPOS', separator = '|')
SKU_Grouping = getDataFromBlob(blob_address = blob_path, pattern = 'SMARTGROUP_', tableName ='SMARTGROUP', separator = '|')
visits = getDataFromBlob(blob_address = blob_path, pattern = 'RB01_VISITPLAN_', tableName ='PLANNED_VISITS', separator = '|')

# COMMAND ----------

# DBTITLE 1,Import data
prev_data = spark.sql('select * from ' + database_name + '.PREVIOUS_DATA')
prev_data = prev_data.withColumn('Category_Desc', trim(col('Category_Desc')))
# prev_data = prev_data.withColumn('Bucket', upper(col('Bucket')))

## latest 6 months sales data
SKU_mapping = spark.sql('select * from ' + database_name + '.ALLGROUPSTORE_CLEANED')  
SKU_mapping = SKU_mapping.withColumnRenamed('CATEGORY_DESC','Category_Desc')\
                          .withColumn('Category_Desc', trim(col('Category_Desc')))\
#                           .withColumn('CLASSIFICATION', upper(col('CLASSIFICATION')))

## RSL and CSL recommendation multiplier
reco_limit_df = spark.read.csv(path = increment_path + '/rsl_csl_factor.csv', header = True, inferSchema = True) 

buckets = spark.read.csv(path = config_path + '/BUCKETS_CUTOFF.csv', header = True, inferSchema = True) 
buckets = buckets.withColumn('CLASS', regexp_replace(col("CLASS"), " ", ""))

sg_sku_mapping = spark.read.csv(path = increment_path +'/'+ region_loop + '_' + focus_loop + '_SKU_GROUPING.csv', header = True, inferSchema = True).select(['Product SKU Code', 'Supergroup']).withColumnRenamed('Product SKU Code','Product_SKU_Code')

category_cutoff = spark.read.csv(path = config_path + '/CONFIGURATION.csv', header = True, inferSchema = True)

seasonal_map = spark.read.csv(path = config_path +'/'+ region_loop + '_' + focus_loop + '_SEASONAL_MONTHS.csv', header = True, inferSchema = True)
seasonal_map = seasonal_map.withColumnRenamed('Months', 'month').withColumnRenamed("Brand", "Category_Desc")
seasonal_map = seasonal_map.withColumn('Category_Desc', trim(col("Category_Desc")))


nd_input = spark.read.csv(path = increment_path +'/'+ region_loop + '_' + focus_loop + '_ND_INPUT.csv', header = True, inferSchema = True)
# nd_input = nd_input.withColumn('Classification', upper(col('Classification')))

to_factor = spark.sql('select * from ' + database_name + '.TO_FACTOR')

## Incentive File 
incentive_df = spark.read.csv(path = increment_path + '/'+ 'Incentive.csv', header = True, inferSchema = True)

## Price data for Focus & NPD
price_df = spark.read.csv(path = increment_path + '/'+ 'Price.csv', header = True, inferSchema = True)

## Pharma SKUs to be filtered out
pharma_skus = spark.read.csv(path = increment_path + '/'+ 'PHARMA_BRAND.csv', header = True, inferSchema = True)

# COMMAND ----------

# Order month
order_month = SKU_mapping.select('YEAR_MONTH').withColumn('ANALYSIS_MONTHS', to_date(concat(col('YEAR_MONTH'), lit('/01')), 'yyyy/MM/dd').cast('date')).select('ANALYSIS_MONTHS').agg(max(col('ANALYSIS_MONTHS')).alias('ORDER_MONTH')).withColumn('ORDER_MONTH', add_months(col('ORDER_MONTH'), 1)).withColumn('ORDER_MONTH', month('ORDER_MONTH')).select('ORDER_MONTH').toPandas()['ORDER_MONTH'].tolist()

## Filter seasonal month data for order month
seasonal_map = seasonal_map.filter(col('month') == order_month[0])

# COMMAND ----------

### Parameterized file for multiplier
split_line = split(reco_limit_df['Current Lines (Avg Monthly)'], ' ')
dr_processed = reco_limit_df.withColumn('line_range_min', split_line.getItem(0).cast('int'))\
                 .withColumn('line_range_max', split_line.getItem(2).cast('int'))\
                 .withColumn('line_range_max', when(col('line_range_max').isNull(), 99999)\
                             .otherwise(col('line_range_max')))\
                .withColumnRenamed('Store_type','class')

dr_processed = dr_processed.withColumn('line_range_max', 
                                       when( (col('class')=='S')&(col('line_range_min')==0),  5)\
                                       .otherwise(col('line_range_max') ))

# COMMAND ----------

# Calculating current date
order_month2 = SKU_mapping.withColumn('ANALYSIS_MONTHS', to_date(concat(col('YEAR_MONTH'), lit('/01')),'yyyy/MM/dd').cast('date'))\
                .select('ANALYSIS_MONTHS').agg(max(col('ANALYSIS_MONTHS')).alias('ORDER_MONTH'))\
                .withColumn('ORDER_MONTH', add_months(col('ORDER_MONTH'), 1))\
                
Current_date = order_month2.select('ORDER_MONTH').toPandas()['ORDER_MONTH'].tolist()
Current_date

# COMMAND ----------

# DBTITLE 1,Store Type mapping file
store_type_map = prev_data.select('Store_Code','Bucket').drop_duplicates().withColumnRenamed('Store_Code','StoreCode').withColumnRenamed('Bucket','Type')
split_char = split(store_type_map['Type'], '-')
store_type_map  = store_type_map.withColumn('Type', upper(split_char.getItem(0).cast('string')))
store_type_map = store_type_map.withColumn('Type', regexp_replace(col("Type"), " ", ""))

# COMMAND ----------

# Add '11mon' and '13mon'  columns to filter 3MLY Seasonal data
df_all = prev_data.withColumn("month", substring(col("Year_Month"), -2, 2))\
             .withColumn('ANALYSIS_MONTHS', to_date(concat(col('YEAR_MONTH'), lit('/01')), 'yyyy/MM/dd').cast('date'))\
             .withColumn('Store_type', upper(col('Bucket')).alias('Bucket'))\
             .withColumnRenamed('Store_Code','Outlet_Unique_Code')\
             .withColumnRenamed('Region','State')\
             .withColumnRenamed('ValueSales','Secondary_Sales_Amount')\
             .withColumnRenamed('VolSales','Secondary_Sales_Qty_Eaches')\
             .withColumn('Current_date',lit(Current_date[0]))\
             .withColumn('11mon', add_months(col('Current_date'), -11))\
             .withColumn('13mon', add_months(col('Current_date'), -13))\
             .join(sg_sku_mapping, on='Product_SKU_Code', how='inner')\
             .drop_duplicates()


df_6Mon_latest = SKU_mapping.withColumn("month", substring(col("YEAR_MONTH"), -2, 2))\
                         .withColumnRenamed('PRODUCT_SKU_CODE', 'Product_SKU_Code').withColumnRenamed('SECONDARY_SALES_AMOUNT', 'Secondary_Sales_Amount')\
                         .withColumnRenamed('SECONDARY_SALES_QTY_EACHES', 'Secondary_Sales_Qty_Eaches').withColumnRenamed('YEAR_MONTH', 'Year_Month')\
                         .withColumnRenamed('OUTLET_UNIQUE_CODE','Outlet_Unique_Code')
                         
df_6Mon_latest = df_6Mon_latest.join(sg_sku_mapping, on='Product_SKU_Code', how='inner').drop_duplicates()
df_6Mon_latest = df_6Mon_latest.join(store_type_map.withColumnRenamed('StoreCode','Outlet_Unique_Code').withColumnRenamed('Type','Store_type'),on=['Outlet_Unique_Code'],how='inner')

# COMMAND ----------

# Filter seasonal data
df_month_non_seasonal = df_6Mon_latest.select('Outlet_Unique_Code','Product_SKU_Code','Year_Month','STATE','Category_Desc','Secondary_Sales_Amount'
                                              ,'Secondary_Sales_Qty_Eaches','NUM_INVOICES','month','Supergroup','Store_type')

df_month_seasonal = df_all.filter((col("ANALYSIS_MONTHS") >= col("13mon")) & (col("ANALYSIS_MONTHS") <= col("11mon")))\
                                .join(seasonal_map.drop('month'), on='Category_Desc',how='inner')

df_month_seasonal = df_month_seasonal.select('Outlet_Unique_Code','Product_SKU_Code','Year_Month','State','Category_Desc','Secondary_Sales_Amount'
                                            ,'Secondary_Sales_Qty_Eaches','NUM_INVOICES','month','Supergroup','Store_type')

df_month_seasonal_select = df_month_seasonal.select('Outlet_Unique_Code','Supergroup').drop_duplicates()

# COMMAND ----------

### Grouping Seaonal supergroups in category
df_month_seasonal_category = df_month_seasonal.groupby(['Category_Desc', "Outlet_Unique_Code", "Supergroup"])\
                                              .agg(countDistinct("month").alias('Distinct_month')
                                                   ,sum("Secondary_Sales_Qty_Eaches").alias('Secondary_Sales_Qty_Eaches')
                                                   ,max("Year_Month").alias('max_year_month')
                                                   ,sum("Secondary_Sales_Amount").alias('Secondary_Sales_Amount')
                                                   ,sum("NUM_INVOICES").alias('NUM_INVOICES'))

### Taking one supergroup per category for seasonal
df_supergroup_ss_all = df_month_seasonal_category.withColumn("supergroup_rank", 
                                rank().over(Window.partitionBy(['Category_Desc', "Outlet_Unique_Code"])\
                                .orderBy(F.col("Secondary_Sales_Amount").desc(),F.col("Distinct_month").desc(),
                                         F.col("Secondary_Sales_Qty_Eaches").desc(),F.col("Supergroup").desc())))\

df_supergroup_ss = df_supergroup_ss_all.filter(col('supergroup_rank') == 1)\
                                           .drop('Category_Desc','supergroup_rank')

# Assuming Supergroup as lowest level, aggreagte at Outlet and Supergroup level
df_supergroup_ns = df_month_non_seasonal.groupby(['Outlet_Unique_Code','Supergroup'])\
                            .agg(countDistinct("month").alias('Distinct_month')
                                ,sum("Secondary_Sales_Qty_Eaches").alias('Secondary_Sales_Qty_Eaches'),max("Year_Month").alias('max_year_month')
                                ,sum("Secondary_Sales_Amount").alias('Secondary_Sales_Amount')
                                 ,sum("NUM_INVOICES").alias('NUM_INVOICES'))

# COMMAND ----------

### Calculating RFM
df_recency = df_supergroup_ns.withColumn('max_year_month', to_date(concat(col('max_year_month'), lit('/01')), 'yyyy/MM/dd').cast('date'))\
                          .withColumn('Current_Date',lit(Current_date[0]))\
                          .withColumn("month_diff", months_between(col("Current_Date"),col("max_year_month")))\
                          .withColumn("Recency",
                                 when((col("month_diff") == 1), 4)\
                                 .when((col("month_diff") == 2), 3)\
                                 .when(((col("month_diff") == 3) | (col("month_diff") == 4)), 2)\
                                 .otherwise(1))\
                          .select('Outlet_Unique_Code','Supergroup','Recency') 

df_frequency = df_supergroup_ns.select('Outlet_Unique_Code','NUM_INVOICES','Supergroup')\
                        .withColumn("Frequency",
                                  when((col("NUM_INVOICES") > 15), 4)\
                                 .when((col("NUM_INVOICES") >= 11), 3)\
                                 .when((col("NUM_INVOICES") >= 6), 2)\
                                 .otherwise(1))\
                    .select('Outlet_Unique_Code','Supergroup','Frequency')   


grp_window = Window.partitionBy('Outlet_Unique_Code')
magic_percentile1 = F.expr('percentile_approx(Secondary_Sales_Amount, 0.25)')
magic_percentile2 = F.expr('percentile_approx(Secondary_Sales_Amount, 0.50)')
magic_percentile3 = F.expr('percentile_approx(Secondary_Sales_Amount, 0.75)')

## Montetary - Quantisation of Secondary_Sales_Amount
df_monetary = df_supergroup_ns.select('Outlet_Unique_Code','Secondary_Sales_Amount','Supergroup')\
                        .withColumn('25percentile', magic_percentile1.over(grp_window))\
                        .withColumn('50percentile', magic_percentile2.over(grp_window))\
                        .withColumn('75percentile', magic_percentile3.over(grp_window))\
                        .withColumn("monetary",
                                 when((col("Secondary_Sales_Amount") >= col("75percentile")), 4)\
                                 .when((col("Secondary_Sales_Amount") < col("75percentile")) & (col("Secondary_Sales_Amount") >= col("50percentile")), 3)\
                                 .when((col("Secondary_Sales_Amount") < col("50percentile")) & (col("Secondary_Sales_Amount") >= col("25percentile")), 2)\
                                 .otherwise(1))

# Adding RFM Score
df_rfm = df_supergroup_ns.join(df_recency.select('Outlet_Unique_Code','Supergroup','Recency'), on =['Outlet_Unique_Code','Supergroup'],how='inner')\
                 .join(df_frequency.select('Outlet_Unique_Code','Supergroup','Frequency'), on =['Outlet_Unique_Code','Supergroup'], how='inner')\
                 .join(df_monetary.select('Outlet_Unique_Code','Supergroup','monetary'), on =['Outlet_Unique_Code','Supergroup'], how='inner')\
                 .drop_duplicates()\
                 .withColumn('rfm_sum', col('Recency')+col('Frequency')+col('monetary'))

# COMMAND ----------

### Dividing data into 3 priorirtes i) >=4 month, ii) Rest of the seasonal iii) <4 months

df_priority = df_rfm.withColumn('Seasonality_flag', lit('NS'))\
                    .withColumn('priority', F.when(F.col('Distinct_month')>=4, 1).otherwise(3))

df_reco_rfm = df_priority.filter(col('Distinct_month')<4)\
                          .withColumn("individual_rank", 
                                rank().over(Window.partitionBy(["Outlet_Unique_Code"])\
                                .orderBy(F.col("rfm_sum").desc(), F.col("Secondary_Sales_Amount").desc())))\
                                .join(df_month_seasonal_select, on=['Outlet_Unique_Code', 'Supergroup'],how='left_anti')

df_reco_non_seasonal = df_priority.filter(col('Distinct_month')>=4)\
                                  .withColumn("individual_rank", 
                                rank().over(Window.partitionBy(["Outlet_Unique_Code"])\
                                .orderBy(F.col("rfm_sum").desc(), F.col("Secondary_Sales_Amount").desc())))

df_reco_seasonal = df_supergroup_ss.withColumn('Recency',lit(0))\
                                  .withColumn('Frequency',lit(0))\
                                  .withColumn('monetary',lit(0))\
                                  .withColumn('rfm_sum',lit(0))\
                                  .withColumn('Seasonality_flag', lit('S'))\
                                  .withColumn('priority',lit(2))\
                                  .withColumn("individual_rank", 
                                rank().over(Window.partitionBy(["Outlet_Unique_Code"])\
                                .orderBy(F.col("Secondary_Sales_Amount").desc(), F.col("Distinct_month").desc(), F.col("Secondary_Sales_Qty_Eaches").desc())))

# checking for duplicates
df_reco_non_seasonal_store_sg = df_reco_non_seasonal.select('Outlet_Unique_Code', 'Supergroup').drop_duplicates()
df_reco_seasonal = df_reco_seasonal.join(df_reco_non_seasonal_store_sg, on = ['Outlet_Unique_Code', 'Supergroup'], how='left_anti')

df_reco_seasonal_store_sg = df_reco_seasonal.select('Outlet_Unique_Code', 'Supergroup').drop_duplicates()
df_reco_rfm = df_reco_rfm.join(df_reco_seasonal_store_sg,  on = ['Outlet_Unique_Code', 'Supergroup'], how='left_anti' )

# COMMAND ----------

# Final supergroup rank
final_reco = df_reco_rfm.union(df_reco_non_seasonal).union(df_reco_seasonal)

final_rank = final_reco.withColumn("final_rank", 
                                rank().over(Window.partitionBy(["Outlet_Unique_Code"])\
                                .orderBy(F.col('priority'), F.col('individual_rank'))))

# COMMAND ----------

### Calculation at store level(p6m) and class of Store
df_p6M = df_6Mon_latest.groupby(['Outlet_Unique_Code','month']).agg(countDistinct("Supergroup").alias('Distinct_Supergroups'))\
                  .groupby('Outlet_Unique_Code').agg(sum('Distinct_Supergroups').alias('Distinct_Supergroups'),countDistinct("month").alias('Distinct_month'))\
                  .withColumn('P6M_avg', col('Distinct_Supergroups')/col('Distinct_month'))\
                  .select('Outlet_Unique_Code','P6M_avg')

split_class = split(df_6Mon_latest['CLASSIFICATION'], '_')
ds_store_type = df_6Mon_latest.withColumn('class',split_class.getItem(0).cast('string'))\
                              .select('Outlet_Unique_Code','class')\
                              .join(df_p6M, on='Outlet_Unique_Code',how='inner')\
                              .drop_duplicates()


# getting CSL and RSL reco lines
df_store_param = ds_store_type.join(dr_processed, on='class', how='left')\
                          .drop_duplicates()\
                          .filter((col('P6M_avg') >= col('line_range_min')) & (col('P6M_avg') < col('line_range_max') + 1))\
                          .withColumn("RSL_reco_line", ceil(col("P6M_avg")*col("RSL_Recos")))\
                          .withColumn("CSL_reco_line", ceil(col("P6M_avg")*col("CSL_Recos")))

# COMMAND ----------

# Merging multiplier factor from business file and selecting RSL lines
all_ranks = final_rank.join(df_store_param.select('Outlet_Unique_Code','RSL_reco_line', 'CSL_reco_line'),on='Outlet_Unique_Code',how='inner')\
                          .drop_duplicates()

### Selecting lines based on rank
df_final_reco = final_rank.join(df_store_param.select('Outlet_Unique_Code','RSL_reco_line', 'CSL_reco_line'),on='Outlet_Unique_Code',how='inner')\
                          .drop_duplicates()\
                          .filter(col('final_rank') <= col('RSL_reco_line'))

# COMMAND ----------

### Sales Qty average at supergroup level
df_group_month_avg = df_6Mon_latest.groupby(['Outlet_Unique_Code','Supergroup'])\
                                           .agg(sum("Secondary_Sales_Qty_Eaches").alias('Secondary_Sales_Qty_Eaches')
                                                ,countDistinct('month').alias('Distinct_month'))\
                                           .withColumn("Qty_6_avg", col("Secondary_Sales_Qty_Eaches")/col('Distinct_month'))\
                                           .drop('Secondary_Sales_Qty_Eaches','Distinct_month')

df_RSL_recos_volume = df_final_reco.join(df_group_month_avg, on=['Outlet_Unique_Code','Supergroup'], how='left').drop_duplicates()

# COMMAND ----------

# Bringing 'State','Store_type','Category_Desc' and 'class' for each type of Store

df_RSL_recos_volume_ns = df_RSL_recos_volume.filter(col('Seasonality_flag')=='NS').join(ds_store_type.select('Outlet_Unique_Code','class'),
                                                       on=['Outlet_Unique_Code'], how='inner').drop_duplicates()\
                                            .join(df_month_non_seasonal.select('Outlet_Unique_Code','State','Store_type','Category_Desc','Supergroup'),
                                                       on=['Outlet_Unique_Code','Supergroup'], how='inner').drop_duplicates()

df_RSL_recos_volume_s = df_RSL_recos_volume.filter(col('Seasonality_flag')=='S').join(ds_store_type.select('Outlet_Unique_Code','class'),
                                                       on=['Outlet_Unique_Code'], how='inner').drop_duplicates()\
                                            .join(df_month_seasonal.select('Outlet_Unique_Code','State','Store_type','Category_Desc','Supergroup'),
                                                       on=['Outlet_Unique_Code','Supergroup'], how='inner').drop_duplicates()

df_RSL_recos_volume_combination = df_RSL_recos_volume_ns.union(df_RSL_recos_volume_s).drop_duplicates()

# COMMAND ----------

# DBTITLE 1,Calculating Avg_price at Combination and Supergroup Level for Suggestion_Value calculation
df_combination_price = df_month_non_seasonal.select('Outlet_Unique_Code','State','Store_type','Category_Desc','Supergroup','Secondary_Sales_Amount',
                                                           'Secondary_Sales_Qty_Eaches')\
                                                    .join(ds_store_type.select('Outlet_Unique_Code','class'),
                                                                   on=['Outlet_Unique_Code'], how='inner').drop_duplicates()

df_combination_suggested_price_ns = df_combination_price.groupby('State','Store_type','class','Supergroup')\
                                                                .agg(sum('Secondary_Sales_Amount').alias('Secondary_Sales_Amount')
                                                                     ,sum('Secondary_Sales_Qty_Eaches').alias('Secondary_Sales_Qty_Eaches'))\
                                                                .withColumn('Average_Price', col('Secondary_Sales_Amount')/col('Secondary_Sales_Qty_Eaches'))\
                                                                .drop('Secondary_Sales_Amount','Secondary_Sales_Qty_Eaches')

df_combination_suggested_price_ns = df_combination_suggested_price_ns.withColumn('Seasonality_flag', lit('NS'))

df_combination_price_s = df_month_seasonal.select('Outlet_Unique_Code','State','Store_type','Category_Desc','Supergroup','Secondary_Sales_Amount',
                                                               'Secondary_Sales_Qty_Eaches')\
                                                        .join(ds_store_type.select('Outlet_Unique_Code','class'),
                                                                       on=['Outlet_Unique_Code'], how='inner').drop_duplicates()

df_combination_suggested_price_s = df_combination_price_s.groupby('State','Store_type','class','Supergroup')\
                                                                .agg(sum('Secondary_Sales_Amount').alias('Secondary_Sales_Amount')
                                                                     ,sum('Secondary_Sales_Qty_Eaches').alias('Secondary_Sales_Qty_Eaches'))\
                                                                 .withColumn('Average_Price', col('Secondary_Sales_Amount')/col('Secondary_Sales_Qty_Eaches'))\
                                                                 .drop('Secondary_Sales_Amount','Secondary_Sales_Qty_Eaches')

df_combination_suggested_price_s = df_combination_suggested_price_s.withColumn('Seasonality_flag', lit('S'))

df_combination_suggested_price = df_combination_suggested_price_ns.union(df_combination_suggested_price_s.select(df_combination_suggested_price_ns.columns))\
                                                      .drop_duplicates()

# COMMAND ----------

### Bringing SKUs correspoding to each Supergroup

# the number of stores in SKU is selling
df_group_outlet = df_month_non_seasonal.groupby(['Supergroup','Product_SKU_Code'])\
                       .agg(countDistinct('Outlet_Unique_Code').alias('Distinct_Outlet_Unique_Code'))

df_group_combination = df_month_non_seasonal.join(ds_store_type.select('Outlet_Unique_Code','class'),on='Outlet_Unique_Code',how='inner')

# get sku sales volume and amount
df_group_sku = df_group_combination.groupby(['State','class','Category_Desc','Store_type', 'Outlet_Unique_Code','Supergroup','Product_SKU_Code'])\
                                             .agg(sum("Secondary_Sales_Qty_Eaches").alias('Secondary_Sales_Qty_Eaches')
                                                  ,sum("Secondary_Sales_Amount").alias('Secondary_Sales_Amount'))\
                                             .join(df_group_outlet,on=['Supergroup','Product_SKU_Code'], how='left')\
                                             .drop_duplicates()

# ranking SKUs and selecting top ranked
df_sku_rank = df_group_sku.withColumn("sku_rank", 
                                    rank().over(Window.partitionBy(['State','class','Category_Desc','Store_type', "Outlet_Unique_Code", "Supergroup"])\
                                    .orderBy(F.col("Secondary_Sales_Amount").desc(),F.col("Distinct_Outlet_Unique_Code").desc(),
                                             F.col("Secondary_Sales_Qty_Eaches").desc())))\
                                    .filter(col('sku_rank') == 1)

df_sku_rank = df_sku_rank.withColumn('Seasonality_flag', lit('NS') )

# COMMAND ----------

###### SKU ranking for seasonal SKUs
df_group_outlet_s = df_month_seasonal.groupby(['Supergroup','Product_SKU_Code'])\
                       .agg(countDistinct('Outlet_Unique_Code').alias('Distinct_Outlet_Unique_Code'))


df_group_combination_s = df_month_seasonal.join(ds_store_type.select('Outlet_Unique_Code','class'),on='Outlet_Unique_Code',how='inner')

# get sku sales volume and amount
df_group_sku_s = df_group_combination_s.groupby(['State','class','Category_Desc','Store_type', 'Outlet_Unique_Code','Supergroup','Product_SKU_Code'])\
                                               .agg(avg("Secondary_Sales_Qty_Eaches").alias('Secondary_Sales_Qty_Eaches')
                                                    ,avg("Secondary_Sales_Amount").alias('Secondary_Sales_Amount'))\
                                              .join(df_group_outlet_s,on=['Supergroup','Product_SKU_Code'], how='left')\
                                               .drop_duplicates()
                       
# ranking SKUs and selecting top ranked
df_sku_rank_s = df_group_sku_s.withColumn("sku_rank", 
                                      rank().over(Window.partitionBy(['State','class','Category_Desc','Store_type', "Outlet_Unique_Code", "Supergroup"])\
                                      .orderBy(F.col("Secondary_Sales_Amount").desc(),F.col("Distinct_Outlet_Unique_Code").desc(),
                                               F.col("Secondary_Sales_Qty_Eaches").desc())))\
                                      .filter(col('sku_rank') == 1)

df_sku_rank_s = df_sku_rank_s.withColumn('Seasonality_flag', lit('S') )

# COMMAND ----------

### Combining Seasonal and non-seasonal SKU ranking
df_sku_rank_combined = df_sku_rank.union(df_sku_rank_s)
df_sku_rank_combined = df_sku_rank_combined.withColumnRenamed('Secondary_Sales_Qty_Eaches', 'Secondary_Sales_Qty_seasonal')

# COMMAND ----------

## Cloned to avoid AnalysisException Error in devrbnesmartorder_5_2vs cluster. Not an issue in IndiaAnalytics_Dev02
df_RSL_recos_volume_cloned = df_RSL_recos_volume_combination.toDF('Outlet_Unique_Code', 'Supergroup', 'Distinct_month', 'Secondary_Sales_Qty_Eaches', 'max_year_month', 'Secondary_Sales_Amount', 'NUM_INVOICES', 'Recency', 'Frequency', 'monetary', 'rfm_sum', 'Seasonality_flag', 'priority', 'individual_rank', 'final_rank', 'RSL_reco_line', 'CSL_reco_line', 'Qty_6_avg', 'class', 'State', 'Store_type', 'Category_Desc')

# Merging RSL reco line with SKU ranking
df_RSL_assort_sku = df_RSL_recos_volume_cloned.join(df_sku_rank_combined.select('Supergroup','Outlet_Unique_Code','Product_SKU_Code', 'Seasonality_flag','Secondary_Sales_Qty_seasonal'),on=['Supergroup','Outlet_Unique_Code', 'Seasonality_flag'], how='inner').drop_duplicates()

# COMMAND ----------

# DBTITLE 1,Combination level calculation
### Bring Store type for combination definition
df_allstore_combination = df_month_non_seasonal.select('State','Store_type','Category_Desc','Outlet_Unique_Code','month','Secondary_Sales_Qty_Eaches', 'Supergroup').join(ds_store_type.select('Outlet_Unique_Code','class'),on='Outlet_Unique_Code',how='inner')

### Aggregating Sales for each month in combination  
df_combination_month = df_allstore_combination.groupby('Supergroup','State','class','Category_Desc','Store_type','month')\
                                    .agg(sum('Secondary_Sales_Qty_Eaches').alias('Secondary_Sales_Qty_Eaches')
                                         ,countDistinct('Outlet_Unique_Code').alias('Distinct_Outlet_Unique_Code'))\
                                    .withColumn('combination_month_avg', col('Secondary_Sales_Qty_Eaches')/(col('Distinct_Outlet_Unique_Code'))) 

### Averaging Sales for 6 month in combination  
df_combination = df_combination_month.groupby('Supergroup','State','class','Category_Desc','Store_type')\
                                .agg(sum('combination_month_avg').alias('combination_month_avg'),countDistinct('month').alias('Distinct_month'))\
                                .withColumn('combination_avg', col('combination_month_avg')/col('Distinct_month')).drop('Distinct_month')

# COMMAND ----------

# Choosing Sales Qty for each supergroup recommended
df_reco_volume = df_RSL_assort_sku.join(df_combination, on = ['Supergroup','State','class','Category_Desc','Store_type'], how = "left")\
                                            .drop_duplicates()\
                                            .withColumn("Final_vol1",
                                                when(((col("Qty_6_avg") > col("combination_avg")) & (col("Seasonality_flag")=='NS')), col("Qty_6_avg"))\
                                                .otherwise(col("combination_avg")))
# Final volume assigned to RSL reco based on seasonal and no-seasonal quantitites
df_reco_volume = df_reco_volume.withColumn("Final_vol", 
                                           when((col('Seasonality_flag') == 'S'), col('Secondary_Sales_Qty_seasonal'))\
                                           .otherwise(col('Final_vol1')))\
                                           .withColumn("RSL_Final_vol", ceil(col("Final_vol")))

# COMMAND ----------

## Duplicate Check
df_reco_volume.groupby('Outlet_Unique_Code','Supergroup').agg(count('RSL_Final_vol').alias('c')).sort('c',ascending=False).show(5,False)

# COMMAND ----------

# DBTITLE 1,Save RSL Order
## Save final RSL output file
df_reco_volume.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true")\
                          .save(output_path + '/OUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'RSL_Order.csv')

## Save all rfm supergroups 
all_ranks.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true")\
                          .save(output_path + '/aOUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'all_rfm_supergroups.csv')

## Save all seasonal supergroups
df_supergroup_ss_all.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true")\
                          .save(output_path + '/OUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'all_seasonal_supergroups.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSL Logic

# COMMAND ----------

## creating year and month column
df_data = df_6Mon_latest.drop('month')
split_date = split(df_6Mon_latest['Year_Month'], '/')
df_data = df_data.withColumn('Year', split_date.getItem(0).cast('int'))\
                  .withColumn('Month', split_date.getItem(1).cast('int'))\
                  .withColumnRenamed('STATE','State').select('Outlet_Unique_Code','Product_SKU_Code','Year_Month','State','Category_Desc'
                                                            ,'Secondary_Sales_Amount','Secondary_Sales_Qty_Eaches','Supergroup','Store_type','Year','Month')

# COMMAND ----------

# DBTITLE 1,Creating category rank
# Starting with category ranking ----------------------------------------------

df_combination = ds_store_type.select('Outlet_Unique_Code','class').drop_duplicates()

df_data = df_data.join(df_combination,on=['Outlet_Unique_Code'], how='left')

df_cat_store = df_data.select('State', 'Store_type', 'Category_Desc', 'class', 'Supergroup', 'Product_SKU_Code','Outlet_Unique_Code', 'Secondary_Sales_Amount', 'Secondary_Sales_Qty_Eaches')

####### calculating percentage sales at category level
window2 = Window.partitionBy('State', 'Store_type', 'class', 'Category_Desc').orderBy(lit(1))
window3 = Window.partitionBy('State', 'Store_type', 'class').orderBy(lit(1))
df_cat = df_cat_store.withColumn('SALES', sum(col('Secondary_Sales_Qty_Eaches')).over(window2)).withColumn('TOTAL_SALES', sum(col('Secondary_Sales_Qty_Eaches')).over(window3)).withColumn('PERC_TOTAL_SALES', (col('SALES')/col('TOTAL_SALES')*100))

# dropping the columns not required 
df_cat = df_cat.drop('Supergroup', 'Product_SKU_Code',"Outlet_Unique_Code","Secondary_Sales_Amount", "Secondary_Sales_Qty_Eaches")
df_cat = df_cat.distinct()
df_cat = df_cat.orderBy(col('State').asc(), col('Store_type').asc(), col('class').asc(), col('PERC_TOTAL_SALES').desc())

## calculating cumulative sales
windowval = (Window.partitionBy('State', 'Store_type', 'class').orderBy(col('PERC_TOTAL_SALES').desc(),col('Category_Desc').desc()))
df_cat = df_cat.withColumn('PERC_CUM_SALES', F.sum('PERC_TOTAL_SALES').over(windowval))

df_cat = df_cat.withColumn('PERC_TOTAL_SALES_dummy', when(col("Category_Desc") =='Others', lit(0)).otherwise(col('PERC_TOTAL_SALES')))
df_cat = df_cat.orderBy(col('State').asc(), col('Store_type').asc(),  col('class').asc(), col('PERC_TOTAL_SALES').desc())

## creating category rank based on percenatge sales
df_cat = df_cat.withColumn('Category_Rank', dense_rank().over(window3.orderBy(col('PERC_TOTAL_SALES_dummy').desc())))
df_cat = df_cat.orderBy(col('State').asc(), col('Store_type').asc(), col('Category_Desc').asc(), col('Category_Rank').asc())\
                                .select('State','Store_type','Category_Desc','class','Category_Rank').distinct()

# COMMAND ----------

## Cloned to avoid AnalysisException Error in devrbnesmartorder_5_2vs cluster. Not an issue in IndiaAnalytics_Dev02
df_data_cloned = df_data.toDF('Outlet_Unique_Code', 'Product_SKU_Code', 'Year_Month', 'State', 'Category_Desc', 'Secondary_Sales_Amount', 'Secondary_Sales_Qty_Eaches', 'Supergroup', 'Store_type', 'Year', 'Month', 'class')

df_data1 = df_data_cloned.select('Year','Month','State','Store_type','Outlet_Unique_Code','Supergroup','Secondary_Sales_Qty_Eaches','Category_Desc','class')

df_data2 = df_data_cloned.select('Year','Month','State','Store_type','Outlet_Unique_Code','Supergroup','Secondary_Sales_Qty_Eaches','Category_Desc','class'
                          ,'Product_SKU_Code','Secondary_Sales_Amount')

# COMMAND ----------

# getting supergroup ranking

# getting combination avg. sales volume
df_month_combination = df_data1.groupby(['Month','State','Store_type','Category_Desc','class', 'Supergroup'])\
                                   .agg(countDistinct('Outlet_Unique_Code').alias('distinct_stores_m')
                                        ,sum('Secondary_Sales_Qty_Eaches').alias('sales_sg_m'))

df_month_combination = df_month_combination.withColumn('combination_avg_sales_m', col('sales_sg_m')/col('distinct_stores_m'))

df_combination_sales = df_month_combination.groupby(['State','Store_type','Category_Desc','class', 'Supergroup'])\
                                        .agg(countDistinct('Month').alias('distinct_months')
                                             ,sum('combination_avg_sales_m').alias('combination_sales_m'))

df_combination_sales = df_combination_sales.withColumn('combination_avg_sales', col('combination_sales_m')/col('distinct_months'))

## calculate %age stores sold in p6m at sg level
df_combination_sg_store_count = df_data1.groupby(['State','Store_type','Category_Desc','class', 'Supergroup'])\
                                  .agg(countDistinct('Outlet_Unique_Code').alias('stores_in_sg')
                                       ,sum('Secondary_Sales_Qty_Eaches').alias('sales_sg'))


df_combination_store_count = df_data1.groupby(['State','Store_type','Category_Desc','class'])\
                      .agg(countDistinct('Outlet_Unique_Code').alias('stores_in_class'))

df_combination_store_prcnt = df_combination_sg_store_count.join(df_combination_store_count, on=['State','Store_type','Category_Desc','class'], how='left')

df_combination_store_prcnt = df_combination_store_prcnt.withColumn('perc_stores_sold_p6m', (col('stores_in_sg')/col('stores_in_class')*100))


## generating supergroup rank
window4 = Window.partitionBy('State','Store_type','Category_Desc', 'class').orderBy(lit(1))
df_combination_sg_rank = df_combination_store_prcnt.withColumn('sg_Rank', dense_rank().over(window4.orderBy(col('perc_stores_sold_p6m').desc())))
df_combination_sg_rank = df_combination_sg_rank.orderBy(col('State').asc(), col('Store_type').asc(), col('Category_Desc').asc(),col('class').asc(), col('sg_Rank').asc())

## join combination avg sales
df_combination_sg_rank = df_combination_sg_rank.join(df_combination_sales, on=['State','Store_type','Category_Desc', 'class', 'Supergroup'],
                       how='left').select(df_combination_sg_rank['*'],df_combination_sales['combination_avg_sales'])

# join category rank from df_cat
df_combination_cat_sg_rank = df_combination_sg_rank.join(df_cat, on=['State','Store_type','Category_Desc', 'class']
                                                         ,how='left').select(df_combination_sg_rank['*'],df_cat['Category_Rank'])

# COMMAND ----------

## getting CSL reco lines from RSL output
df_csl_reco = df_reco_volume.select('Outlet_Unique_Code', 'class',  'RSL_reco_line','CSL_reco_line').drop_duplicates()
df_data_filtered = df_data1.select(['Outlet_Unique_Code',  'State', 'Store_type', 'Supergroup', 'Category_Desc']).drop_duplicates()
df_csl_reco = df_csl_reco.join(df_data_filtered, on = ['Outlet_Unique_Code'], how='inner')
df_csl_reco = df_csl_reco.withColumnRenamed('CSL_reco_line', 'csl_reco_number' )
df_csl_reco_cloned= df_csl_reco.distinct()

# COMMAND ----------

## Bringing stores and csl_reco_line to every combination
df_csl_outlet_reco_final = df_combination_cat_sg_rank.join(df_csl_reco_cloned.select('State','Store_type','class','Outlet_Unique_Code','RSL_reco_line'
                                                           ,'csl_reco_number').drop_duplicates(), on=['State','Store_type','class'], how= 'left')

# COMMAND ----------

# filtering out supergroups that were sold in the store in the last 6 months
df_csl_reco_cloned = df_csl_reco_cloned.withColumnRenamed('Supergroup','Supergroup_t1').withColumnRenamed('State','State_t1')\
                                        .withColumnRenamed('Store_type','Store_type_t1').withColumnRenamed('class','class_t1')\
                                        .withColumnRenamed('Outlet_Unique_Code','Outlet_Unique_Code_t1')

cond =  [df_csl_reco_cloned['State_t1']== df_csl_outlet_reco_final['State']
        ,df_csl_reco_cloned['Store_type_t1']== df_csl_outlet_reco_final['Store_type']
        ,df_csl_reco_cloned['class_t1']== df_csl_outlet_reco_final['class']
        ,df_csl_reco_cloned['Outlet_Unique_Code_t1']== df_csl_outlet_reco_final['Outlet_Unique_Code']
        ,df_csl_reco_cloned['Supergroup_t1'] == df_csl_outlet_reco_final['Supergroup']]

df_csl_outlet_reco_filtered = df_csl_outlet_reco_final.join(df_csl_reco_cloned, cond, how= 'left').select(df_csl_outlet_reco_final['*']
                                                                                                   ,df_csl_reco_cloned['Supergroup_t1'])

df_csl_outlet_reco_filtered = df_csl_outlet_reco_filtered.filter((col('Supergroup_t1').isNull()) & (col('Outlet_Unique_Code').isNotNull()))\
                                         .select('State','Store_type','Category_Desc','class','Supergroup','sales_sg'
                                            ,'combination_avg_sales','sg_Rank','Outlet_Unique_Code','Category_Rank','RSL_reco_line'
                                            ,'csl_reco_number').drop_duplicates()

# COMMAND ----------

# DBTITLE 1,Modify Supergroup ranking and Select top ranked
window4 = Window.partitionBy('State', 'Store_type','class','Outlet_Unique_Code', 'Category_Rank').orderBy(lit(1))
df_final = df_csl_outlet_reco_filtered.withColumn('Mod_sg_Rank', dense_rank().over(window4.orderBy(col('sg_Rank').asc(), 
                                                                         col('combination_avg_sales'), col('Supergroup'))))
# selecting top ranked super group per category
df_final = df_final.filter(col('Mod_sg_Rank' )==1 )

# calculating final rank 
window5 = Window.partitionBy('State', 'Store_type','class','Outlet_Unique_Code' ).orderBy(lit(1))
df_final = df_final.withColumn('Final_Rank', dense_rank().over(window5.orderBy(col('Category_Rank').asc(),
                                                                               col('combination_avg_sales'), col('Supergroup'))))
# filtering for CSL recommended supergroups to be less than CSL reco line
df_csl_result = df_final.filter(df_final['Final_Rank'] <= df_final['csl_reco_number'] )   

df_csl_result = df_csl_result.orderBy('State', 'Store_type', 'Category_Desc', 'class', 'Outlet_Unique_Code', 'Final_Rank')

# COMMAND ----------

# picking top skus in the supergroup
df_group_outlet = df_data2.groupby(['Supergroup','Product_SKU_Code'])\
                       .agg(countDistinct('Outlet_Unique_Code').alias('Distinct_Outlet_Unique_Code'))

df_group_combination = df_data2 

# getting sku ranking
df_group_sku = df_group_combination.groupby(['State','class','Category_Desc','Store_type', 'Supergroup','Product_SKU_Code'])\
                       .agg(sum("Secondary_Sales_Qty_Eaches").alias('Secondary_Sales_Qty_Eaches')
                            ,sum("Secondary_Sales_Amount").alias('Secondary_Sales_Amount'))\
                       .join(df_group_outlet,on=['Supergroup','Product_SKU_Code'], how='left')\
                       .drop_duplicates()

# ## Remove Pharma SKUs from store types other than Pharmacy and WSPharmacy
# pharma_sku_list = pharma_skus.select('Material').distinct().toPandas()['Material'].tolist()
# df_group_sku = df_group_sku.filter(~((~col('Store_type').isin(['PHARMACY','WSPHARMACY'])) & (col('Product_SKU_Code').isin(pharma_sku_list))))

# selecting top ranked sku per supergroup
df_sku_rank = df_group_sku.withColumn("sku_rank", 
                                dense_rank().over(Window.partitionBy(['State','class','Category_Desc','Store_type', "Supergroup"])\
                                .orderBy(F.col("Secondary_Sales_Amount").desc(),F.col("Distinct_Outlet_Unique_Code").desc(),
                                         F.col("Secondary_Sales_Qty_Eaches").desc())))\
                                .filter(col('sku_rank') == 1)
  
csl_sku_rank = df_sku_rank.select('State','class','Category_Desc','Store_type', "Product_SKU_Code",  "Supergroup", "sku_rank").drop_duplicates()

# COMMAND ----------

### Bringing SKU ranking to CSL recommendations
df_csl_result = df_csl_result.join(csl_sku_rank,on=['State', 'Store_type', 'Category_Desc', 'class', 'Supergroup'],how='inner').drop_duplicates()

## Renaming column as per RSL
df_csl_result = df_csl_result.withColumnRenamed('combination_avg_sales', 'Final_vol').withColumnRenamed('csl_reco_number', 'CSL_reco_line' )

# COMMAND ----------

## Duplicate Check
df_csl_result.groupby('Outlet_Unique_Code','Supergroup').agg(count('Final_vol').alias('c')).sort('c',ascending=False).show(5,False)

# COMMAND ----------

# DBTITLE 1,Save CSL Order
#Generating CSL output
df_csl_result.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true").save(output_path + '/OUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'CSL_Order.csv')

# COMMAND ----------

# DBTITLE 1,Combine RSL + CSL Order
### Adding Priority flags
df_csl_result2 = df_csl_result.withColumn('Priority_flag', lit('CSL'))\
                              .withColumn('Num_Invoices',lit(0))\
                              .withColumn('Recency',lit(0))\
                              .withColumn('Frequency',lit(0))\
                              .withColumn('monetary',lit(0))\
                              .withColumn('rfm_sum',lit(0))\
                              .withColumn('Seasonality_flag', lit("CSL"))\
                              .withColumn('priority',lit(0))\
                              .withColumn('individual_rank',lit(0))\
                              .withColumn('final_rank',lit(0))\
                              .withColumn('Qty_6_avg',lit(0))\
                              .withColumn('combination_avg',lit(0))\
                              .withColumn('sku_rank',lit(0))\
                              .withColumn('max_year_month',lit("CSL"))\
                              .withColumn('Distinct_month',lit(0))\
                              .withColumn('Secondary_Sales_Qty_seasonal',lit(0))\
                              .withColumn('Secondary_Sales_Qty_Eaches',lit(0))\
                              .withColumn('Secondary_Sales_Amount',lit(0))\
                              .drop('sg_Rank','Category_Rank')

               
df_reco_volume1 = df_reco_volume.withColumn('Priority_flag', lit('RSL'))\
                              .withColumn('sales_sg',lit(0))\
                              .withColumn('Mod_sg_Rank',lit(0))\
                              .withColumn('sku_rank',lit(0))\
                              .drop('combination_month_avg','Final_vol1','RSL_Final_vol')
                                                         
final_cols = ['Supergroup','State', 'class', 'Category_Desc', 'Store_type','Outlet_Unique_Code','Seasonality_flag','Distinct_month'
              ,'Secondary_Sales_Qty_Eaches','max_year_month', 'Secondary_Sales_Amount', 'Num_Invoices','Recency','Frequency','monetary'
              ,'rfm_sum','priority','individual_rank','final_rank','RSL_reco_line','CSL_reco_line','Qty_6_avg','Product_SKU_Code'
              ,'Secondary_Sales_Qty_seasonal','combination_avg','Final_vol','Priority_flag','sales_sg','Mod_sg_Rank','sku_rank']

# ### Selecting required columns
df_csl_final  = df_csl_result2.select(final_cols)
df_rsl_final = df_reco_volume1.select(final_cols)

# COMMAND ----------

## Adding price 
df_all_final_suggested_value_rsl = df_rsl_final.join(df_combination_suggested_price, 
                                                     on=['State','Store_type','class','Supergroup','Seasonality_flag'], how='left')\
                                                       .drop_duplicates()

df_all_final_suggested_value_csl = df_csl_final.join(df_combination_suggested_price_ns.drop('Seasonality_flag'),\
                                                    on=['State','Store_type','class','Supergroup'], how='left')\
                                                       .drop_duplicates()

# COMMAND ----------

## Removing duplicates among RSL and CSL reco lines
df_csl_final_check = df_all_final_suggested_value_csl.join(df_all_final_suggested_value_rsl.select('Outlet_Unique_Code','Supergroup')
                                                           ,on=['Outlet_Unique_Code','Supergroup'], how='left_anti')
df_rsl_final_csl = df_all_final_suggested_value_rsl.union(df_csl_final_check.select(df_all_final_suggested_value_rsl.columns))

# COMMAND ----------

## Modified on 12/27/2020 by KM to identify the base NP_VOL in P6M on which the TO Factor is to be applied
np_vol = spark.sql('select * from ' + database_name + '.NP_VOL')\
.withColumnRenamed('NP_VOL','Basevol')\
.withColumnRenamed('P_VOL','PAgg')\
.drop('Discount')\
.join(to_factor, on=['StoreCode','Supergroup'], how='left')\
.orderBy(col('StoreCode'), col('Supergroup'))

dt_max_discount = np_vol.groupby('StoreCode','Supergroup').agg(max('Discount').alias('Discount'))

dt_max_vol = np_vol.groupby('StoreCode','Supergroup').agg(max('PAgg').alias('PAgg'))

dt_max_discount_filtered = dt_max_discount.join(np_vol, on=['StoreCode','Supergroup','Discount'],how='inner').drop_duplicates()

dt_max_discount_filt = dt_max_vol.join(dt_max_discount_filtered, on=['StoreCode','Supergroup','PAgg'],how='inner').drop_duplicates()

dt_max_discount_filt = dt_max_discount_filt.withColumn('NP_Vol', when(col('Basevol').isNull(), col('PAgg'))\
                                                               .otherwise(col('Basevol')))

dt_max_discount_ratio = dt_max_discount_filt.withColumn('Vol_Ratio',
                                                                    when(((col('P_Vol').isNotNull()) & (col('NP_Vol')>1) & (col('Basevol').isNotNull()))
                                                                    ,col('P_Vol')/col('NP_Vol'))\
                                                          .otherwise(lit(1)))\
                                                          .withColumnRenamed('StoreCode','Outlet_Unique_Code')

# COMMAND ----------

# DBTITLE 1,To Factor Ratio
df_rsl_csl_final_vol_promoted = df_rsl_final_csl.join(dt_max_discount_ratio.select('Outlet_Unique_Code','Supergroup','Vol_Ratio','NP_Vol')
                                                      ,on=['Outlet_Unique_Code','Supergroup'],how='left').drop_duplicates()

df_rsl_csl_final_vol_promoted = df_rsl_csl_final_vol_promoted.withColumn('Vol_Ratio', when((col('Vol_Ratio') > 1), col('Vol_Ratio')).otherwise(lit(1)))\
                                           .withColumn("Final_Promoted_vol", when(col('NP_Vol').isNotNull(), ceil(col('NP_Vol')*col('Vol_Ratio')))\
                                                                                  .otherwise(ceil(col('Final_vol')*col('Vol_Ratio'))))\
                                           .withColumn('Suggested_Value', col('Average_Price')*col('Final_Promoted_vol'))\
                                           .withColumn('Suggested_Value', round(col('Suggested_Value'), 2))

# COMMAND ----------

# DBTITLE 1,Saving combined RSL & CSL data with all the columns
df_rsl_csl_final_vol_promoted.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true")\
                          .save(output_path + '/OUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'RSL_CSL_Order.csv')

# COMMAND ----------

## Load rsl_csl_combined Order 
df_rsl_csl_final_vol_promoted = spark.read.csv(path = output_path + '/OUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'RSL_CSL_Order.csv', header = True, inferSchema = True)

# COMMAND ----------

## Duplicate Check
df_rsl_csl_final_vol_promoted.groupby('Outlet_Unique_Code','Supergroup').agg(count('Final_Promoted_vol').alias('c')).sort('c',ascending=False).show(5,False)

# COMMAND ----------

## Select only required columns
df_rsl_csl_final_vol_promoted = df_rsl_csl_final_vol_promoted.select('State', 'Store_type','Category_Desc','Outlet_Unique_Code','Product_SKU_Code'
                                                                       ,'class','Supergroup','Final_Promoted_vol','Priority_flag','Suggested_Value')

# COMMAND ----------

# MAGIC %md ## Focus and NPD Inclusion

# COMMAND ----------

### Adding Date columns as per flag
df_npd_month_prep = nd_input.withColumn('Start_date', lit(Current_date[0]))\
                            .withColumn("End_date",when((col('Priority_flag') == 'NPD'), add_months(col('Start_date'), 2))\
                                        .otherwise(add_months(col('Start_date'), 1)))

df_npd_month_prep = df_npd_month_prep.withColumn('Priority_flag', when(col('Priority_flag') == 'FOCUS', 'Focus').otherwise(col('Priority_flag')))

# COMMAND ----------

## Getting order month and year for saving NPD_combined file and accessing previous month file
order_year = Current_date[0].year
order_month = Current_date[0].month
if order_month == 1:
  prev_month = 12
  prev_year = order_year - 1
else:
  prev_month = order_month -1 
  prev_year = order_year

# COMMAND ----------

## Reading previous months nd_input file and combining 
try:
  df_previous_month = spark.read.csv(path = inter_path + '/NPD_Update/' + region_loop + '_' + focus_loop + '_' + 'NPD_' + str(prev_year) +'_' + str(prev_month) + '.csv', header = True, inferSchema = True)
  df_previous_month = df_previous_month.withColumn("Start_date",df_previous_month['Start_date'].cast(DateType()))\
                                         .withColumn("End_date",df_previous_month['End_date'].cast(DateType()))
  df_npd_combined = df_npd_month_prep.union(df_previous_month)
except:
  df_npd_combined = df_npd_month_prep

# COMMAND ----------

## Prioritize SKUs by latest date
df_npd_combined = df_npd_combined.withColumn('Latest_Date', F.max('Start_date').over(Window.partitionBy('State','Classification'
                                                                                                      ,'Supergroup','PR_CODE','Priority_flag')))

df_npd_combined = df_npd_combined.filter(col('Start_date') == col('Latest_Date')).drop('Latest_Date')

# COMMAND ----------

# DBTITLE 1,Save the combined ND_INPUT File to a common folder
## Create a folder _NPD update and save only NPD not focus
df_npd_combined.filter(col('Priority_Flag')=='NPD').coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true")\
                        .save(inter_path + '/NPD_Update/' + region_loop + '_' + focus_loop + '_' + 'NPD_' + str(order_year) +'_' + str(order_month) + '.csv')

# COMMAND ----------

### Pre-proceesing on combined file
split_col = F.split(df_npd_combined['Classification'], '_')
df_npd_combined = df_npd_combined.withColumn('class', split_col.getItem(0))
df_npd_combined = df_npd_combined.withColumn('Store_type', split_col.getItem(1)).drop('Classification')

df_npd_combined = df_npd_combined.withColumnRenamed('STRATERGIC_COUNT','Final_vol')\
                               .withColumnRenamed('PR_CODE','Product_SKU_Code')\
                               .withColumnRenamed('Category.Desc','Category_Desc')\
                               .withColumn('Category_Desc', trim(col('Category_Desc')))

# COMMAND ----------

### Focus and NPD
df_focus = df_npd_combined.filter(col('Priority_flag') == 'Focus')
df_focus_agg = df_focus.groupby('State','class','Store_type','Category_Desc','Supergroup').agg(sum('Final_vol').alias('Final_vol'))

df_npd = df_npd_combined.filter(col('Priority_flag') == 'NPD')

# COMMAND ----------

## Addition of outlets and historical sales for every Supergroup 
df_focus_outlet = df_focus_agg.join(df_rsl_csl_final_vol_promoted.select('Outlet_Unique_Code','State','class','Store_type').drop_duplicates(),
                              on=['State','class','Store_type'], how='left').dropna(subset=('Outlet_Unique_Code')).drop_duplicates()

df_month_non_seasonal_class = df_month_non_seasonal.join(df_store_param,on=['Outlet_Unique_Code'],
                                                   how='inner').select(df_month_non_seasonal['*'],df_store_param['class'])\
                                                  .withColumnRenamed('STATE', 'State')

df_supergroup_ns_agg = df_month_non_seasonal_class.groupby('State','Store_type','class','Supergroup')\
                                            .agg(countDistinct("Outlet_Unique_Code").alias('Distinct_outlet')
                                                 ,sum("Secondary_Sales_Qty_Eaches").alias('Secondary_Sales_Qty_Eaches')
                                                 ,sum("Secondary_Sales_Amount").alias('Secondary_Sales_Amount'))

df_focus_value = df_focus_outlet.join(df_supergroup_ns_agg,on=['State','Store_type','class','Supergroup'],how='left').drop_duplicates()

# COMMAND ----------

df_focus_value_combination = df_focus_value.groupby('State','class','Store_type','Category_Desc')\
                                            .agg(countDistinct("Outlet_Unique_Code").alias('Distinct_outlet')
                                                 ,sum("Secondary_Sales_Qty_Eaches").alias('Secondary_Sales_Qty_Eaches')
                                                 ,sum("Secondary_Sales_Amount").alias('Secondary_Sales_Amount'))

## Category_ranking
df_focus_category_rank = df_focus_value_combination.withColumn("category_rank",
                               dense_rank().over(Window.partitionBy(['State','class','Store_type'])\
                               .orderBy(F.col("Secondary_Sales_Amount").desc(),F.col("Distinct_outlet").desc(),
                                        F.col("Secondary_Sales_Qty_Eaches").desc(), F.col("Category_Desc").desc())))\
                                        .select('State','class','Store_type','Category_Desc','category_rank')

## Supergroup_ranking
df_focus_supergroup_rank = df_focus_value.withColumn("supergroup_rank",
                               dense_rank().over(Window.partitionBy(['State','class','Store_type','Category_Desc'])\
                               .orderBy(F.col("Secondary_Sales_Amount").desc(),F.col("Distinct_outlet").desc(),
                                        F.col("Secondary_Sales_Qty_Eaches").desc(),F.col('Final_vol').desc(),F.col("Supergroup").desc())))\
                                        .select('State','Store_type','class','Outlet_Unique_Code','Category_Desc','Supergroup','supergroup_rank')

# COMMAND ----------

## Joining Category_rank & Supergroup_rank
df_focus_all_rank = df_focus_supergroup_rank.join(df_focus_category_rank,on=['State','class','Store_type','Category_Desc'], how='left')\
                                                 .drop_duplicates()

## Creating Final_rank
df_focus_all_rank_final = df_focus_all_rank.withColumn("first_rank",rank().over(Window.partitionBy(['State','class','Store_type','Outlet_Unique_Code'])\
                               .orderBy(F.col("supergroup_rank").asc(), F.col("category_rank").asc())))

# COMMAND ----------

## Joining CSL_reco_line
df_focus_reco = df_reco_volume.select('Outlet_Unique_Code', 'class', 'CSL_reco_line').drop_duplicates()
df_focus_all_rank_all = df_focus_all_rank_final.join(df_focus_reco,on=['Outlet_Unique_Code','class'],how='left')
df_focus_all_rank_reco = df_focus_all_rank_all.filter(col('first_rank') <= col('CSL_reco_line'))

# COMMAND ----------

## Select top ranked sku for every Supergroup
df_focus_sg_sku_outlet = df_focus.join(df_rsl_csl_final_vol_promoted.select('Outlet_Unique_Code','State','class','Store_type').drop_duplicates(),
                              on=['State','class','Store_type'],how='left').dropna(subset=('Outlet_Unique_Code')).drop_duplicates()

df_supergroup_sku_ns_agg = df_month_non_seasonal_class.groupby('State','Store_type','class','Supergroup','Product_SKU_Code')\
                                            .agg(countDistinct("Outlet_Unique_Code").alias('Distinct_outlet')
                                                 ,sum("Secondary_Sales_Qty_Eaches").alias('Secondary_Sales_Qty_Eaches')
                                                 ,sum("Secondary_Sales_Amount").alias('Secondary_Sales_Amount'))

df_focus_sku_value = df_focus_sg_sku_outlet.join(df_supergroup_sku_ns_agg,on=['State','Store_type','class','Supergroup'
                                                                                           ,'Product_SKU_Code'],how='left').drop_duplicates()

df_focus_sku_rank = df_focus_sku_value.withColumn("sku_rank", 
                                dense_rank().over(Window.partitionBy(['State','class','Category_Desc','Store_type', "Supergroup"])\
                                .orderBy(F.col("Secondary_Sales_Amount").desc(),F.col("Distinct_outlet").desc(),
                                         F.col("Secondary_Sales_Qty_Eaches").desc(),F.col('Final_vol').desc(),F.col('Product_SKU_Code').desc())))\
                                .filter(col('sku_rank') == 1)

focus_sku_top = df_focus_sku_rank.select('State','class','Category_Desc','Store_type', "Product_SKU_Code",  "Supergroup").drop_duplicates()

# COMMAND ----------

## Combine top ranked sku and Supergroup
df_focus_sg_sku_reco = df_focus_all_rank_reco.join(focus_sku_top,on=['State','class','Category_Desc','Store_type','Supergroup'],how = 'inner')

## Cloned to avoid AnalysisException Error (in next cmd while joining) in devrbnesmartorder_5_2vs cluster. Not an issue in IndiaAnalytics_Dev02
df_focus_sg_sku_reco_cloned = df_focus_sg_sku_reco.toDF('State', 'class', 'Category_Desc', 'Store_type', 'Supergroup', 'Outlet_Unique_Code', 'supergroup_rank', 'category_rank', 'first_rank', 'CSL_reco_line', 'Product_SKU_Code')

# COMMAND ----------

## Focus with mapped outlets and top supergroups and sku
df_focus_final = df_focus.join(df_focus_sg_sku_reco_cloned.select('State','class','Store_type','Outlet_Unique_Code','Category_Desc','Supergroup',
                                                           'Product_SKU_Code').distinct(),on=['State','Store_type','class','Category_Desc','Supergroup',
                                                                                              'Product_SKU_Code'],how='inner')

## NPD with mapped outlets
df_npd_final = df_npd.join(df_RSL_recos_volume_combination.select('State','Store_type','class','Outlet_Unique_Code').drop_duplicates(),
                                             on=['State','Store_type','class'],how='left').dropna(subset = ('Outlet_Unique_Code'))\
                                            .drop_duplicates()

## Combining Focus & NPD
df_focus_npd_combined = df_focus_final.union(df_npd_final.select(df_focus_final.columns))

# COMMAND ----------

# DBTITLE 1,Focus & NPD Supergroups Price Calculation
## Supergroup price at class level
df_supergroup_price_class = df_combination_price.groupby('State','Store_type','class','Supergroup')\
                                                                .agg(sum('Secondary_Sales_Amount').alias('Secondary_Sales_Amount')
                                                                     ,sum('Secondary_Sales_Qty_Eaches').alias('Secondary_Sales_Qty_Eaches'))\
                                                                .withColumn('Average_Price', col('Secondary_Sales_Amount')/col('Secondary_Sales_Qty_Eaches'))\
                                                                .drop('Secondary_Sales_Amount','Secondary_Sales_Qty_Eaches')

## Supergroup price at store_type level
df_supergroup_price_store_type = df_combination_price.groupby('State','Store_type','Supergroup')\
                                                                .agg(sum('Secondary_Sales_Amount').alias('Secondary_Sales_Amount')
                                                                     ,sum('Secondary_Sales_Qty_Eaches').alias('Secondary_Sales_Qty_Eaches'))\
                                                                .withColumn('Average_Price', col('Secondary_Sales_Amount')/col('Secondary_Sales_Qty_Eaches'))\
                                                                .drop('Secondary_Sales_Amount','Secondary_Sales_Qty_Eaches')

## Supergroup price at state level
df_supergroup_price_state = df_combination_price.groupby('State','Supergroup')\
                                                                .agg(sum('Secondary_Sales_Amount').alias('Secondary_Sales_Amount')
                                                                     ,sum('Secondary_Sales_Qty_Eaches').alias('Secondary_Sales_Qty_Eaches'))\
                                                                .withColumn('Average_Price', col('Secondary_Sales_Amount')/col('Secondary_Sales_Qty_Eaches'))\
                                                                .drop('Secondary_Sales_Amount','Secondary_Sales_Qty_Eaches')
## Joining price
df_npd_combined_with_price = df_focus_npd_combined.join(df_supergroup_price_class.withColumnRenamed('Average_Price','Average_Price_class')\
                                                  ,on=['State','Store_type','class','Supergroup'],how='left')

df_npd_combined_with_price = df_npd_combined_with_price.join(df_supergroup_price_store_type.withColumnRenamed('Average_Price','Average_Price_stype')\
                                                  ,on=['State','Store_type','Supergroup'],how='left')

df_npd_combined_with_price = df_npd_combined_with_price.join(df_supergroup_price_state.withColumnRenamed('Average_Price','Average_Price_state')\
                                                  ,on=['State','Supergroup'],how='left')

## Price from Price.csv
df_npd_combined_with_price = df_npd_combined_with_price.join(price_df,on=['Supergroup'],how='left')

df_npd_combined_with_price = df_npd_combined_with_price.withColumn("Avg_price", when(col('Average_Price_class').isNull()\
                                                                      ,col('Price_Shared')).otherwise(col('Average_Price_class')))\
                                                       .withColumn("Avg_price", when(col('Avg_price').isNull()\
                                                                      ,col('Average_Price_stype')).otherwise(col('Avg_price')))\
                                                       .withColumn("Avg_price", when(col('Avg_price').isNull()\
                                                                      ,col('Average_Price_state')).otherwise(col('Avg_price')))

# COMMAND ----------

## To factor logic
df_reco_volume_with_npd_promoted = df_npd_combined_with_price.join(dt_max_discount_ratio.select('Outlet_Unique_Code','Supergroup','Vol_Ratio','NP_Vol')
                                                                   ,on=['Outlet_Unique_Code','Supergroup'],how='left').drop_duplicates()

## Where Suggested_Value is Null, fill it with Final_Promoted_vol
df_reco_volume_with_npd_promoted = df_reco_volume_with_npd_promoted.withColumn('Vol_Ratio', when((col('Vol_Ratio') > 1), col('Vol_Ratio')).otherwise(lit(1)))\
                                                .withColumn("Final_Promoted_vol", when(col('NP_Vol').isNotNull(), ceil(col('NP_Vol')*col('Vol_Ratio')))\
                                                                                  .otherwise(ceil(col('Final_vol')*col('Vol_Ratio'))))\
                                                .withColumn('Suggested_Value', round(col('Avg_price')*col('Final_Promoted_vol'), 2))\
                                                .withColumn('Suggested_Value', when(col('Suggested_Value').isNull()
                                                                                   ,col('Final_Promoted_vol')).otherwise(col('Suggested_Value')))


### Selecting NPD lines only for current month
df_npd_reco_current_month = df_reco_volume_with_npd_promoted.filter((col('End_date') > Current_date[0]) & (col('Start_date') <= Current_date[0]))\
                                              .drop('Start_date','End_date')

# COMMAND ----------

# DBTITLE 1,Save NPD Order
df_npd_reco_current_month.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true")\
                          .save(output_path + '/OUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'NPD_Order.csv')

# COMMAND ----------

# MAGIC %md ## Overlap

# COMMAND ----------

### Combining RSL-CSL and NPD files
cols_to_select = ['State', 'Store_type','Category_Desc','Outlet_Unique_Code','Product_SKU_Code','class','Supergroup','Final_Promoted_vol',
                                            'Priority_flag','Suggested_Value']
df_rsl_csl_updated = df_rsl_csl_final_vol_promoted.select(cols_to_select)
df_rsl_csl_npd = df_rsl_csl_updated.select(cols_to_select)\
                       .union(df_npd_reco_current_month.select(df_rsl_csl_updated.columns))
df_rsl_csl_npd = df_rsl_csl_npd.persist()

# COMMAND ----------

### Selecting higher in the priority Supergroup and higher volume among common recommendations 
df_rsl_csl_npd_rank = df_rsl_csl_npd.withColumn("Priority_rank",
                                         when((col("Priority_flag") == "Focus"), lit(1))\
                                        .when((col("Priority_flag") == "NPD"), lit(2))\
                                        .when((col("Priority_flag") == "RSL"), lit(3))\
                                        .otherwise(lit(4)))  

df_rsl_csl_npd_overlap = df_rsl_csl_npd_rank.groupby('Outlet_Unique_Code','Supergroup').agg(min('Priority_rank').alias('Priority_rank'))

df_all_final_vol_rank = df_rsl_csl_npd_rank.join(df_rsl_csl_npd_overlap, on=['Outlet_Unique_Code','Supergroup','Priority_rank'], how='inner')\
                                             .drop_duplicates()

df_rsl_csl_npd_volume = df_rsl_csl_npd_rank.withColumn('max_Promoted_vol', F.max('Final_Promoted_vol')\
                                                        .over(Window.partitionBy('Outlet_Unique_code','Supergroup')))\
                                            .withColumn('Flag', when(col('max_Promoted_vol')==col('Final_Promoted_vol'), lit(1)).otherwise(lit(0)))\
                                            .filter(col('Flag')==1)
df_min_priority = df_rsl_csl_npd_volume.groupby('Outlet_Unique_Code','Supergroup').agg(min('Priority_rank').alias('Priority_rank'))

df_rsl_csl_npd_volume = df_rsl_csl_npd_volume.join(df_min_priority, on=['Outlet_Unique_Code','Supergroup','Priority_rank']
                                        ,how='inner').drop('Priority_rank').select('Outlet_Unique_Code','Supergroup','Final_Promoted_vol','Suggested_Value')

df_all_final = df_all_final_vol_rank.drop('Final_Promoted_vol','Suggested_Value').join(df_rsl_csl_npd_volume, on=['Outlet_Unique_Code','Supergroup']
                                                                                       ,how='inner').drop_duplicates()

# COMMAND ----------

## List of Pharma SKUs to filter out
if pharma_skus.count() > 0:
  pharma_skus = pharma_skus.withColumn('Material', concat(lit('S_'), col('Material')))
  pharma_sku_list = pharma_skus.select('Material').distinct().toPandas()['Material'].tolist()

  display(df_all_final.filter(((~col('Store_type').isin(['PHARMACY','WSPHARMACY'])) & (col('Product_SKU_Code').isin(pharma_sku_list)))))

# COMMAND ----------

# DBTITLE 1,Filter out Pharma SKUs 
if pharma_skus.count() > 0:
  df_all_final = df_all_final.filter(~((~col('Store_type').isin(['PHARMACY','WSPHARMACY'])) & (col('Product_SKU_Code').isin(pharma_sku_list))))

# COMMAND ----------

# DBTITLE 1,Remove Store which have no sr_id in POS_data
sr_id = POS_data.select("POSCode", 'SalesRepCode').distinct()
sr_id = sr_id.withColumnRenamed('SalesRepCode',"SR_ID")
sr_id = sr_id.withColumnRenamed('POSCode',"CUST_CD")

split_outlet = split(df_all_final['Outlet_Unique_Code'], '_')
df_all_final = df_all_final.withColumn('CUST_CD', split_outlet.getItem(1)).withColumn("CUST_CD", regexp_replace(col('CUST_CD'), ">", "_"))

df_all_final = df_all_final.join(sr_id, on = ['CUST_CD'], how = 'left')
df_all_combined = df_all_final.filter(col('SR_ID').isNotNull()).drop('SR_ID','CUST_CD')

# COMMAND ----------

## Duplicate Check
df_all_combined.groupby('Outlet_Unique_Code','Supergroup').agg(count('Final_Promoted_vol').alias('c')).sort('c',ascending=False).show(5,False)

# COMMAND ----------

# DBTITLE 1,Save Final Recommendation
### Writing final output
df_all_combined.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true")\
                          .save(output_path + '/OUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'Combined_Order.csv')

# COMMAND ----------

## Unpersist the cached dataframes
df_rsl_csl_npd.unpersist()
print('All logics Implemented')

# COMMAND ----------

# MAGIC %md ## Saving Monthly & Weekly Order

# COMMAND ----------

## Load Combined Order 
df_combined_output = spark.read.csv(path = output_path + '/OUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'Combined_Order.csv', header = True, inferSchema = True)

##Rename Incentive Columns 
incentive_df = incentive_df.withColumnRenamed('ASM','State').withColumnRenamed('Store_Type','Store_type').withColumnRenamed('Line_Type','Priority_flag')\
                           .withColumnRenamed('Incentive_Flag','IncentiveTag').withColumnRenamed('Incentive_Amount','IncentiveAmount')

## Add incentive columns
df_final_out = df_combined_output.join(incentive_df.drop_duplicates(), on=['State','Store_Type','Priority_flag'],how='left')
df_final_out = df_final_out.withColumn('IncentiveTag', when(col('IncentiveTag').isNull(), 'N').otherwise(col('IncentiveTag')))\
                           .withColumn('IncentiveAmount', when(col('IncentiveTag')=='N', lit(0)).otherwise(col('IncentiveAmount')))

# COMMAND ----------

# DBTITLE 1,Formatting for saving monthly and weekly order
split_outlet = split(df_final_out['Outlet_Unique_Code'], '_')
df_final_out = df_final_out.withColumn('DIST_CD', split_outlet.getItem(0)).withColumn('CUST_CD', split_outlet.getItem(1))

split_prd = split(df_final_out['Product_SKU_Code'], '_')
df_final_out = df_final_out.withColumn('PRD_CD', split_prd.getItem(1))\
                            .withColumnRenamed('Final_Promoted_vol','MONTHLY_PROPOSED_QTY')\
                            .withColumnRenamed('Suggested_Value','MONTHLY_SUGGESTED_VALUE')\
                            .withColumn('PRODUCT_TYPE', col('Priority_flag'))\
                            .withColumn("YEAR", lit(Current_date[0].year))\
                            .withColumn("MONTH", lit(Current_date[0].month))\
                            .withColumn("CUST_CD", regexp_replace(col('CUST_CD'), ">", "_"))\
                            .withColumn("MONTHLY_SUGGESTED_VALUE", round(col('MONTHLY_SUGGESTED_VALUE'), 2))\
                            .withColumn("MONTH", F.lpad(col("MONTH"),2,'0'))\

                
df_final_out = df_final_out.withColumn('NPDTag', when(col('Priority_flag')=='NPD', lit('Y')).otherwise(lit('N')))
df_final_out = df_final_out.withColumn('PRIORITY', when(col('PRODUCT_TYPE')=='Focus', lit(1))\
                                                  .when(col('PRODUCT_TYPE')=='NPD', lit(2))\
                                                  .when(col('PRODUCT_TYPE')=='RSL', lit(3))\
                                                  .when(col('PRODUCT_TYPE')=='CSL', lit(4)))

fore_sub2 = df_final_out.select('State','Store_type','DIST_CD','CUST_CD','PRD_CD','MONTHLY_PROPOSED_QTY','MONTHLY_SUGGESTED_VALUE','PRIORITY','PRODUCT_TYPE'
                                ,'YEAR','MONTH','IncentiveTag','IncentiveAmount','NPDTag').drop_duplicates()

te_file_mapping_df = df_final_out.select('DIST_CD','CUST_CD','Store_type','class','PRD_CD','Supergroup').drop_duplicates()
### Writing te file mapping dataframe
te_file_mapping_df.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header","true")\
                          .save(output_path + '/OUTPUT_INTERMEDIATE/' + region_loop + '_' + focus_loop + '_' + 'te_mapping.csv')

fore_sub2.createOrReplaceTempView('fore_sub2')

# COMMAND ----------

# DBTITLE 1,Save Monthly_Order txt files
fore_sub2_1 = spark.table('fore_sub2').drop('State','Store_type')

fore_sub2_1 = fore_sub2_1.withColumn("Priority", row_number().over(Window.partitionBy("DIST_CD", "CUST_CD").orderBy("Priority", desc("MONTHLY_SUGGESTED_VALUE")))).sort("DIST_CD", "CUST_CD", "Priority")

fore_sub2_1 = fore_sub2_1.withColumn("YEAR", col("YEAR").cast("integer")).withColumn("MONTHLY_PROPOSED_QTY", col("MONTHLY_PROPOSED_QTY").cast("integer")) 

## Saving the dataframe in the blob but in a csv format
fore_sub2_1.coalesce(1).write.mode('OVERWRITE').format('csv').option('delimiter','|').option('header','true').option('lineterminator','\r\n').save(output_path +'/'+ 'MonthlyDB/Monthly_Order_' + region_loop + '.txt') 

df = dbutils.fs.ls(output_path +'/'+ "MonthlyDB/Monthly_Order_"+region_loop + '.txt') 

## Changing file format from csv to txt
for f in df:
    if "csv" in f.name:
      dbutils.fs.mv(f.path,output_path +'/'+ 'MonthlyDB/Monthly_Order_' + region_loop + '.txt' + '/Monthly_Order_' + region_loop + '.txt') 
    else:
      dbutils.fs.rm(f.path)

# COMMAND ----------

# DBTITLE 1,Save IN_HOME_MO txt files
dist_loop_list = fore_sub2_1.select('DIST_CD').distinct().rdd.flatMap(lambda x: x).collect()

c_date = dt.date.today()
c_date = c_date.strftime('%Y%m%d')

c_time = dt.datetime.now().time()
c_time = c_time.strftime("%H%M%S")

## MonthlyDB
for i in dist_loop_list:
  fore_sub2_2 = fore_sub2_1.where(col('DIST_CD') == i)
  
  fore_sub2_2 = fore_sub2_2.withColumn("Priority", row_number().over(Window.partitionBy("DIST_CD", "CUST_CD").orderBy("Priority", desc("MONTHLY_SUGGESTED_VALUE")))).sort("DIST_CD", "CUST_CD", "Priority")
  
  fore_sub2_2 = fore_sub2_2.withColumn("YEAR", col("YEAR").cast("integer")).withColumn("MONTHLY_PROPOSED_QTY", col("MONTHLY_PROPOSED_QTY").cast("integer")) 
  
  ## Saving the dataframe in the blob but in a csv format
  fore_sub2_2.coalesce(1).write.mode('OVERWRITE').format('csv').option('delimiter','|').option('header','true').option('lineterminator','\r\n').save(output_path +'/'+ 'MonthlyDB/IN_HEALTH_MO_' + i + '_' + c_date + '_' + c_time + '.txt')

  df = dbutils.fs.ls(output_path +'/'+ 'MonthlyDB/IN_HEALTH_MO_' + i + '_' + c_date + '_' + c_time + '.txt')

  ## Changing file format from csv to txt
  for f in df:
    if "csv" in f.name:
      dbutils.fs.mv(f.path, output_path +'/'+ 'MonthlyDB/IN_HEALTH_MO_' + i + '_' + c_date + '_' + c_time + '.txt/IN_HEALTH_MO_' + i + '_' + c_date + '_' + c_time + '.txt')
    else:
      dbutils.fs.rm(f.path)

# COMMAND ----------

# MAGIC %md ### Weekly Order

# COMMAND ----------

# MAGIC %r
# MAGIC Current_date <- as.Date(Sys.time()+19800)
# MAGIC 
# MAGIC SO_Data <- SparkR::sql('select * from fore_sub2')
# MAGIC 
# MAGIC SO_Data <- as.data.frame(SO_Data)
# MAGIC 
# MAGIC ## df for priority and incentive mapping
# MAGIC prd_type_mapping <- unique(SO_Data[,c('DIST_CD','CUST_CD','PRD_CD','PRIORITY','PRODUCT_TYPE','NPDTag')]) 
# MAGIC incentive_mapping <- unique(SO_Data[,c('State','Store_type','PRODUCT_TYPE','IncentiveTag','IncentiveAmount')])
# MAGIC 
# MAGIC SO_Data <- SO_Data[, 1:11]
# MAGIC SO_Data$MONTH <- formatC(SO_Data$MONTH,width = 2,flag = "0")
# MAGIC 
# MAGIC #Update2
# MAGIC SKU_Grouping <- SparkR::sql('select * from SMARTGROUP')
# MAGIC 
# MAGIC SKU_Grouping <- as.data.frame(SKU_Grouping)
# MAGIC 
# MAGIC SKU_Grouping$'End Date' = substr(SKU_Grouping$'End Date',1,10)
# MAGIC SKU_Grouping$'End Date' <- as.Date(SKU_Grouping$'End Date', format = "%m/%d/%Y")
# MAGIC SKU_Grouping$Current_Date <- as.Date(paste0(format(lubridate::ymd(Current_date,tz=NULL),"%Y/%m"),"/01"), format = "%Y/%m/%d")
# MAGIC SKU_Grouping <- SKU_Grouping[SKU_Grouping$'End Date' > SKU_Grouping$Current_Date, ] ## Filter added on request by Kapil - (06/01/2021)
# MAGIC 
# MAGIC SKU_Grouping <- SKU_Grouping[SKU_Grouping$`ASSIGNMENT Type`=="PRD" & SKU_Grouping$`ASSIGNMENT Level`==10,]  
# MAGIC SKU_Grouping <- SKU_Grouping[,c("ASSIGNMENT Value","Smart Order Code")]
# MAGIC SKU_Grouping <-data.frame(unique(SKU_Grouping ))
# MAGIC names(SKU_Grouping) <- c("Product SKU Code","Supergroup")
# MAGIC 
# MAGIC #Update 3 only if Kapil asks
# MAGIC 
# MAGIC visits <- SparkR::sql('select * from PLANNED_VISITS')
# MAGIC visits <- as.data.frame(visits)
# MAGIC 
# MAGIC #Update 4
# MAGIC POS_data <- SparkR::sql('select * from RB01_PARTNERPOS')
# MAGIC 
# MAGIC phase_split <- data.frame("Week" = 1:4, "Percentage" = c(100,0,0,0)) 
# MAGIC 
# MAGIC POS_data <- as.data.frame(POS_data)
# MAGIC POS_data <- POS_data[,c("PartnerCode", "POSCode")]
# MAGIC names(POS_data)<-c("DIST_CD","Store ID")
# MAGIC POS_data<-POS_data[!(duplicated(POS_data)),]

# COMMAND ----------

# MAGIC %r
# MAGIC visits <- as.data.frame(visits)
# MAGIC visits <- data.table(visits)
# MAGIC visits <- visits[,c("CustomerCode","VisitDate", "SalesRepCode")]
# MAGIC visits$Date <- format(lubridate::ymd(visits$VisitDate,tz=NULL),"%Y/%m")
# MAGIC visits$Day <- format(lubridate::ymd(visits$VisitDate,tz=NULL),"%d")
# MAGIC visits$VisitDate <- NULL
# MAGIC setnames(visits, c("SalesRepCode", "CustomerCode"), c("Sales Representative ID", "Store ID"))
# MAGIC visits <- visits[,c("Store ID", "Date", "Day", "Sales Representative ID")]
# MAGIC visits<-data.table(visits)
# MAGIC 
# MAGIC visits_c <- visits[visits$Date==paste0(year(Current_date),"/",formatC(month(Current_date),width = 2,flag = "0")),]
# MAGIC if (nrow(visits_c) != 0){
# MAGIC ### Comment for Order Month (Use for previous month)
# MAGIC # visits$Date <- paste0(year(Current_date),"/",formatC(month(Current_date)-1,width = 2,flag = "0"))
# MAGIC }
# MAGIC ###
# MAGIC names(visits) <- c("Store ID","Date","Day","Sales Representative ID")
# MAGIC visits<-visits[!(duplicated(visits)),]
# MAGIC visits$Day<-as.numeric(visits$Day)
# MAGIC 
# MAGIC so <- SO_Data
# MAGIC so$CUST_CD<-gsub(">","_",so$CUST_CD)
# MAGIC so$PRD_CD<-gsub("S_","",so$PRD_CD)

# COMMAND ----------

# MAGIC %r
# MAGIC # New Code ----------------------------------------
# MAGIC visits$week <- ifelse(visits$Day >=1 & visits$Day < 8, "1", 
# MAGIC                       ifelse(visits$Day >= 8 & visits$Day < 15, "2",
# MAGIC                              ifelse(visits$Day >= 15 & visits$Day < 22, "3", "4")))
# MAGIC 
# MAGIC visits$week <- paste0("W", visits$week)
# MAGIC phase_split$Week <- paste0("W",as.character(phase_split$Week))
# MAGIC visits_1 <- left_join(visits, phase_split, by = c("week" = "Week"))
# MAGIC visits_1 <- unique(visits_1[,c("Store ID", "week" ,"Percentage")])
# MAGIC 
# MAGIC visits_cast <- data.table(dcast(visits_1, `Store ID` ~ week, value.var = "Percentage"))
# MAGIC visits_cast[is.na(visits_cast)] <- 0
# MAGIC visits_cast[,2:5] <- data.frame(t(apply(visits_cast[,2:5],1, function(x) { (x*100)/sum(x) } )))
# MAGIC visits_cast <- data.table(left_join(visits_cast,POS_data))
# MAGIC visits_cast$CUST_CD <- visits_cast$`Store ID`
# MAGIC visits_cast <- na.omit(visits_cast)
# MAGIC so$DIST_CD <- as.character(so$DIST_CD)
# MAGIC names(so) <- c("State","Store_type","DIST_CD","CUST_CD","PRD_CD" ,"PROPOSED_QTY","SUGGESTED_VALUE","Priority" ,"Product_Type", "YEAR", "MONTH" )
# MAGIC so_1 <- so[so$PROPOSED_QTY>6,]
# MAGIC so_lt_6 <- so[so$PROPOSED_QTY<=6,]

# COMMAND ----------

# MAGIC  %r
# MAGIC so_1 <- left_join(so_1, visits_cast, by = c("DIST_CD","CUST_CD"))
# MAGIC so_1[,c("W1","W2","W3","W4")] <- data.frame(t(apply(so_1[,c("W1","W2","W3","W4")], 1, function(x) { if(all(is.na(x))) { phase_assumed <- c(100,0,0,0); return(phase_assumed) } else return(x)})))
# MAGIC so_1[,c("W1","W2","W3","W4")] <- ceiling((so_1$PROPOSED_QTY*so_1[,c("W1","W2","W3","W4")]/100))
# MAGIC 
# MAGIC so_split_fix <- apply(so_1[,c("PROPOSED_QTY","W1","W2","W3","W4")], 1, function(x) { if(x[1] < sum(x[2:length(x)])) { 
# MAGIC   
# MAGIC   st <- 5
# MAGIC   
# MAGIC   while(x[1] <= sum(x[2:length(x)])){
# MAGIC     if(x[st] != 0) x[st] <- x[st] + (x[1] - sum(x[2:length(x)]))
# MAGIC     if(x[st] < 0) x[st] <- 0
# MAGIC     st <- st-1
# MAGIC     if(st == 1) break()
# MAGIC   }
# MAGIC   
# MAGIC   return(x[2:length(x)])
# MAGIC } else {
# MAGIC   return(x[2:length(x)])
# MAGIC }
# MAGIC })
# MAGIC 
# MAGIC so_1[,c("W1","W2","W3","W4")] <- t(so_split_fix)
# MAGIC so_1<-data.table(so_1)
# MAGIC 
# MAGIC so_price <- unique(so_1[,c("DIST_CD","CUST_CD","PRD_CD","PROPOSED_QTY","SUGGESTED_VALUE")])
# MAGIC so_price$PRICE <- so_price$SUGGESTED_VALUE/so_price$PROPOSED_QTY
# MAGIC so_price[,c("SUGGESTED_VALUE","PROPOSED_QTY")] <- NULL
# MAGIC 
# MAGIC so_1_melt <- melt(so_1[,c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","Priority","Product_Type","YEAR","MONTH","W1","W2","W3","W4")], id.vars = c("State","Store_type","DIST_CD", "CUST_CD", "PRD_CD","Priority","Product_Type","YEAR","MONTH"))
# MAGIC so_1_melt <- left_join(so_1_melt, so_price, by = c("DIST_CD","CUST_CD","PRD_CD"))
# MAGIC so_2 <- so_1_melt
# MAGIC so_2$VALUE <- so_2$value*so_2$PRICE
# MAGIC so_2$PRICE <- NULL
# MAGIC 
# MAGIC names(so_2) <- c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","Priority","Product_Type","YEAR","MONTH","WEEK","PROPOSED_QTY","SUGGESTED_VALUE")

# COMMAND ----------

# MAGIC %r
# MAGIC so_lt_6$WEEK<-"W1"
# MAGIC so_lt_6<-so_lt_6[,names(so_2)]
# MAGIC so_lt_6<-data.frame(so_lt_6)
# MAGIC fore <- data.frame(rbind(so_2,so_lt_6))
# MAGIC fore<-fore[fore$PROPOSED_QTY>0,]
# MAGIC names(fore) <- c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","SKU_Type" ,"Product_Type","YEAR","MONTH","WEEK","WEEKLY_PROPOSED_QTY","WEEKLY_SUGGESTED_VALUE")
# MAGIC fore$CUST_CD<-gsub(">","_",fore$CUST_CD)
# MAGIC fore$PRD_CD<-gsub("S_","",fore$PRD_CD)

# COMMAND ----------

# MAGIC %r
# MAGIC ###############
# MAGIC monthly_output<-melt(fore,id.vars=names(fore)[!names(fore)%in%c("WEEKLY_PROPOSED_QTY","WEEKLY_SUGGESTED_VALUE")])
# MAGIC monthly_output<-dcast(monthly_output,State+Store_type+DIST_CD+CUST_CD+PRD_CD+YEAR+MONTH~WEEK+variable,sum,value.var="value")
# MAGIC 
# MAGIC ## Codes Addition -Start
# MAGIC ## To create columns which are not there because of setting W2, W3, W4 = 0 and then filtering PROPOSED_QTY > 0
# MAGIC weekly_columns_check_list = c('W1_WEEKLY_PROPOSED_QTY','W1_WEEKLY_SUGGESTED_VALUE','W2_WEEKLY_PROPOSED_QTY','W2_WEEKLY_SUGGESTED_VALUE','W3_WEEKLY_PROPOSED_QTY','W3_WEEKLY_SUGGESTED_VALUE','W4_WEEKLY_PROPOSED_QTY','W4_WEEKLY_SUGGESTED_VALUE')
# MAGIC 
# MAGIC cols_toAdd = c()
# MAGIC for(col in weekly_columns_check_list){
# MAGIC   if(col %in% names(monthly_output)) next()
# MAGIC   else{cols_toAdd <- c(cols_toAdd, col)}}
# MAGIC 
# MAGIC monthly_output[cols_toAdd] <- 0
# MAGIC ## Codes Addition -End
# MAGIC 
# MAGIC monthly_output$W2_PROPOSED_QTY<-monthly_output$W1_WEEKLY_PROPOSED_QTY+monthly_output$W2_WEEKLY_PROPOSED_QTY
# MAGIC monthly_output$W2_SUGGESTED_VALUE<-round(monthly_output$W1_WEEKLY_SUGGESTED_VALUE+monthly_output$W2_WEEKLY_SUGGESTED_VALUE, 2)
# MAGIC monthly_output$W3_PROPOSED_QTY<-monthly_output$W2_PROPOSED_QTY+monthly_output$W3_WEEKLY_PROPOSED_QTY
# MAGIC monthly_output$W3_SUGGESTED_VALUE<-round(monthly_output$W2_SUGGESTED_VALUE+monthly_output$W3_WEEKLY_SUGGESTED_VALUE, 2)
# MAGIC monthly_output$W4_PROPOSED_QTY<-monthly_output$W3_PROPOSED_QTY+monthly_output$W4_WEEKLY_PROPOSED_QTY
# MAGIC monthly_output$W4_SUGGESTED_VALUE<-round(monthly_output$W3_SUGGESTED_VALUE+monthly_output$W4_WEEKLY_SUGGESTED_VALUE, 2)
# MAGIC 
# MAGIC Week1_Order <- monthly_output[,c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","YEAR","MONTH","W1_WEEKLY_PROPOSED_QTY","W1_WEEKLY_SUGGESTED_VALUE")]
# MAGIC Week1_Order <- Week1_Order[Week1_Order$W1_WEEKLY_PROPOSED_QTY>0,]
# MAGIC names(Week1_Order) <- c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","YEAR","MONTH","WEEKLY_PROPOSED_QTY","WEEKLY_SUGGESTED_VALUE")
# MAGIC Week2_Order <- monthly_output[,c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","YEAR","MONTH","W2_PROPOSED_QTY","W2_SUGGESTED_VALUE")]
# MAGIC Week2_Order <- Week2_Order[Week2_Order$W2_PROPOSED_QTY>0,]
# MAGIC names(Week2_Order) <- c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","YEAR","MONTH","WEEKLY_PROPOSED_QTY","WEEKLY_SUGGESTED_VALUE")
# MAGIC Week3_Order <- monthly_output[,c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","YEAR","MONTH","W3_PROPOSED_QTY","W3_SUGGESTED_VALUE")]
# MAGIC Week3_Order <- Week3_Order[Week3_Order$W3_PROPOSED_QTY>0,]
# MAGIC names(Week3_Order) <- c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","YEAR","MONTH","WEEKLY_PROPOSED_QTY","WEEKLY_SUGGESTED_VALUE")
# MAGIC Week4_Order <- monthly_output[,c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","YEAR","MONTH","W4_PROPOSED_QTY","W4_SUGGESTED_VALUE")]
# MAGIC Week4_Order <- Week4_Order[Week4_Order$W4_PROPOSED_QTY>0,]
# MAGIC names(Week4_Order) <- c("State","Store_type","DIST_CD","CUST_CD","PRD_CD","YEAR","MONTH","WEEKLY_PROPOSED_QTY","WEEKLY_SUGGESTED_VALUE")
# MAGIC 
# MAGIC ## Command added to fix issue related with headers misplaced in final txt files (15/11/2019)
# MAGIC Week1_Order <- Week1_Order[, c('State','Store_type','DIST_CD', 'CUST_CD', 'PRD_CD', 'WEEKLY_PROPOSED_QTY', 'WEEKLY_SUGGESTED_VALUE', 'YEAR', 'MONTH')]
# MAGIC Week1_Order <- as.DataFrame(Week1_Order)
# MAGIC registerTempTable(Week1_Order, "Week1_Order")
# MAGIC 
# MAGIC Week2_Order <- Week2_Order[, c('State','Store_type','DIST_CD', 'CUST_CD', 'PRD_CD', 'WEEKLY_PROPOSED_QTY', 'WEEKLY_SUGGESTED_VALUE', 'YEAR', 'MONTH')]
# MAGIC Week2_Order <- as.DataFrame(Week2_Order)
# MAGIC registerTempTable(Week2_Order, "Week2_Order")
# MAGIC 
# MAGIC Week3_Order <- Week3_Order[, c('State','Store_type','DIST_CD', 'CUST_CD', 'PRD_CD', 'WEEKLY_PROPOSED_QTY', 'WEEKLY_SUGGESTED_VALUE', 'YEAR', 'MONTH')]
# MAGIC Week3_Order <- as.DataFrame(Week3_Order)
# MAGIC registerTempTable(Week3_Order, "Week3_Order")
# MAGIC 
# MAGIC Week4_Order <- Week4_Order[, c('State','Store_type','DIST_CD', 'CUST_CD', 'PRD_CD', 'WEEKLY_PROPOSED_QTY', 'WEEKLY_SUGGESTED_VALUE', 'YEAR', 'MONTH')]
# MAGIC Week4_Order <- as.DataFrame(Week4_Order)
# MAGIC registerTempTable(Week4_Order, "Week4_Order")

# COMMAND ----------

# MAGIC %r
# MAGIC ## To map incentive, product_type, priority
# MAGIC prd_type_mapping_df <- as.DataFrame(prd_type_mapping)
# MAGIC registerTempTable(prd_type_mapping_df, "prd_type_mapping_df")
# MAGIC 
# MAGIC incentive_mapping_df <- as.DataFrame(incentive_mapping)
# MAGIC registerTempTable(incentive_mapping_df, "incentive_mapping_df")

# COMMAND ----------

# DBTITLE 1,Save Weekly_Order txt files
prd_type_mapping_df = spark.table("prd_type_mapping_df") ## read temp table
incentive_mapping_df = spark.table("incentive_mapping_df")

weeks = ['Week1', 'Week2', 'Week3', 'Week4']

for w in weeks:
  fore_sub2_1 = spark.table(w + '_Order') 
  
  ## Code Addition - Start -- Priority and Incentive Info Mapping
  fore_sub2_1 = fore_sub2_1.join(prd_type_mapping_df, on = ['DIST_CD','CUST_CD','PRD_CD'])
  fore_sub2_1 = fore_sub2_1.join(incentive_mapping_df, on = ['State','Store_type','PRODUCT_TYPE'], how = 'left')
  
  fore_sub2_1 = fore_sub2_1.withColumn("Priority", row_number().over(Window.partitionBy("DIST_CD", "CUST_CD").orderBy("Priority", desc("WEEKLY_SUGGESTED_VALUE")))).sort("DIST_CD", "CUST_CD", "Priority")
  
  cols_order = ['DIST_CD','CUST_CD','PRD_CD','WEEKLY_PROPOSED_QTY','WEEKLY_SUGGESTED_VALUE','Priority','Product_Type',
                'YEAR','MONTH','IncentiveTag','IncentiveAmount','NPDTag']
  fore_sub2_1 = fore_sub2_1.select(cols_order)
  ## Code Addition -End
  
  fore_sub2_1 = fore_sub2_1.withColumn("YEAR", col("YEAR").cast("integer")).withColumn("WEEKLY_PROPOSED_QTY", col("WEEKLY_PROPOSED_QTY").cast("integer"))\
                           .withColumn('WEEKLY_SUGGESTED_VALUE', round(col('WEEKLY_SUGGESTED_VALUE'),2))
  
  ## Saving the dataframe in the blob but in a csv format
  fore_sub2_1.coalesce(1).write.mode('OVERWRITE').format('csv').option('delimiter','|').option('header','true').option('lineterminator','\r\n').save(output_path +'/'+ 'WeeklyDB/' + w + region_loop + '.txt') 

  df = dbutils.fs.ls(output_path +'/'+ 'WeeklyDB/' + w + region_loop + '.txt') 

  ## Changing file format from csv to txt
  for f in df:
    if "csv" in f.name:
      dbutils.fs.mv(f.path,output_path +'/'+ 'WeeklyDB/' + w + region_loop + '.txt' + '/' + w + region_loop + '.txt') 
    else:
      dbutils.fs.rm(f.path)

# COMMAND ----------

# MAGIC %r
# MAGIC fore_filtered_w1<-fore[fore$WEEK=="W1",]
# MAGIC 
# MAGIC fore_sub <- fore_filtered_w1 
# MAGIC 
# MAGIC Type_Rank <- data.frame("Product_Type"=c("Focus","NPD","RSL","CSL"),
# MAGIC                       "Type_Rank"=c(1:4))
# MAGIC fore_sub <- left_join(fore_sub, Type_Rank)
# MAGIC dist_loop <- unique(fore_sub$DIST_CD)
# MAGIC fore_sub <- fore_sub[fore_sub$WEEKLY_PROPOSED_QTY>0,]
# MAGIC 
# MAGIC fore_sub2 <- as.DataFrame(fore_sub)
# MAGIC registerTempTable(fore_sub2, "fore_sub2")

# COMMAND ----------

# DBTITLE 1,Save IN_HOME_WO txt files
fore_sub2 = spark.table('fore_sub2')

dist_loop_list = fore_sub2.select('DIST_CD').distinct().rdd.flatMap(lambda x: x).collect()

c_date = dt.date.today()
c_date = c_date.strftime('%Y%m%d')

c_time = dt.datetime.now().time()
c_time = c_time.strftime("%H%M%S")

## WeeklyDB
for i in dist_loop_list:
  fore_sub2_2 = fore_sub2.where(col('DIST_CD') == i)
  
  fore_sub2_2 = fore_sub2_2.join(incentive_mapping_df, on = ['State','Store_type','PRODUCT_TYPE'], how = 'left')
  fore_sub2_2 = fore_sub2_2.withColumn('NPDTag', when(col('Product_Type')=='NPD',lit('Y')).otherwise('N'))
  fore_sub2_2 = fore_sub2_2.withColumn("WEEKLY_SUGGESTED_VALUE", round(fore_sub2_2["WEEKLY_SUGGESTED_VALUE"], 2))
  fore_sub2_2 = fore_sub2_2.withColumn("YEAR", col("YEAR").cast("integer")).withColumn("WEEKLY_PROPOSED_QTY", col("WEEKLY_PROPOSED_QTY").cast("integer")) 
  
  fore_sub2_2 = fore_sub2_2.withColumn("Priority", row_number().over(Window.partitionBy("DIST_CD", "CUST_CD").orderBy("Type_Rank", desc("WEEKLY_SUGGESTED_VALUE")))).sort("DIST_CD", "CUST_CD", "Priority")
  
  fore_sub2_2 = fore_sub2_2.select(cols_order)
  
  ## Saving the dataframe in the blob but in a csv format
  fore_sub2_2.coalesce(1).write.mode('OVERWRITE').format('csv').option('delimiter','|').option('header','true').option('lineterminator','\r\n').save(output_path +'/'+ 'WeeklyDB/IN_HEALTH_WO_' + i + '_' + c_date + '_' + c_time + '.txt')

  df = dbutils.fs.ls(output_path +'/'+ 'WeeklyDB/IN_HEALTH_WO_' + i + '_' + c_date + '_' + c_time + '.txt')

  ## Changing file format from csv to txt
  for f in df:
    if "csv" in f.name:
      dbutils.fs.mv(f.path, output_path +'/'+ 'WeeklyDB/IN_HEALTH_WO_' + i + '_' + c_date + '_' + c_time + '.txt/IN_HEALTH_WO_' + i + '_' + c_date + '_' + c_time + '.txt')
    else:
      dbutils.fs.rm(f.path)

# COMMAND ----------


