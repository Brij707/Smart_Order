# Databricks notebook source
# MAGIC %md
# MAGIC ## Changelog
# MAGIC * 24/06/2019 - Change table path on `cmd 6` from `smart_order.NAMET_vwDS_SO_PK_2YR` to `smart_order_' + region_home + '_' + country_home + '.vwDS_SO_' + country_home + '_2YR'`;  
# MAGIC This method will prevent conflict when running multiple regions and countries at the same time on different pipelines. This change required additional arguments (`region_home` and `country_home`);  
# MAGIC * 24/06/2019 - On `cmd 6` `SparkR::` was added to the `sql` function. R was refering the `dplyr` package instead of the `SparkR` due to cluster configuration;
# MAGIC * 28/06/2019 - On `cmd 8` import of Planned visits is added;
# MAGIC This logic is added to filter Route stores from the data and DBS for which order needs to be run;
# MAGIC * 28/06/2019 - On `cmd 9` added new_DB_list and added while filtering N_master data;
# MAGIC This logic is added to take care of changed format of DIST_Mapping File;
# MAGIC * 22/07/2019 - changed `length(old_prefix)` to `1:length(old_prefix)` in `cmd 10 line 71`; 
# MAGIC * 25/07/2019 - Changed the argumments to `try` `catch` approach;  
# MAGIC * 25/07/2019 - Change `cmd 8` `line 5` from `temp` to `NMaster`;
# MAGIC * 25/07/2019 - Renamed databases;
# MAGIC * 24/10/2019 - cmd2: Added a key vault to prevent from having passwords on display;
# MAGIC * 18/05/2020 - cmd10: Added "country_loop != 'PAKISTAN_HOME'" to condition, requested by Rakesh because this filter is no longer used for Pakistan too.

# COMMAND ----------

# DBTITLE 1,Define Variables and paths - R
region_loop = tryCatch(getArgument("VAR_REGION"), error = function(e) {return('ASM_MUMBAI_METRO')}) ## ASM_CHENNAI-HOME
focus_loop = tryCatch(getArgument("VAR_FOCUS"), error = function(e) {return('HEALTH')})
country_loop = tryCatch(getArgument("VAR_COUNTRY"), error = function(e) {return('INDIA_HEALTH')})
region_home = tryCatch(getArgument("VAR_SCHEMA_NAME"), error = function(e) {return('SOA')})
country_home = tryCatch(getArgument("VAR_COUNTRY_HOME"), error = function(e) {return('INDIA')})
# database_name = tryCatch(getArgument("VAR_DATABASE_NAME"), error = function(e) {return('smart_order')}) ## Production
database_name = tryCatch(getArgument("VAR_DATABASE_NAME"), error = function(e) {return('smart_order_dev_hc')}) ## Testing
environment = tryCatch(getArgument('VAR_ENVIRONMENT'), error = function(e) {return('dev')}) ## prod, dev ## Creation of the variable "Environment" 09/03/2020 - MS
 
## Data path definition
if (environment == 'dev'){
	if (country_loop == 'INDIA_HEALTH' | country_loop == 'PAK_HEALTH'){
		data_path <- 'wasbs://smartorder@devrbainesasmartorderhc.blob.core.windows.net/dev'
	}else{
		data_path <- 'wasbs://smartorder@devrbainesasmartorder.blob.core.windows.net/dev'}
} else{ if (environment == 'prod'){
  if (country_loop == 'INDIA_HEALTH' | country_loop == 'PAK_HEALTH'){
    data_path <- 'wasbs://smartorder@prdrbainesasmartorderhc.blob.core.windows.net/prod'
    } else{
  data_path <- 'wasbs://smartorder@prdrbainesasmartorder.blob.core.windows.net/prod' ## New Prod Env
    }
} else{
  stop('Undefined Environment! Please select "dev" or "prod" as the environment.')
}
}


database_name_region <- paste0(database_name, '_', region_home)

inter_path <- paste0(data_path,"/",country_loop,"/INTERMEDIATE/")
print(inter_path)
config_path <- paste0(data_path,"/",country_loop,"/INPUTS/CONFIG/")
print(config_path)
increment_path <- paste0(data_path,"/",country_loop,"/INPUTS/INCREMENTAL/")
print(increment_path)
output_path <- paste0(data_path,"/",country_loop,"/OUTPUTS")
print(output_path)

# COMMAND ----------

# DBTITLE 1,Define Variables & Paths - Python
# MAGIC %python
# MAGIC 
# MAGIC try:
# MAGIC   environment = getArgument('VAR_ENVIRONMENT')
# MAGIC   country_loop = getArgument("VAR_COUNTRY")
# MAGIC   
# MAGIC except:
# MAGIC   environment = 'dev' ## prod, dev ## Creation of the variable "Environment" 09/03/2020 - MS
# MAGIC   country_loop = 'INDIA_HEALTH'
# MAGIC   
# MAGIC if country_loop == 'INDIA_HEALTH':
# MAGIC   blob_path =  "wasbs://newspageinputindiahealth@prdrbainesasmartorderhc.blob.core.windows.net"
# MAGIC elif country_loop == 'PAK_HEALTH':
# MAGIC   blob_path = "wasbs://newspageinputpakhealth@prdrbainesasmartorderhc.blob.core.windows.net"
# MAGIC else:
# MAGIC   blob_path = "wasbs://newspageinputindiahealth@prdrbainesasmartorderhc.blob.core.windows.net"

# COMMAND ----------

# DBTITLE 1,AI Model Automation DEV or PROD blob storage access
# MAGIC %python
# MAGIC 
# MAGIC if environment == 'dev':
# MAGIC   ## DEV Storage Account
# MAGIC   if country_loop == 'INDIA_HEALTH' or country_loop == 'PAK_HEALTH':
# MAGIC     storage_account_name = "devrbainesasmartorderhc"
# MAGIC     spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'devrbainekvsmartorderhc', key = 'Blob-devrbainesasmartorderhc-key'))
# MAGIC   else: 
# MAGIC     storage_account_name = "devrbainesasmartorder"
# MAGIC     spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'devrbainekvsmartorder', key = 'Blob-devrbainesasmartorder-key'))
# MAGIC 
# MAGIC 
# MAGIC elif environment == 'prod':
# MAGIC   if country_loop == 'INDIA_HEALTH' or country_loop == 'PAK_HEALTH':
# MAGIC     ## PROD Storage Account
# MAGIC     storage_account_name = "prdrbainesasmartorderhc"
# MAGIC     spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorderhc', key = 'storageaccountsohcpulse'))
# MAGIC   else:
# MAGIC     ## PROD Storage Account
# MAGIC     storage_account_name = "prdrbainesasmartorder"
# MAGIC     spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorder', key = 'blob-prdrbainesasmartorder-key'))
# MAGIC 
# MAGIC else:
# MAGIC   raise ValueError('Undefined Environment! Please select "dev" or "prod" as the environment.')

# COMMAND ----------

# DBTITLE 1,NewsPage Blob container - only for DEV
# MAGIC %python
# MAGIC if environment == 'dev':
# MAGIC   if country_loop == 'INDIA_HEALTH' or country_loop == 'PAK_HEALTH':
# MAGIC     ## PROD Storage Account
# MAGIC     storage_account_name_2 = "prdrbainesasmartorderhc"
# MAGIC     spark.conf.set("fs.azure.account.key." + storage_account_name_2 + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorderhc', key = 'storageaccountsohcpulse'))
# MAGIC   else:
# MAGIC     ## PROD Storage Account
# MAGIC     storage_account_name_2 = "prdrbainesasmartorder"
# MAGIC 
# MAGIC     spark.conf.set("fs.azure.account.key." + storage_account_name_2 + ".blob.core.windows.net", dbutils.secrets.get(scope = 'prdrbainekvsmartorder', key = 'blob-prdrbainesasmartorder-key'))

# COMMAND ----------

# DBTITLE 1,Libraries import - R
library(SparkR)
library(data.table)
library(lubridate)
library(dplyr)
library(stringr)

# COMMAND ----------

# DBTITLE 1,Python libraries Import
# MAGIC %python
# MAGIC import datetime
# MAGIC import dateutil.relativedelta
# MAGIC import datetime as dt
# MAGIC import dateutil as du
# MAGIC import calendar
# MAGIC 
# MAGIC from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Create getDataFromBlob function to get data from blob
# MAGIC %python
# MAGIC ## Get the latest file in the blob.
# MAGIC   
# MAGIC def getDataFromBlob(blob_address, pattern, tableName, separator):
# MAGIC   
# MAGIC   if dt.date.today().day == calendar.monthrange(dt.datetime.now().year,dt.datetime.now().month)[1]: ## Added to get a last day of each month (20/09/2019) - MS
# MAGIC     file_date = dt.date.today() + du.relativedelta.relativedelta(months=1)
# MAGIC     file_date = file_date.strftime('%Y%m01')
# MAGIC     pattern = pattern + file_date
# MAGIC #     pattern = pattern + dt.date.today().strftime('%Y%m%d')
# MAGIC #     pattern = pattern + '20230131' ## ONLY for TESTING Purpose (2020/01/03) MS
# MAGIC   
# MAGIC   else:
# MAGIC #     pattern = pattern + dt.date.today().strftime('%Y%m01')
# MAGIC     pattern = pattern + dt.date.today().strftime('%Y%m%d') ### Changed to get the files for same day-Brij
# MAGIC #     pattern = pattern + '20230131' ## ONLY for TESTING Purpose (2020/01/03) MS
# MAGIC 
# MAGIC 
# MAGIC   print(pattern) 
# MAGIC   list_of_files = dbutils.fs.ls(blob_path)
# MAGIC   lof = []
# MAGIC   for name in list_of_files:
# MAGIC     if pattern in name[1]:
# MAGIC       lof.append(name[1])
# MAGIC   lof.sort()
# MAGIC   latest_file = lof[len(lof) - 1]
# MAGIC   spark.read.csv(blob_address + '/' + latest_file, sep = separator, header = True).registerTempTable(tableName)
# MAGIC   return(spark.read.csv(blob_address + '/' + latest_file, sep = separator, header = True))

# COMMAND ----------

# MAGIC %python
# MAGIC POS_data = getDataFromBlob(blob_address = blob_path, pattern = 'RB01_PARTNERPOS_', tableName ='RB01_PARTNERPOS', separator = '|')

# COMMAND ----------

POS_data_old <- SparkR::sql('select * from RB01_PARTNERPOS')
POS_data_old <- as.data.frame(POS_data_old)
POS_data_old<- POS_data_old[(!is.na(POS_data_old$SalesRepCode))& (POS_data_old$SalesRepCode!="") & (!is.na(POS_data_old$PartnerCode))& (!is.na(POS_data_old$POSCode)),] ## added on request of Kapil- 11/06/2021
POS_data_old <-POS_data_old[POS_data_old$POSStatus==1,]

## Adjusting for duplicates due to 2 way outlets, causing duplication of data - 8/12/2022 KM. inserted _old above
POS_data <- unique(POS_data_old[,c('POSCode','PartnerCode')])



# COMMAND ----------

## List of stores with counter in their name
POSCode_list1 <- POS_data_old %>% select(POSCode, POSDesc) %>% filter(str_detect(toupper(POSDesc), pattern = "COUNTER"))
POSCode_list1 <- unique(POSCode_list1[,c("POSCode")])

## List of stores with counter in sales_representative name
POSCode_list2 <- POS_data_old %>% select(POSCode, SalesRepName) %>% filter(str_detect(toupper(SalesRepName), pattern = "COUNTER"))
POSCode_list2 <- unique(POSCode_list2[,c("POSCode")])

# COMMAND ----------

# Retrive data from temp table based on the filter condiion applied in the WHERE class
#Establish connection to obtain sqlContext for the processing

query <- paste0("SELECT * FROM ", database_name, '_', country_loop, '_', gsub('[\\W]','_', region_loop, perl = T), '_', focus_loop, '.vwDS_SO_', country_home, "_2YR WHERE region_of_store = '", region_loop,"'") ## adaptation to run regions in parallel (29/08/2019)


dtDetails <- SparkR::sql(query)

dtDetails <- as.data.frame(dtDetails)

print(nrow(dtDetails))

# COMMAND ----------

# DBTITLE 1,Remove Leading and Trailing Whitespace
dtDetails$'bucket' <- trimws(dtDetails$'bucket', which = c("both"))
dtDetails$'outlet_category_name' <- trimws(dtDetails$'outlet_category_name', which = c("both"))

# COMMAND ----------

### Commented
# remap <- read.df(file.path(config_path, paste0(region_loop, "_", focus_loop, "_", "DIST_MAPPING.csv")), source = "csv", header="true", inferSchema = "true") ## Added paste0(region_loop, "_", focus_loop, "_", "File_Name.csv") due to naming correction to run in paralel (25/09/2019) - MS
# remap <- as.data.frame(remap)
# if (country_home == 'INDONESIA') names(remap) = c('Region', 'Focus', 'Old_DB', 'New_DB', "Old_StoreCode", "New_StoreCode", "Weekly", "Region7")
## Commented

lob <- read.df(file.path(config_path,"/DIST_BRANDS.csv"), source = "csv", header="true", inferSchema = "true")
lob <- as.data.frame(lob)

## SKUs to remove from Pharmacy & WSPharmacy
if (country_loop == 'INDIA_HEALTH' | country_loop == 'PAK_HEALTH'){
pharma_skus <- read.df(file.path(increment_path, paste0("PHARMA_BRAND.csv")), source = "csv", header="true", inferSchema = "true")
pharma_skus <- as.data.frame(pharma_skus)
  }

# COMMAND ----------

# Directory setup ---------------------------------------------------------
NMaster <- data.table(dtDetails)
names(NMaster) <- paste0("V",1:17) ## Changed from 16 to 17 to include NUM_INVOICES column

NMaster <- NMaster[NMaster$V10   == region_loop,]
NMaster <- data.table(NMaster)
NMaster <- NMaster[NMaster$V14 > 0 & NMaster$V15 > 0,]
NMaster[, `:=`("V18" = str_split_fixed(V3, "_", 2)[, 1], "V19" = str_split_fixed(V3, "_", 2)[, 2]), ]
#nrow(NMaster)

# COMMAND ----------

######################################################################################################## Commented #####

# remap <- remap[remap$Region %in% region_loop & remap$Focus %in% focus_loop,]

# if (country_loop == 'PAKISTAN_HOME'){ ## Added due to store codes be updated directly from Business (25/11/2019) - Anshul
#   old_DB <- as.character(remap$Old_DB[remap$Old_DB != "-"]) ## Added due to store codes be updated directly from Business (25/11/2019) - Anshul
#   new_DB <- as.character(remap$New_DB[remap$Old_DB != "-"]) ## Added due to store codes be updated directly from Business (25/11/2019) - Anshul
#   new_DB_list <- as.character(remap$New_DB[remap$New_DB != "-"]) ## Added due to store codes be updated directly from Business (25/11/2019) - Anshul
# } else { ## Added due to store codes be updated directly from Business (25/11/2019) - Anshul
#   old_prefix <- as.character(remap$Old_StoreCode[remap$Old_StoreCode != "-"])
#   new_prefix <- as.character(remap$New_StoreCode[remap$New_StoreCode != "-"])
#   old_DB <- as.character(remap$Old_DB[remap$Old_DB != "-"])
#   new_DB <- as.character(remap$New_DB[remap$Old_DB != "-"])
#   new_DB_list <- as.character(remap$New_DB[remap$New_DB != "-"])

#   if(!identical(old_prefix, character(0))){
#     for(i in 1:length(old_prefix)){
#     NMaster$V19 <- ifelse(NMaster$V18 %in% old_DB, gsub(old_prefix[i], new_prefix[i], NMaster$V19), NMaster$V19)
#     }
#   }
# } ## Added due to store codes be updated directly from Business (25/11/2019) - Anshul

# if(!identical(old_DB, character(0))){
#   NMaster$V18 <- plyr::mapvalues(NMaster$V18, as.character(old_DB), as.character(new_DB))
# }
# NMaster$V3 <- paste0(NMaster$V18, "_", NMaster$V19)

######################################################################################################## Commented #####

# br_focus <- unique(lob$Brands[lob$Focus %in% focus_loop]) ## Request to remove filter a, As brands don't have same name in sales Data-16/06/22 (Brij)
# NMaster_sub <- NMaster[NMaster$V18 %in% new_DB_list & NMaster$V4 %in% br_focus, ]  


# NMaster_sub <- NMaster[NMaster$V18 %in% new_DB_list]  ## Commented 

NMaster_sub <- NMaster[!grep("Based on", NMaster$V11), ]
nrow(NMaster_sub)

# COMMAND ----------

NMaster_sub_unique <- NMaster_sub[!(duplicated(NMaster_sub[, c("V1", "V2", "V3", "V4", "V5", "V14", "V15")])), ]
nrow(NMaster_sub_unique)

# COMMAND ----------

N_class <- NMaster_sub_unique %>% select(V1, V3, V6, V11) %>% group_by(V3) %>% arrange(V3, V1) %>% slice(n())
N_class <- N_class[, c("V3", "V6", "V11")]
names(N_class) <- c("StoreCode", "CatName", "Class")

# COMMAND ----------

NMaster_sub_unique <- left_join(NMaster_sub_unique, N_class, by = c("V3" = "StoreCode"))
NMaster_sub_unique$V11 <- NMaster_sub_unique$Class
NMaster_sub_unique$V6 <- NMaster_sub_unique$CatName
nrow(NMaster_sub_unique)

# COMMAND ----------

# MAGIC %md
# MAGIC replace old store_code with New store Code
# MAGIC replace old db with new db
# MAGIC remap the class , cat name

# COMMAND ----------

## Re-mapping of DB form POS_DATA on request of Kapil -06/08/2021
NMaster_sub_unique$V19_temp <- gsub(x = NMaster_sub_unique$V19, replacement = "_", pattern = ">")
NMaster_sub_unique <- left_join(NMaster_sub_unique,POS_data[,c('POSCode','PartnerCode')],by=c('V19_temp'='POSCode'))
NMaster_sub_unique$V18 <-NMaster_sub_unique$PartnerCode
NMaster_sub_unique$V3 <- paste0(NMaster_sub_unique$V18,"_",NMaster_sub_unique$V19)
NMaster_sub_unique$PartnerCode <-NULL
NMaster_sub_unique$V19_temp <- NULL
nrow(NMaster_sub_unique)

# COMMAND ----------

# Final_DB_List <- read.df(file.path(increment_path, paste0(region_loop, "_", focus_loop, "_", "DB_List.csv")), source = "csv", header="true", inferSchema = "true")
# Final_DB_List <- as.data.frame(Final_DB_List)
# Final_DB_List$'DB CODE4' <- as.character(Final_DB_List$'DB CODE4')
# Final_DB_List <- unique(Final_DB_List[,c('DB CODE4')])

# NMaster_sub_unique <-  NMaster_sub_unique[NMaster_sub_unique$V18 %in% Final_DB_List,]


# COMMAND ----------

analysis_period <- unique(NMaster_sub_unique$V1)
analysis_period <- paste0(analysis_period,"/01")
analysis_months <- ymd(analysis_period)

prev_dates <- sort(analysis_months)
# print(prev_dates)
if (country_loop == 'INDIA_HEALTH' | country_loop == 'PAK_HEALTH'){   ## Added the condition for INDIA_HEALTH, to filter latest 6 month data
  {if(length(prev_dates) > 5){
  focus_dates = prev_dates[c((length(prev_dates) - 5):(length(prev_dates)))]} else {focus_dates = prev_dates}}
} 
else {if(length(prev_dates) > 12){
  focus_dates <- prev_dates[c((length(prev_dates) - 11), (length(prev_dates) - 10),(length(prev_dates) - 3):(length(prev_dates)))]
} else {
  focus_dates <- prev_dates[c((length(prev_dates) - 3):(length(prev_dates)))]
}
   }
focus_dates <- format(focus_dates, "%Y/%m")
print(focus_dates)

valid_months <-c("2022/09","2022/10","2022/11","2022/12","2023/01","2023/02")

focus_dates <-intersect(focus_dates,valid_months)
print(focus_dates)

# COMMAND ----------

NMaster_sub_unique <- NMaster_sub_unique[,1:17] ## Changed from 16 to 17 to include NUM_INVOICES column

## Removing stores that are absent in Route data

# File PLANNED_VISITS won't run for India. That's the reason for the "IF"

if (country_loop != 'INDIA_HOME' & country_loop != 'PAKISTAN_HOME' & country_loop != 'INDIA_HEALTH' & country_loop != 'PAK_HEALTH') { ## Added country_loop != 'PAKISTAN_HOME' to condition, requested by Rakesh because this filter is no longer used for Pakistan too (2020/05/18) - MS
  map <- read.df(file.path(increment_path, paste0(region_loop, "_", focus_loop, "_", "PLANNED_VISITS.csv")), source = "csv", header="true", inferSchema = "true") ## Added paste0(region_loop,"_",focus_loop,"_","File_Name.csv") due to naming correction to run in paralel (25/09/2019) - MS
  map <- as.data.frame(map)
  map <- map[,c("StoreID","Sales Representative ID")]
  route_stores <- data.table(unique(map$StoreID))
  NMaster_sub_unique <- NMaster_sub_unique[NMaster_sub_unique$V3 %in% route_stores$V1,]
}

NMaster_extract <- NMaster_sub_unique[NMaster_sub_unique$V1 %in% c(focus_dates),]
colname_corrector <- function(T) {
  names(T) <-
    c(
      "Year_Month",
      "City",
      "Store_Code",
      "Brand",
      "Product_SKU_Code",
      "Outlet_category_name",
      "Category_Desc",
      "Address",
      "Metro",
      "Region",
      "Bucket",
      "YAGO",
      "Beat_Code",
      "ValueSales",
      "VolSales",
      "RR",
      "NUM_INVOICES"
    )
  T <- T[!(Store_Code %like% "ZOOM|ADJ")]
  print("Numeric conversion ------start")
  T$VolSales <- as.numeric(gsub(x = T$VolSales, replacement = "", pattern = ","))
  print("Numeric conversion ------25%")
  T$ValueSales <- as.numeric(gsub(x = T$ValueSales, replacement = "", pattern = ","))
  print("Numeric conversion ------50%")
  T$YAGO <- as.numeric(gsub(x = T$YAGO, replacement = "", pattern = ","))
  print("Numeric conversion ------75%")
  T$RR <- as.numeric(gsub(x = T$RR, replacement = "", pattern = ","))
  print("Numeric conversion ------100%")
  T <- T[VolSales > 0][ValueSales > 0]
  print("Filtered non zero sales")
  return(T)
}
NMaster_sub_unique <- colname_corrector(data.table(NMaster_sub_unique))

# COMMAND ----------

# DBTITLE 1,New command to filter stores with no sales in the last 3 months
# if (country_loop == 'INDIA_HOME') {  
#   NMaster_sub_unique$Date <- paste0(NMaster_sub_unique$`Year_Month`, "/01")
#   NMaster_sub_unique$Date <- ymd(NMaster_sub_unique$Date)
#   MonthFilter <- sort(unique(NMaster_sub_unique$Date))
#   MonthFilter3M <- MonthFilter[c(length(MonthFilter), length(MonthFilter)-1, length(MonthFilter)-2)]
#   TempDF <- NMaster_sub_unique[NMaster_sub_unique$Date %in% MonthFilter3M, ]
#   TempDF <- TempDF[TempDF$VolSales > 0, ]
#   store_fil <- unique(TempDF$Store_Code)
#   NMaster_sub_unique <- NMaster_sub_unique[NMaster_sub_unique$Store_Code %in% store_fil, ]
#   NMaster_sub_unique$Date<-NULL
#   NMaster_extract <- NMaster_extract[NMaster_extract$V3 %in% store_fil, ] 
# } 

# COMMAND ----------

# DBTITLE 1,New command to filter stores with no sales in last 4 months 
if (country_loop == 'PAKISTAN_HOME') {
  NMaster_sub_unique$Date <- paste0(NMaster_sub_unique$`Year_Month`, "/01")
  NMaster_sub_unique$Date <- ymd(NMaster_sub_unique$Date)
  MonthFilter <- sort(unique(NMaster_sub_unique$Date))
  MonthFilter4M <- MonthFilter[c(length(MonthFilter), length(MonthFilter)-1, length(MonthFilter)-2, length(MonthFilter)-3)]
  TempDF <- NMaster_sub_unique[NMaster_sub_unique$Date %in% MonthFilter4M, ]
  TempDF <- TempDF[TempDF$VolSales > 0, ]
  store_fil <- unique(TempDF$Store_Code)
  NMaster_sub_unique <- NMaster_sub_unique[NMaster_sub_unique$Store_Code %in% store_fil, ]
  NMaster_sub_unique$Date <- NULL
  NMaster_extract <- NMaster_extract[NMaster_extract$V3 %in% store_fil, ]
}

# COMMAND ----------

# DBTITLE 1,Filter Out Pharma SKUs
if (country_loop == 'INDIA_HEALTH' | country_loop == 'PAK_HEALTH') {
if (nrow(pharma_skus) > 0){
pharma_skus$Material <- paste0('S_', pharma_skus$Material)
pharma_skus_list <- unique(pharma_skus$Material)

NMaster_sub_unique <- NMaster_sub_unique[!((NMaster_sub_unique$Product_SKU_Code %in% pharma_skus_list) & !(NMaster_sub_unique$Bucket %in% c('Pharmacy','WSPharmacy'))), ]

NMaster_extract <- NMaster_extract[!((NMaster_extract$V5 %in% pharma_skus_list) & !(NMaster_extract$V11 %in% c('Pharmacy','WSPharmacy'))), ]
  }}

# COMMAND ----------

# DBTITLE 1,Filter out Store with 'counter' in either POSDesc or SalesRepName
NMaster_sub_unique$CUST_ID <- str_split_fixed(NMaster_sub_unique$Store_Code, "_", 2)[, 2]
NMaster_sub_unique$CUST_ID <- gsub(">", "_", NMaster_sub_unique$CUST_ID)
NMaster_sub_unique <- NMaster_sub_unique[!(NMaster_sub_unique$CUST_ID %in% POSCode_list1), ]
NMaster_sub_unique <- NMaster_sub_unique[!(NMaster_sub_unique$CUST_ID %in% POSCode_list2), ]
NMaster_sub_unique$CUST_ID <- NULL

NMaster_extract$CUST_ID <- str_split_fixed(NMaster_extract$V3, "_", 2)[, 2]
NMaster_extract$CUST_ID <- gsub(">", "_", NMaster_extract$CUST_ID)
NMaster_extract <- NMaster_extract[!(NMaster_extract$CUST_ID %in% POSCode_list1), ]
NMaster_extract <- NMaster_extract[!(NMaster_extract$CUST_ID %in% POSCode_list2), ]
NMaster_extract$CUST_ID <- NULL

# COMMAND ----------

## If category_desc is null and value_sales > 0 then category_desc = "Others"
if (nrow(NMaster_sub_unique[is.na(NMaster_sub_unique$Category_Desc) & (NMaster_sub_unique$ValueSales >0), ]) >0){
NMaster_sub_unique[is.na(NMaster_sub_unique$Category_Desc) & (NMaster_sub_unique$ValueSales >0), ]$Category_Desc <- "Others"}

if (nrow(NMaster_extract[is.na(NMaster_extract$V7) & (NMaster_extract$V14 >0), ]) >0){
NMaster_extract[is.na(NMaster_extract$V7) & (NMaster_extract$V14 >0), ]$V7 <- "Others"}

# COMMAND ----------

# DBTITLE 1,Fix India's Region loop issues "-" and " "
region_loop = gsub('[\\W]','_', region_loop, perl = T) ## added to fix issue " " and "-"
region_loop

# COMMAND ----------

NMaster_sub_unique <- unique(NMaster_sub_unique)
NMaster_extract <- unique(NMaster_extract)

# COMMAND ----------

if ((country_loop != 'INDIA_HOME') & (country_loop != 'INDIA_HEALTH') & (country_loop != 'PAK_HEALTH')) { 
  SparkR::sql(paste0('drop database if exists ', database_name, '_', country_loop, '_', region_loop, '_', focus_loop, ' cascade'))
  SparkR::sql(paste0('create database ', database_name, '_', country_loop, '_', region_loop, '_', focus_loop))
}
# Write the 6 month extract and 2 year data in blob.
NMaster_sub_unique <- as.DataFrame(NMaster_sub_unique)

SparkR::saveAsTable(NMaster_sub_unique, paste0(database_name, '_', country_loop, '_', region_loop, '_', focus_loop, '.PREVIOUS_DATA'), mode = 'overwrite')

NMaster_extract <- as.DataFrame(NMaster_extract)

SparkR::saveAsTable(NMaster_extract, paste0(database_name, '_', country_loop, '_', region_loop, '_', focus_loop, '.6M_EXTRACT'), mode = 'overwrite')