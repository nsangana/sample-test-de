# Databricks notebook source
#library(arrow)
library(sparklyr)
library(magrittr)


# COMMAND ----------

sc <- spark_connect(method="databricks")

# COMMAND ----------



# COMMAND ----------

sdf_sql(sc,"USE CATALOG hive_metastore")
sdf_sql(sc,"USE SCHEMA srijit_nair")

mm <- sdf_sql(sc,"select * from matrix")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/dbfs/srijit.nair/temp/part*

# COMMAND ----------

read_data <- function(fname){
  #print(paste0("Reading:", fname))
  d <- readRDS(fname)
  #print(paste0("Deleting:", fname))
  file.remove(fname)
  return(d)
}

store_data <- function (df,context){
    file_path <- "/dbfs/dbfs/srijit.nair/temp/"
    file_name <- paste0(file_path, "part", as.numeric(Sys.time()), "_",  round(runif(1, 1,1000)) )
    saveRDS(df,file=file_name)
    return(file_name)
}

# COMMAND ----------

mm_part <- sdf_repartition(mm,partitions=1000)

# COMMAND ----------

filename_list <- spark_apply(mm_part, store_data,packages = FALSE)

# COMMAND ----------

filename_list

# COMMAND ----------

display(cc)

# COMMAND ----------

files <- collect(cc)

# COMMAND ----------

res_list <- foreach(i=1:length(files)) %do% readRDS(files[[i]])
result <- rbindlist(res_list)
