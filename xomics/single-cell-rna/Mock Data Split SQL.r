# Databricks notebook source
library(SparkR)
library(magrittr)

# COMMAND ----------

sc <- sparkR.session()

# COMMAND ----------

sql("USE CATALOG hive_metastore")
sql("USE SCHEMA srijit_nair")

mm <- sql(paste0("SELECT *, (row_number() OVER (ORDER BY reference_id)) AS rnum  FROM matrix ") )
cache(mm)
df_size <- count(mm)

# COMMAND ----------

split_size <- 50000000

# COMMAND ----------

df_size

# COMMAND ----------

start <- (batch_num-1)*10
end <- start + split_size

result <- mm %>%
 filter( .$rnum >= start & .$rnum < end ) 

# COMMAND ----------

library(foreach)

# COMMAND ----------

result_list <- foreach(i=1: ceiling(df_size/split_size)) %do% {
  start <- (i-1)*10
  end <- start + split_size
  mm %>%
    filter( .$rnum >= start & .$rnum < end ) %>%
    collect()    
}

# COMMAND ----------


