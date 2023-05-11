# Databricks notebook source
library(sparklyr)
library(dplyr)
library(arrow)
library(foreach)

split_collect <- function(sc, sql_query, split_size=5e7, verbose=FALSE, row_num_field_name='__r_idx_nm'){
  library(dplyr)
  library(arrow)
  library(foreach)
  
  if (verbose) print("Executing query and caching results")

  query_result <- sdf_sql(sc, sql_query) %>% 
      sdf_with_sequential_id(id = row_num_field_name, from = 1L) %>%
      compute() 
  
  df_size <- collect(count(query_result))[[1]][[1]]
  num_batches <- ceiling( (df_size+1)/split_size )
  if (verbose) print(paste0(num_batches," batches to collect for ", df_size, " rows"))
  
  result_list <- foreach(i=1: num_batches) %do% {
    
    start <- (i-1) * split_size
    end <- start + split_size
    if (verbose) print(paste0("Collecting results for batch ",i, " from ", start, " to ",  end))
    
    query_result %>%
      filter( !!sym(row_num_field_name) >= start & !!sym(row_num_field_name) < end) %>%
       select(-(!!sym(row_num_field_name))) %>%
       collect()   
  }
  
  result <- bind_rows(result_list)
  
  return(result)
}

# COMMAND ----------

sc = spark_connect(method = "databricks")

rr <- split_collect(sc, "SELECT * FROM hive_metastore.srijit_nair.matrix" , verbose=TRUE)

# COMMAND ----------

dim(rr)

# COMMAND ----------

print(object.size(rr), units="Gb")

# COMMAND ----------

rr[1000:1010, ]

# COMMAND ----------


