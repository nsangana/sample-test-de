# Databricks notebook source
library(Matrix)
library(sparklyr)


# COMMAND ----------

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

readDgcMatrix <- function(assay_id){
  
  sc = spark_connect(method="databricks")
  
  reference_id <- collect(sdf_sql( sc,
    paste0(" SELECT reference_id from single_cell.reference WHERE assay_id ='",assay_id,"' ORDER BY update_dt DESC LIMIT 1") ))[[1]]
  
  mat_data <- split_collect(sc,
    paste0(" SELECT barcode_seq, gene_seq, count from single_cell.matrix WHERE reference_id ='",reference_id,"'") )
  
  gene_data <- split_collect(sc,
    paste0(" SELECT ensemble_gencode from single_cell.genes WHERE reference_id =",reference_id," ORDER BY gene_seq ASC") )
  
  barcode_data <- split_collect(sc,
    paste0(" SELECT barcode from single_cell.barcode WHERE reference_id =",reference_id," ORDER BY barcode_seq ASC") )
  
  dgc_matrix <- sparseMatrix(i = mat_data$barcode_seq, 
                           j = mat_data$gene_seq, 
                           x = mat_data$count, 
                           repr = "C",
                           dims=(c(length(gene_data$ensemble_gencode),length(barcode_data$barcode))),
                           dimnames = list(gene_data$ensemble_gencode,barcode_data$barcode))
  
  return (dgc_matrix)
}

# COMMAND ----------

dgc_matrix <- readDgcMatrix('ASSAY1')

# COMMAND ----------

dgc_matrix

# COMMAND ----------


