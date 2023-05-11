# Databricks notebook source
library(Matrix)
library(SparkR)

sparkR.session()


# COMMAND ----------

readDgcMatrix <- function(assay_id){
  
  reference_id <- collect(sql( 
    paste0(" SELECT reference_id from single_cell.reference WHERE assay_id ='",assay_id,"' ORDER BY update_dt DESC LIMIT 1") ))[[1]]
  
  mat_data <- collect(sql(
    paste0(" SELECT barcode_seq, gene_seq, count from single_cell.matrix WHERE reference_id ='",reference_id,"'") ))
  
  gene_data <- collect(sql(
    paste0(" SELECT ensemble_gencode from single_cell.genes WHERE reference_id =",reference_id," ORDER BY gene_seq ASC") ))
  
  barcode_data <- collect(sql(
    paste0(" SELECT barcode from single_cell.barcode WHERE reference_id =",reference_id," ORDER BY barcode_seq ASC") ))
  
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
