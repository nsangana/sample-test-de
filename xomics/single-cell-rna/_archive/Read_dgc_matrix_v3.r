# Databricks notebook source
library(Matrix)
library(SparkR)

sparkR.session()


# COMMAND ----------

readDgcMatrix <- function(assay_id){
  
  reference_id <- collect(sql( 
    paste0(" SELECT reference_id from poc_single_cell.reference WHERE assay_id ='",assay_id,"' ORDER BY update_dt DESC LIMIT 1") ))[[1]]
  
  matrix_data <- collect(sql(
    paste0(" SELECT  gene_seq, barcode_seq, count from poc_single_cell.matrix WHERE reference_id ='",reference_id,"'") ))
  
  gene_data <- collect(sql(
    paste0(" SELECT gene_name from poc_single_cell.genes WHERE reference_id =",reference_id," ORDER BY gene_seq ASC") ))
  
  barcode_data <- collect(sql(
    paste0(" SELECT barcode from poc_single_cell.barcode WHERE reference_id =",reference_id," ORDER BY barcode_seq ASC") ))
  
  dgc_matrix <- sparseMatrix(i = matrix_data$gene_seq, 
                             j = matrix_data$barcode_seq, 
                             x = matrix_data$count, 
                             repr = "C",
                             dims=(c(length(gene_data$gene_name), length(barcode_data$barcode))),
                             dimnames = list(gene_data$gene_name, barcode_data$barcode))
  
  
  return (dgc_matrix)
}

# COMMAND ----------

dgc_matrix <- readDgcMatrix('snRNAseq_PD_PRJNA662780')

# COMMAND ----------

dgc_matrix

# COMMAND ----------

dgc_matrix <- readDgcMatrix('snRNAseq_PD_PRJNA662780')

# COMMAND ----------

dgc_matrix

# COMMAND ----------

dgc_matrix2 <- readDgcMatrix('CD33_ASO_5XFAD_scRNAseq_1')

# COMMAND ----------

dgc_matrix3 <- readDgcMatrix('CD33_ASO_5XFAD_scRNAseq_1')

# COMMAND ----------

dgc_matrix4 <- readDgcMatrix('CD33_ASO_5XFAD_scRNAseq_1')

# COMMAND ----------

sessionInfo()
