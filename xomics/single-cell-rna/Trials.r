# Databricks notebook source
library(Matrix)
matrix_dir = "/dbfs/FileStore/philsalm/eisai/singlecell/GRCh38/"

barcode.path <- paste0(matrix_dir, "barcodes.tsv")
features.path <- paste0(matrix_dir, "genes.tsv")
matrix.path <- paste0(matrix_dir, "matrix.mtx")

mat <- readMM(file = matrix.path)

feature.names = read.delim(features.path,
                           header = FALSE,
                           stringsAsFactors = FALSE)
barcode.names = read.delim(barcode.path,
                           header = FALSE,
                           stringsAsFactors = FALSE)
colnames(mat) = barcode.names$V1
rownames(mat) = feature.names$V1


# COMMAND ----------

mat

# COMMAND ----------

library(jsonlite)

tags <- list(owner="srijit", department="it")
reference_data <- data.frame("exp1","Experiment 1","ASSAY1","m","s3://mybucket/path", Sys.time())
colnames(reference_data) <- c("name","description","assay_id", "mlflow_experiment_uri", "data_file_path", "update_dt")
reference_data$tags <- toString(toJSON(tags,auto_unbox=TRUE))


# COMMAND ----------

 library(SparkR)
  sparkR.session()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

reference_spark <- as.DataFrame(reference_data)
insertInto( select(reference_spark,c("assay_id") ),"single_cell.reference")

# COMMAND ----------

barcodes <- as.data.frame(barcode.names)
barcodes$idx <- seq(1, nrow(barcodes))
barcodes$ref <- 2
colnames(barcodes) <- c("barcode", "barcode_seq","reference_id")
barcode_spark <- as.DataFrame(barcodes)

head(barcode_spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED single_cell.reference

# COMMAND ----------


insertInto(barcode_spark,"single_cell.barcode")

# COMMAND ----------

saveCMatrixToDelta <- function(sparkConnection, reference_data, dgc_matrix_data, gene_data, barcodes_data){
  
  library(SparkR)
  sparkR.session()
  
  reference_spark <- as.DataFrame(reference_data)
  createOrReplaceTempView(reference_spark, "reference_spark")
  reference_result <- sql(
     "MERGE INTO single_cell.reference AS target USING reference_spark AS source
      ON target.assay_id = source.assay_id
      WHEN MATCHED THEN UPDATE SET
        name = source.name,
        description = source.description,
        assay_id = source.assay_id,
        mlflow_experiment_uri = source.mlflow_experiment_uri,
        data_file_path = source.data_file_path,
        update_dt = source.update_dt
      WHEN NOT MATCHED THEN INSERT (name,description,assay_id,mlflow_experiment_uri,data_file_path,update_dt) VALUES (
        source.name,
        source.description,
        source.assay_id,
        source.mlflow_experiment_uri,
        source.data_file_path,
        source.update_dt
      )")
  
  reference_id <- collect(sql( 
    paste0(" SELECT reference_id from single_cell.reference WHERE assay_id ='",reference_data$assay_id[[1]],"' ORDER BY update_dt DESC LIMIT 1") ))[[1]]         
  
  barcodes <- as.data.frame(barcodes_data)
  barcodes$idx <- seq(1, nrow(barcodes))
  barcodes$ref <- reference_id
  colnames(barcodes) <- c("barcode", "barcode_seq","reference_id")
  barcode_spark <- as.DataFrame(barcodes)
  createOrReplaceTempView(barcode_spark, "barcode_spark")
  barcode_result <- sql(
     "MERGE INTO single_cell.barcode AS target USING barcode_spark AS source
      ON target.reference_id = source.reference_id 
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *")
  
  genes <- as.data.frame(gene_data)
  genes$idx <- seq(1, nrow(genes))
  genes$ref <- reference_id
  colnames(genes) <- c("ensemble_gencode","gene_name","gene_seq","reference_id")
  gene_spark <- as.DataFrame(genes)
  createOrReplaceTempView(gene_spark, "gene_spark")  
  genes_result <- sql(
     "MERGE INTO single_cell.genes AS target USING gene_spark AS source
      ON target.reference_id = source.reference_id 
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *")
  
  mat_data <- Matrix::summary(dgc_matrix_data)
  mat_data$ref <- reference_id
  colnames(mat_data) <- c("barcode_seq","gene_seq","count","reference_id")
  mat_spark <- as.DataFrame(mat_data, numPartitions=1000)
  createOrReplaceTempView(mat_spark, "mat_spark")  
  mat_result <- sql(
     "MERGE INTO single_cell.matrix AS target USING mat_spark AS source
      ON target.reference_id = source.reference_id 
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *")
  
}

# COMMAND ----------

saveCMatrixToDelta(sc, reference_data, mat, feature.names, barcode.names)

# COMMAND ----------

readDgcMatrix <- function(assay_id){
  library(Matrix)
  library(SparkR)
  
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

# COMMAND ----------


