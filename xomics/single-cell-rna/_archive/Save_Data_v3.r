# Databricks notebook source
# MAGIC %md
# MAGIC This notebook demonstrates how we can read matrix data from files and persis them into delta tables.
# MAGIC
# MAGIC This notebook also demonstrates a pattern that can be used to read the change log of files and automatically process the files and persist them in delta

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/tables/scRNASeq/PRJNA662780

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/tables/scRNASeq/CD33_ASO_5XFAD

# COMMAND ----------

library(Matrix)
library(SparkR)

sparkR.session()

# COMMAND ----------

root_dir <- "/dbfs/FileStore/tables/scRNASeq/CD33_ASO_5XFAD/"
barcode_file <- "CD33_ASO_5XFAD_barcodes.tsv.gz"
gene_file <- "CD33_ASO_5XFAD_genes.tsv.gz"
matrix_file <- "CD33_ASO_5XFAD_matrix.mtx.gz"

assay_file <- "CD33_ASO_5XFAD_scRNAseq_assaydata.csv"
metadata_file <- "CD33_ASO_5XFAD_scRNAseq_metadata_cell_ranger_filtered.csv"
diff_expression_file <- "CD33_ASO_5XFAD_scRNAseq_diff_expression_cell_ranger_filtered.csv"
feature_data_file <- "CD33_ASO_5XFAD_scRNAseq_featuredata_cell_ranger_filtered.csv"

# COMMAND ----------



readAssayDataFromFiles <- function(path, assay_file){
  #implement to read from the appropriate file

  library(jsonlite)
  assay_data = read.csv(paste0(path, assay_file),
                              header = TRUE,
                              stringsAsFactors = FALSE)
  #assay_data$tags <- toString(jsonlite::toJSON(list(owner="eisai", department="R&D"),auto_unbox=TRUE))
  
  return(assay_data)
}


# COMMAND ----------



#root_dir <- "/dbfs/FileStore/philsalm/eisai/singlecell/GRCh38/"
#barcode_file <- "barcodes.tsv"
#gene_file <- "genes.tsv"
#matrix_file <- "matrix.mtx"

root_dir <- "/dbfs/FileStore/tables/scRNASeq/PRJNA662780/"
barcode_file <- "snRNAseq_PD_PRJNA662780_barcodes.tsv"
gene_file <- "snRNAseq_PD_PRJNA662780_genes.tsv"
matrix_file <- "snRNAseq_PD_PRJNA662780_matrix.mtx"

assay_file <- "snRNAseq_PD_PRJNA662780_assaydata.csv"
metadata_file <- "snRNAseq_PD_PRJNA662780_metadata.csv"
diff_expression_file <- "snRNAseq_PD_PRJNA662780_diff_expression.csv"
feature_data_file <- "snRNAseq_PD_PRJNA662780_featuredata.csv"

# COMMAND ----------

getFilesToProcess <- function(){

  unprocessed_files = collect(sql(
    "SELECT * FROM  hive_metastore.poc_single_cell.dna_nexus_files WHERE processed=false"
  ))
  
  return(unprocessed_files)
}

# COMMAND ----------

readMatrixFromFiles <- function(path, barcode_file, gene_file, matrix_file){
  barcode.path <- paste0(path, barcode_file)
  features.path <- paste0(path, gene_file)
  matrix.path <- paste0(path, matrix_file)

  mat <- readMM(file = matrix.path)

  feature.names = read.delim(features.path,
                             header = FALSE,
                             stringsAsFactors = FALSE)
  
  if(ncol(feature.names) == 2) {
    feature.names$expression_type <- NA
  } 
    
  barcode.names = read.delim(barcode.path,
                             header = FALSE,
                             stringsAsFactors = FALSE)
  
  colnames(mat) = barcode.names$V1
  rownames(mat) = feature.names$V2
  
  return(list(genes=feature.names, barcodes=barcode.names,matrix=mat))
}

# COMMAND ----------

readMetadataFromFiles <- function(path, metadata_file, diff_expression_file, feature_data_file){
  #implement
  return(list())
}

# COMMAND ----------

combineMatrices <- function(path, matrix_files){
  #the combine logic here
}

# COMMAND ----------

saveToDelta <- function(dgc_matrix, gene_data, barcodes_data, assay_data, metadata){
  
  reference_spark <- as.DataFrame(assay_data)
  
  createOrReplaceTempView(reference_spark, "reference_spark")
  reference_result <- sql(
     "MERGE INTO poc_single_cell.reference AS target USING reference_spark AS source
      ON target.assay_id = source.assay_id
      WHEN MATCHED THEN UPDATE SET
        study_id  = source.study_id,
        assay_id = source.assay_id,
        data_type = source.data_type,
        assay_platform = source.assay_platform,
        update_dt = current_timestamp()
      WHEN NOT MATCHED THEN INSERT (study_id,assay_id,data_type,assay_platform,update_dt) VALUES (
        source.study_id,
        source.assay_id,
        source.data_type,
        source.assay_platform,
        current_timestamp()
      )")
  
  print("Done saving reference data")
  
  reference_id <- collect(sql( 
    paste0(" SELECT reference_id from poc_single_cell.reference WHERE assay_id ='",assay_data$assay_id[[1]],"' ORDER BY update_dt DESC LIMIT 1") ))[[1]]         
  
  barcodes <- as.data.frame(barcodes_data)
  barcodes$idx <- seq(1, nrow(barcodes))
  barcodes$ref <- reference_id
  colnames(barcodes) <- c("barcode", "barcode_seq","reference_id")
  barcode_spark <- as.DataFrame(barcodes)
  createOrReplaceTempView(barcode_spark, "barcode_spark")
  barcode_result <- sql(
     "MERGE INTO poc_single_cell.barcode AS target USING barcode_spark AS source
      ON target.reference_id = source.reference_id 
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *")
  
  print("Done saving barcode data")
  
  genes <- as.data.frame(gene_data)
  genes$idx <- seq(1, nrow(genes))
  genes$ref <- reference_id
  colnames(genes) <- c("ensemble_gencode","gene_name","expression_type","gene_seq","reference_id")
  gene_spark <- as.DataFrame(genes)
  createOrReplaceTempView(gene_spark, "gene_spark")  
  genes_result <- sql(
     "MERGE INTO poc_single_cell.genes AS target USING gene_spark AS source
      ON target.reference_id = source.reference_id 
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *")
  
  print("Done saving gene data")
  
  mat_data <- Matrix::summary(dgc_matrix)
  mat_data$ref <- reference_id
  colnames(mat_data) <- c("gene_seq", "barcode_seq", "count", "reference_id")
  mat_spark <- as.DataFrame(mat_data, numPartitions=1000)
  createOrReplaceTempView(mat_spark, "mat_spark")  
  mat_result <- sql(
     "MERGE INTO poc_single_cell.matrix AS target USING mat_spark AS source
      ON target.reference_id = source.reference_id 
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *")
  
  print("Done saving matrix data")
  
}

# COMMAND ----------

updateProcessedStatus <- function(path){  
  num_updates = collect(sql(
    paste0("UPDATE hive_metastore.poc_single_cell.dna_nexus_files SET processed=TRUE, processedTime=CURRENT_TIMESTAMP WHERE path='", path,"'")
  ))
  return(num_updates$num_affected_rows)
}

# COMMAND ----------

process <- function(){
  
  unprocessed_files <- getFilesToProcess()
  
  for(i in 1:nrow(unprocessed_files)) {
    path <- unprocessed_files[i,1]
    filenames <- unprocessed_files[i,2]
    ###write logic to select which files need to be read in
    
    #root_dir <- "/dbfs/FileStore/philsalm/eisai/singlecell/GRCh38/"
    #barcode_file <- "barcodes.tsv"
    #gene_file <- "genes.tsv"
    #matrix_file <- "matrix.mtx"
    #assay_file <- "assay_data.csv"
    #metadata_file <- "metadata.csv"
    #diff_expression_file <- "diff_expression.csv"
    #feature_data_file <- "feature_data.csv"
    
    ##Combine smaller matrices into a large matrix
    #combineMatrices(root_dir)
    
    ##Read large matrix if needed (if we are generating large matrix in this code, you dont need to persist it to disk)
    #assay_data <- readAssayDataFromFiles(root_dir, assay_file)
    #metadata <- readMetadataFromFiles(root_dir, diff_expression_file=diff_expression_file, metadata_file=metadata_file, feature_data_file=feature_data_file)
    #matrix_data <- readMatrixFromFiles(root_dir,barcode_file=barcode_file, gene_file=gene_file, matrix_file=matrix_file)
    #saveToDelta(matrix_data$matrix, matrix_data$genes,matrix_data$barcodes, assay_data, metadata)
    
    ##Mark the file as processed
    #updateProcessedStatus(root_dir)
  }
  
}

# COMMAND ----------

#process()

# COMMAND ----------

assay_data <- readAssayDataFromFiles(root_dir, assay_file)
metadata <- readMetadataFromFiles(root_dir, diff_expression_file=diff_expression_file, metadata_file=metadata_file, feature_data_file=feature_data_file)
matrix_data <- readMatrixFromFiles(root_dir,barcode_file=barcode_file, gene_file=gene_file, matrix_file=matrix_file)


# COMMAND ----------

# MAGIC %python
# MAGIC from resource import getrusage, RUSAGE_SELF
# MAGIC print(getrusage(RUSAGE_SELF).ru_maxrss)

# COMMAND ----------

matrix_data$matrix

# COMMAND ----------

assay_data <- readAssayDataFromFiles(root_dir, assay_file)
metadata <- readMetadataFromFiles(root_dir, diff_expression_file=diff_expression_file, metadata_file=metadata_file, feature_data_file=feature_data_file)
matrix_data <- readMatrixFromFiles(root_dir,barcode_file=barcode_file, gene_file=gene_file, matrix_file=matrix_file)

# COMMAND ----------

matrix_data$matrix

# COMMAND ----------

saveToDelta(matrix_data$matrix, matrix_data$genes,matrix_data$barcodes, assay_data, metadata)

# COMMAND ----------

saveToDelta(matrix_data$matrix, matrix_data$genes,matrix_data$barcodes, assay_data, metadata)
