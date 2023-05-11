-- Databricks notebook source
-- MAGIC %md
-- MAGIC In this notebook we will create the necessary delta tables for storing the combined matrix information. The data model we will be using is as below
-- MAGIC
-- MAGIC **reference table**
-- MAGIC |field name| type|
-- MAGIC |----------|-----|
-- MAGIC |reference_id | BIGINT IDENTITY|
-- MAGIC |name| STRING |
-- MAGIC |description| STRING |
-- MAGIC |assay_id| STRING |
-- MAGIC |mlflow_experiment_uri| STRING |
-- MAGIC |data_file_path| STRING |
-- MAGIC |tags| STRING |
-- MAGIC |update_dt | TIMESTAMP |
-- MAGIC
-- MAGIC
-- MAGIC **matrix table**
-- MAGIC |field name| type|
-- MAGIC |----------|-----|
-- MAGIC |reference_id| BIGINT FK (from reference table)|
-- MAGIC |barcode_seq |BIGINT FK (from barcode table)|
-- MAGIC |gene_seq| BIGINT FK (from genes table)|
-- MAGIC |count |BIGINT|
-- MAGIC
-- MAGIC **genes table**
-- MAGIC |field name| type|
-- MAGIC |----------|-----|
-- MAGIC |reference_id |BIGINT FK (from reference table)|
-- MAGIC |gene_seq |BIGINT |
-- MAGIC |gene_name|STRING |
-- MAGIC |ensemble_gencode|STRING|
-- MAGIC
-- MAGIC **barcode table**
-- MAGIC |field name| type|
-- MAGIC |----------|-----|
-- MAGIC |reference_id| BIGINT FK (from reference table)|
-- MAGIC |barcode_seq| BIGINT |
-- MAGIC |barcode| STRING|
-- MAGIC
-- MAGIC
-- MAGIC **NOTE** The code here is to explain the patterns and provide guidance that could be used to implement the final solution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Single Cell RNA DB Model
-- MAGIC
-- MAGIC The matrix data will look something like this
-- MAGIC
-- MAGIC ```
-- MAGIC 33694 x 737280 sparse Matrix of class "dgTMatrix", with 8520816 entries 
-- MAGIC          i   j   x
-- MAGIC 1    15891   1   1
-- MAGIC 2    31764   3   1
-- MAGIC 3    10737   5   1
-- MAGIC 4    33659   6   1
-- MAGIC 5    17291  10   1
-- MAGIC 6    31764  12   1
-- MAGIC 7      896  13   1
-- MAGIC 8     2171  13   1
-- MAGIC 9     3958  13   1
-- MAGIC ........
-- MAGIC ........
-- MAGIC 3330 11908 156  23
-- MAGIC 3331 11964 156   2
-- MAGIC 3332 12036 156   1
-- MAGIC 3333 12147 156   1
-- MAGIC ........
-- MAGIC ........
-- MAGIC 8520816 23323 233434 44
-- MAGIC ```
-- MAGIC
-- MAGIC The Columns correspond to Genes and Rows corresponds to Barcodes
-- MAGIC  
-- MAGIC Gene data is as below
-- MAGIC
-- MAGIC ```
-- MAGIC                  V1               V2
-- MAGIC 1    ENSG00000243485     RP11-34P13.3
-- MAGIC 2    ENSG00000237613          FAM138A
-- MAGIC 3    ENSG00000186092            OR4F5
-- MAGIC 4    ENSG00000238009     RP11-34P13.7
-- MAGIC 5    ENSG00000239945     RP11-34P13.8
-- MAGIC 6    ENSG00000239906    RP11-34P13.14
-- MAGIC 7    ENSG00000241599     RP11-34P13.9
-- MAGIC 8    ENSG00000279928       FO538757.3
-- MAGIC 9    ENSG00000279457       FO538757.2
-- MAGIC 10   ENSG00000228463       AP006222.2
-- MAGIC 11   ENSG00000236743    RP5-857K21.15
-- MAGIC 12   ENSG00000236601     RP4-669L17.2
-- MAGIC 13   ENSG00000237094    RP4-669L17.10
-- MAGIC 14   ENSG00000278566           OR4F29
-- MAGIC 15   ENSG00000230021     RP5-857K21.4
-- MAGIC ........
-- MAGIC ........
-- MAGIC ```
-- MAGIC Bar code data is as below
-- MAGIC
-- MAGIC ```
-- MAGIC                      V1
-- MAGIC 1     AAACCTGAGAAACCAT-1
-- MAGIC 2     AAACCTGAGAAACCGC-1
-- MAGIC 3     AAACCTGAGAAACCTA-1
-- MAGIC 4     AAACCTGAGAAACGAG-1
-- MAGIC 5     AAACCTGAGAAACGCC-1
-- MAGIC 6     AAACCTGAGAAAGTGG-1
-- MAGIC 7     AAACCTGAGAACAACT-1
-- MAGIC 8     AAACCTGAGAACAATC-1
-- MAGIC 9     AAACCTGAGAACTCGG-1
-- MAGIC 10    AAACCTGAGAACTGTA-1
-- MAGIC 11    AAACCTGAGAAGAAGC-1
-- MAGIC 12    AAACCTGAGAAGATTC-1
-- MAGIC 13    AAACCTGAGAAGCCCA-1
-- MAGIC 14    AAACCTGAGAAGGACA-1
-- MAGIC 15    AAACCTGAGAAGGCCT-1
-- MAGIC 16    AAACCTGAGAAGGGTA-1
-- MAGIC 17    AAACCTGAGAAGGTGA-1
-- MAGIC 18    AAACCTGAGAAGGTTT-1
-- MAGIC 19    AAACCTGAGAATAGGG-1
-- MAGIC ........
-- MAGIC ........
-- MAGIC ```
-- MAGIC  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The idea is to store the i, j, x values of the sparse matrix in the **matrix** table. The i and j corresponds to the row and column sequence number for the value thats being captured. 
-- MAGIC The actual Gene and Barcodes corresponding to the column and row sequences are stored in the **genes** and **barcodes** table. The **gene_seq** and **barcode_seq** fields will correspond to the indexes being captured in **matrix** table

-- COMMAND ----------

USE CATALOG hive_metastore;

-- COMMAND ----------

DROP SCHEMA IF EXISTS single_cell CASCADE;
CREATE SCHEMA single_cell;

-- COMMAND ----------

USE SCHEMA single_cell;

-- COMMAND ----------

DROP TABLE IF EXISTS single_cell.reference;

CREATE TABLE single_cell.reference (
  reference_id BIGINT GENERATED ALWAYS AS IDENTITY,
  name STRING,
  description STRING,
  assay_id STRING,
  mlflow_experiment_uri STRING,
  data_file_path STRING,
  tags STRING,
  update_dt TIMESTAMP
);

-- COMMAND ----------

DROP TABLE IF EXISTS single_cell.matrix;

CREATE TABLE single_cell.matrix (
  reference_id BIGINT,
  barcode_seq BIGINT,  
  gene_seq BIGINT,
  count BIGINT
);

OPTIMIZE single_cell.matrix ZORDER BY (reference_id);

-- COMMAND ----------

DROP TABLE IF EXISTS single_cell.genes;

CREATE TABLE single_cell.genes (
  reference_id BIGINT,
  gene_seq BIGINT,  
  gene_name STRING,
  ensemble_gencode STRING
) 

-- COMMAND ----------

DROP TABLE IF EXISTS single_cell.barcode;

CREATE TABLE single_cell.barcode (
  reference_id BIGINT,
  barcode_seq BIGINT,  
  barcode STRING
) 

-- COMMAND ----------

-- DBTITLE 1,Autoloader related
-- MAGIC %sh
-- MAGIC rm -rf /dbfs/Users/srijit.nair@databricks.com/field_demos/checkpoints/eisai/single_cell/autoloader

-- COMMAND ----------

DROP TABLE IF EXISTS single_cell.dna_nexus_files;

CREATE TABLE single_cell.dna_nexus_files (   
  path STRING,   
  filenames ARRAY<STRING>,
  processed BOOLEAN,
  processedTime TIMESTAMP
);



-- COMMAND ----------


