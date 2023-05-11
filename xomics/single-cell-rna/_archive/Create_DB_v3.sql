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
-- MAGIC |expression_type|STRING|
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


### Single Cell RNA DB Model

The matrix data will look something like this

```
33694 x 737280 sparse Matrix of class "dgTMatrix", with 8520816 entries 
         i   j   x
1    15891   1   1
2    31764   3   1
3    10737   5   1
4    33659   6   1
5    17291  10   1
6    31764  12   1
7      896  13   1
8     2171  13   1
9     3958  13   1
........
........
3330 11908 156  23
3331 11964 156   2
3332 12036 156   1
3333 12147 156   1
........
........
8520816 23323 233434 44
```

The Columns correspond to Barcodes and Rows corresponds to Genes
Gene data is as below

```
                 V1               V2
1    ENSG00000243485     RP11-34P13.3
2    ENSG00000237613          FAM138A
3    ENSG00000186092            OR4F5
4    ENSG00000238009     RP11-34P13.7
5    ENSG00000239945     RP11-34P13.8
6    ENSG00000239906    RP11-34P13.14
7    ENSG00000241599     RP11-34P13.9
8    ENSG00000279928       FO538757.3
9    ENSG00000279457       FO538757.2
10   ENSG00000228463       AP006222.2
11   ENSG00000236743    RP5-857K21.15
12   ENSG00000236601     RP4-669L17.2
13   ENSG00000237094    RP4-669L17.10
14   ENSG00000278566           OR4F29
15   ENSG00000230021     RP5-857K21.4
........
........
```
Bar code data is as below

```
                     V1
1     AAACCTGAGAAACCAT-1
2     AAACCTGAGAAACCGC-1
3     AAACCTGAGAAACCTA-1
4     AAACCTGAGAAACGAG-1
5     AAACCTGAGAAACGCC-1
6     AAACCTGAGAAAGTGG-1
7     AAACCTGAGAACAACT-1
8     AAACCTGAGAACAATC-1
9     AAACCTGAGAACTCGG-1
10    AAACCTGAGAACTGTA-1
11    AAACCTGAGAAGAAGC-1
12    AAACCTGAGAAGATTC-1
13    AAACCTGAGAAGCCCA-1
14    AAACCTGAGAAGGACA-1
15    AAACCTGAGAAGGCCT-1
16    AAACCTGAGAAGGGTA-1
17    AAACCTGAGAAGGTGA-1
18    AAACCTGAGAAGGTTT-1
19    AAACCTGAGAATAGGG-1
........
........
```
 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The idea is to store the i, j, x values of the sparse matrix in the **matrix** table. The i and j corresponds to the row and column sequence number for the value thats being captured. 
-- MAGIC The actual Gene and Barcodes corresponding to the column and row sequences are stored in the **genes** and **barcodes** table. The **gene_seq** and **barcode_seq** fields will correspond to the indexes being captured in **matrix** table

-- COMMAND ----------

USE CATALOG hive_metastore;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #spark.sql('use databricks_metastore')
-- MAGIC spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

-- COMMAND ----------


show databases

-- COMMAND ----------

DROP SCHEMA IF EXISTS poc_single_cell CASCADE;

-- COMMAND ----------

DROP SCHEMA IF EXISTS poc_single_cell CASCADE;
CREATE SCHEMA poc_single_cell;

-- COMMAND ----------

USE  poc_single_cell;

-- COMMAND ----------

DROP TABLE IF EXISTS reference;

CREATE TABLE reference (
  reference_id BIGINT GENERATED ALWAYS AS IDENTITY,
  study_id STRING,
  assay_id STRING,
  data_type STRING,
  assay_platform STRING,
  update_dt TIMESTAMP
);

-- COMMAND ----------

DROP TABLE IF EXISTS poc_single_cell.matrix;

CREATE TABLE poc_single_cell.matrix (
  reference_id BIGINT,
  gene_seq BIGINT,
  barcode_seq BIGINT,
  count BIGINT
);

OPTIMIZE poc_single_cell.matrix ZORDER BY (reference_id);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESCRIBE DETAIL poc_single_cell.matrix

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC OPTIMIZE poc_single_cell.matrix ZORDER BY (reference_id);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESCRIBE DETAIL poc_single_cell.matrix

-- COMMAND ----------

DROP TABLE IF EXISTS poc_single_cell.genes;

CREATE TABLE poc_single_cell.genes (
  reference_id BIGINT,
  gene_seq BIGINT,  
  gene_name STRING,
  ensemble_gencode STRING,
  expression_type STRING
) 

-- COMMAND ----------

-- MAGIC %sql DESCRIBE DETAIL poc_single_cell.genes

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC OPTIMIZE poc_single_cell.genes ZORDER BY (reference_id);

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC DESCRIBE DETAIL poc_single_cell.genes

-- COMMAND ----------

DROP TABLE IF EXISTS poc_single_cell.barcode;

CREATE TABLE poc_single_cell.barcode (
  reference_id BIGINT,
  barcode_seq BIGINT,  
  barcode STRING
) 

-- COMMAND ----------

-- MAGIC %sql DESCRIBE DETAIL poc_single_cell.barcode

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC OPTIMIZE poc_single_cell.barcode ZORDER BY (reference_id);

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC DESCRIBE DETAIL poc_single_cell.barcode

-- COMMAND ----------

-- DBTITLE 1,Autoloader related
-- MAGIC %sh
-- MAGIC rm -rf /dbfs/Users/srijit.nair@databricks.com/field_demos/checkpoints/eisai/single_cell/autoloader

-- COMMAND ----------

DROP TABLE IF EXISTS poc_single_cell.dna_nexus_files;

CREATE TABLE poc_single_cell.dna_nexus_files (   
  path STRING,   
  filenames ARRAY<STRING>,
  processed BOOLEAN,
  processedTime TIMESTAMP
);



-- COMMAND ----------


