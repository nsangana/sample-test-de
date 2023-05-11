# Databricks notebook source
# MAGIC %md ---
# MAGIC title: Part 1, ETL to prep data for training a disease classifier on RNA-seq data
# MAGIC authors:
# MAGIC - William Brandler
# MAGIC - Wes Dias
# MAGIC tags:
# MAGIC - ETL
# MAGIC - pyspark
# MAGIC - python
# MAGIC - HLS
# MAGIC - genomics
# MAGIC - RNASeq
# MAGIC created_at: 2019-10-08
# MAGIC updated_at: 2021-04-26
# MAGIC tldr: Compare and contrast spark-ml with scikit-learn
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Links
# MAGIC - AWS demo.cloud: [https://demo.cloud.databricks.com/#notebook/4193227](https://demo.cloud.databricks.com/#notebook/4193227)

# COMMAND ----------

# MAGIC %md
# MAGIC ## <img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/>
# MAGIC
# MAGIC ## Train a disease classifier on RNA-seq data
# MAGIC
# MAGIC ## With tracking from <img width="90px" src="https://mlflow.org/docs/0.7.0/_static/MLflow-logo-final-black.png">
# MAGIC
# MAGIC ## Part 1: ETL with ![](https://a0x8o.github.io/images/spark/Apache_Spark_logo_320px.png) Spark
# MAGIC
# MAGIC Here we prepare RNA-seq data from the [cancer cell line encyclopedia (CCLE)](https://portals.broadinstitute.org/ccle) for machine learning with spark-sklearn and spark-ml
# MAGIC
# MAGIC [**Part 1**](https://demo.cloud.databricks.com/#notebook/4193227/command/4193230) | [Part 2](https://demo.cloud.databricks.com/#notebook/4192936/command/4192939) | [Part 3](https://demo.cloud.databricks.com/#notebook/4193271/command/4193274)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setup
# MAGIC
# MAGIC - Tested on Databricks Runtime 8.0 ML

# COMMAND ----------

from pyspark.sql.functions import udf, col, row_number, sort_array, struct
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Paths

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/ml/tmp/KnowledgeRepo/rnaseq")

# COMMAND ----------

expression_path = "dbfs:/mnt/databricks-datasets-private/HLS/rnaseq/tpms.tsv"
metadata_path = "dbfs:/mnt/databricks-datasets-private/HLS/rnaseq/ccle_metadata.csv"
transcripts_index_outpath = "dbfs:/ml/tmp/KnowledgeRepo/rnaseq/transcripts_index.parquet"
features_outpath = "dbfs:/ml/tmp/KnowledgeRepo/rnaseq/rnaseq_features_with_metadata.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://a0x8o.github.io/images/spark/Apache_Spark_logo_320px.png) 
# MAGIC ## Load in CCLE data into Spark Dataframe
# MAGIC
# MAGIC The gene expression comes in as a TSV file. Spark natively understands how to read a large number of file formats, such as CSV, JSON, Parquet and Delta
# MAGIC
# MAGIC Data originally downloaded from [here](https://www.ebi.ac.uk/gxa/experiments-content/E-MTAB-2770/resources/ExperimentDownloadSupplier.RnaSeqBaseline/tpms.tsv)

# COMMAND ----------

raw_expression = spark.read.csv(expression_path, sep="\t", header=True)
first_few_columns = raw_expression.columns[:2] + raw_expression.columns[5:8]
display(raw_expression[first_few_columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://a0x8o.github.io/images/spark/Apache_Spark_logo_320px.png) 
# MAGIC ## Downsample data

# COMMAND ----------

raw_expression = raw_expression.sample(False, 0.05, 42)

# COMMAND ----------

raw_expression.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://a0x8o.github.io/images/spark/Apache_Spark_logo_320px.png) 
# MAGIC ## Transpose the data to prepare it for machine learning

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Clean up the cell line IDs
# MAGIC
# MAGIC The original file has lumped the disease with the cell line ID
# MAGIC
# MAGIC Parse the columns and then clean up

# COMMAND ----------

raw_expression_cols = raw_expression.columns
cell_line_names = [x.split(', ')[1] for x in raw_expression_cols[2:]]
new_column_names = ["Gene ID", "Gene Name"] + cell_line_names
print('Here are some of the cell lines:\n' + str(cell_line_names[2:10]))

# COMMAND ----------

raw_expression_cols_backticks = ['`' + x + '`' for x in raw_expression_cols]
mapping = dict(zip(raw_expression_cols_backticks, new_column_names))
raw_expression = raw_expression.select([col(c).alias(mapping.get(c, c)) for c in raw_expression_cols_backticks])
display(raw_expression)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Map gene expression data for each cell line into a single row
# MAGIC
# MAGIC To do this use the RDD API and flatMap()
# MAGIC
# MAGIC Note: the RDD API allows you to execute arbitary Scala/Java/Python/R code on each element of your dataset. For the parsing that we want to run on this row, we need the ability to run an arbitrary python function.

# COMMAND ----------

def parse_expression(row):
  transcript = row[0]
  gene = row[1]
  cell_line_id = 2
  cell_line_tuples = []
  for cell_line in cell_line_names:    
    cell_line_gene_expr = row[cell_line_id]
    if cell_line_gene_expr is None:
      cell_line_tuples.append((gene, transcript, cell_line, 0.0))
    else:
      cell_line_tuples.append((gene, transcript, cell_line, float(cell_line_gene_expr)))
    cell_line_id += 1
    
  return cell_line_tuples

expression = raw_expression.rdd.flatMap(parse_expression).toDF(['gene_id', 'transcript_id', 'cell_line', 'tpkm'])
display(expression)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### assign an index to each transcript
# MAGIC
# MAGIC Using a window function to get the row number of a Spark SQL dataframe

# COMMAND ----------

transcripts = expression.select("gene_id", "transcript_id").distinct()
transcripts_with_index = transcripts.withColumn("index", row_number().over(Window.orderBy("transcript_id")) - 1)
display(transcripts_with_index)

# COMMAND ----------

transcripts_with_index.coalesce(1). \
                       write. \
                       mode("overwrite"). \
                       parquet(transcripts_index_outpath)

# COMMAND ----------

expression_with_index = expression.join(transcripts_with_index.drop("gene_id"), "transcript_id")
display(expression_with_index)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Aggregate all expressed transcripts per cell line into a single row

# COMMAND ----------

expression_struct = expression_with_index.withColumn("count_with_id", struct(expression_with_index.index, expression_with_index.tpkm))

expression_vectors = expression_struct.groupBy("cell_line"). \
                                       agg({"count_with_id": "collect_list"}). \
                                       withColumnRenamed("collect_list(count_with_id)", "expression_vector")

features = expression_vectors.withColumn("features", sort_array(expression_vectors.expression_vector))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Import the CCLE [Cell Line Annotations](https://portals.broadinstitute.org/ccle/data) metadata

# COMMAND ----------

metadata = spark.read.option("header", True).csv(metadata_path)
display(metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Assign disease class to each cell line

# COMMAND ----------

features_with_metadata = features.join(metadata, features.cell_line == metadata["Cell line primary name"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Filter diseases with low counts

# COMMAND ----------

display(features_with_metadata.groupBy("Histology").count().sort(col("count").desc()))

# COMMAND ----------

hisology_filter = ["carcinoma", 
                   "lymphoid_neoplasm", 
                   "malignant_melanoma", 
                   "glioma", 
                   "haematopoietic_neoplasm"
                  ]

features_with_metadata = features_with_metadata.where(col("Histology").isin(hisology_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Write to parquet
# MAGIC
# MAGIC Note: we train the model using this data in the subsequent notebooks

# COMMAND ----------

features_with_metadata.select("cell_line", "expression_vector", "features", "Histology"). \
                       coalesce(1). \
                       write. \
                       mode("overwrite"). \
                       parquet(features_outpath)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean up

# COMMAND ----------

dbutils.fs.rm("dbfs:/ml/tmp/KnowledgeRepo/rnaseq", True)
