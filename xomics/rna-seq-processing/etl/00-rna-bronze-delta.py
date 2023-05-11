# Databricks notebook source
# MAGIC %md
# MAGIC # TCGA: Data Ingest
# MAGIC ---
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 0px;">
# MAGIC   <img src="https://amir-hls.s3.us-east-2.amazonaws.com/public/TCGA.png" width="500">
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC * **The Cancer Genome Atlas (TCGA)**, a landmark cancer genomics program, molecularly characterized over 20,000 primary cancer and matched normal samples spanning 33 cancer types. This joint effort between the National Cancer Institute and the National Human Genome Research Institute began in 2006, bringing together researchers from diverse disciplines and multiple institutions.
# MAGIC
# MAGIC * Over the next dozen years, TCGA generated over **2.5 petabytes of genomic, epigenomic, transcriptomic, and proteomic data**. The data, which has already lead to improvements in our ability to diagnose, treat, and prevent cancer, is publicly available for anyone in the research community to use. 
# MAGIC
# MAGIC In this notebook we ingest, **gene exprssion profiles and clinical records** from [TCGA](https://portal.gdc.cancer.gov/) that has been downloaded using `./gdc-client` [data transfer tools](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool) and create a gene expression deltalake.

# COMMAND ----------

aws_bucket_name = "tcga-2-open"
mount_name = "rna-tcga-2-open"
dbutils.fs.mount(f"s3a://{aws_bucket_name}" , f"/mnt/{mount_name}")

# COMMAND ----------

mount_name = "thrusa-tcga-2-open"
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

import numpy as np
import pandas as pd
import os

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC Specify paths

# COMMAND ----------

# DBTITLE 0,paths to raw data
# user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
raw_input_path='s3://hls-eng-data/genomics/'
expression_path = raw_input_path+'/expression/'
sample_sheet_path = raw_input_path+'/gdc_sample_sheet.2020-02-20.tsv'
metadata_path = raw_input_path+'/metadata.cart.2020-02-26.json'
clinical_path = raw_input_path+'/clinical.tsv'
delta_root_path = 'dbfs:/home/amir.kermany@databricks.com/delta/genomics/tcga'

# COMMAND ----------

os.environ["EXPRESSION"] = expression_path.replace('dbfs:','/dbfs')
os.environ["SAMPLESHEET"]=sample_sheet_path.replace('dbfs:','/dbfs')
os.environ["METADATA"]=metadata_path.replace('dbfs:','/dbfs')
os.environ["CLINICAL"]=clinical_path.replace('dbfs:','/dbfs')

# COMMAND ----------

display(dbutils.fs.ls(raw_input_path))

# COMMAND ----------

# MAGIC %md
# MAGIC create delta path (if not exist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Raw Data Ingest

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2.1 Read gene expression profiles

# COMMAND ----------

expr_files=dbutils.fs.ls(expression_path)
display(expr_files)

# COMMAND ----------

import os
sample_path=dbutils.fs.ls(expr_files[0].path)[0].path
os.environ['SAMPLE_FILE']=dbutils.fs.ls(expr_files[0].path)[0].name
dbutils.fs.cp(sample_path,'/tmp/')

# COMMAND ----------

# MAGIC %sh
# MAGIC zcat /dbfs/tmp/$SAMPLE_FILE | head

# COMMAND ----------

# MAGIC %md
# MAGIC Load all 9000 files into a spark dataframe

# COMMAND ----------

schema = StructType([
  StructField('gene_id', StringType(), True),
  StructField('counts', IntegerType(), True)
  ])

df_expression=(
    spark.read.csv(expression_path,sep='\t',schema=schema, comment='_')
    .withColumn('_file', substring_index(input_file_name(),'/',-2)) # extracting file_id from full path
    .withColumn('id',substring_index(col('_file'), '/', 1))
    .drop('_file')
    .cache()
)
df_expression.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Load metadata

# COMMAND ----------

df_sample_sheet = (
  spark.read.csv(sample_sheet_path,sep='\t',header=True)
  .selectExpr("`File ID` AS file_id",
              "`Project ID` AS project_id",
              "`Case ID` AS case_id",
              "`Sample Type` AS sample_type",
              "`Sample ID` AS sample_id")
)

df_clinical = spark.read.csv(clinical_path,sep='\t',header=True)
print("sample sheet count:%s\nclinical data count:%s\n"%(df_sample_sheet.count(),df_clinical.count()))

# COMMAND ----------

display(df_sample_sheet.limit(5))

# COMMAND ----------

display(df_clinical)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Similary you can load metadata stored in `json` format into spark dataframes

# COMMAND ----------

df_metadata=spark.read.json(metadata_path, multiLine=True)

# COMMAND ----------

display(df_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Some quick data exploration

# COMMAND ----------

# MAGIC %md
# MAGIC What is the breakdown of samples by project? 

# COMMAND ----------

display(
  df_clinical.groupBy('project_id').count().orderBy(desc('count'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC how big are the raw BAM files? 

# COMMAND ----------

display(
  df_metadata
  .select(explode('analysis.input_files').alias('input_files'))
  .select('input_files.data_format','input_files.platform','input_files.file_size')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Write to Delta Bronze Table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.1 Write Expression Data

# COMMAND ----------

dbutils.fs.mkdirs(os.path.join(delta_bronze_path,'expression'))

# COMMAND ----------

(
  df_expression
  .write.format('delta')
  .option("userMetadata", "creating-expression-deltatable")
  .mode('overwrite')
  .save(os.path.join(delta_bronze_path,'expression'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Optimize Delta

# COMMAND ----------

spark.sql("OPTIMIZE delta.`{}`".format(os.path.join(delta_bronze_path,'expression')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.2 Write metadata to Delta

# COMMAND ----------

df_sample_sheet.printSchema()

# COMMAND ----------

dbutils.fs.mkdirs(os.path.join(delta_bronze_path,'gdc_sample_sheet'))

# COMMAND ----------

(
  df_sample_sheet
  .write.format('delta')
  .option("userMetadata", "creating-sample-sheet-deltatable")
  .partitionBy('project_id')
   .mode('overwrite')
  .save(os.path.join(delta_bronze_path,'gdc_sample_sheet'))
)

# COMMAND ----------

dbutils.fs.mkdirs(os.path.join(delta_bronze_path,'clinical'))

# COMMAND ----------

(
  df_clinical
  .write.format('delta')
  .mode('overwrite')
  .save(os.path.join(delta_bronze_path,'clinical'))
)

# COMMAND ----------

dbutils.fs.mkdirs(os.path.join(delta_bronze_path,'metadata'))

# COMMAND ----------

(
  df_metadata.write.format('delta')
  .mode('overwrite')
  .save(os.path.join(delta_bronze_path,'metadata'))
)

# COMMAND ----------


