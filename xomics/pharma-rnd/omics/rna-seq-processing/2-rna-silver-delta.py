# Databricks notebook source
# MAGIC %md
# MAGIC # TCGA: Write to Silver
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
# MAGIC In this notebook we combine RNA profiles and teh available metadata (inclduding clinical data for each sample) and write the resulting tables into delta silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import os

# from pyspark.ml.linalg import Vectors, VectorUDT

import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 0,Specify Paths
user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
delta_bronze_path = 'dbfs:/home/{}/delta/genomics/rna/bronze'.format(user)
delta_silver_path = 'dbfs:/home/{}/delta/genomics/rna/silver'.format(user)
dbutils.fs.mkdirs(delta_silver_path)

# COMMAND ----------

display(dbutils.fs.ls(delta_bronze_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load from Delta

# COMMAND ----------

df_expression=spark.read.load(os.path.join(delta_bronze_path,'expression'))
df_sample_sheet = spark.read.load(os.path.join(delta_bronze_path,'gdc_sample_sheet'))
df_clinical = spark.read.load(os.path.join(delta_bronze_path,'clinical'))
df_metadata=spark.read.load(os.path.join(delta_bronze_path,'metadata'))

print('expressions with {} rows'.format(df_expression.count()))
print('gdc_sample_sheet with {} rows'.format(df_sample_sheet.count()))
print('clinical with {} rows'.format(df_clinical.count()))
print('metadata with {} rows'.format(df_metadata.count()))


# COMMAND ----------

display(df_expression.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Data cleanup and reshaping

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3.1 Create expression tables (sample per row) and add normalized counts 

# COMMAND ----------

w = Window.partitionBy("id")
df_expression_silver = (
  df_expression.repartition('id')
  .withColumn('_sd',stddev('counts').over(w))
  .withColumn('_avg',mean('counts').over(w))
  .withColumn('counts_stand',(col('counts')-col('_avg'))/col('_sd'))
  .drop('_sd','_avg')
  .groupBy('id').agg(collect_list('counts_stand').alias('norm_counts_arr'),collect_list('gene_id').alias('gene_id'),collect_list('counts').alias('counts_arr'))
  .cache()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3.2 Clean up metadata  

# COMMAND ----------

df_meta_selected= (
  df_metadata
  .select('file_id',explode('associated_entities').alias('entities'))
  .select('file_id','entities.case_id')
  .drop_duplicates()
)
df_meta_selected.count()

# COMMAND ----------

clincial_cols=['case_id0',
 'project_id',
 'ethnicity',
 'gender',
 'race',
 'vital_status',
 'year_of_birth',
 'age_at_diagnosis',
 'ajcc_clinical_m',
 'ajcc_clinical_n',
 'ajcc_clinical_stage',
 'ajcc_clinical_t',
 'ajcc_pathologic_m',
 'ajcc_pathologic_n',
 'ajcc_pathologic_stage',
 'ajcc_pathologic_t',
 'anaplasia_present',
 'anaplasia_present_type',
 'ann_arbor_pathologic_stage',
 'case_id40',
 'case_submitter_id',
 'category',
 'classification',
 'entity_id',
 'entity_type',
 'classification_of_tumor',
 'icd_10_code',
 'masaoka_stage',
 'morphology',
 'percent_tumor_invasion',
 'primary_diagnosis',
 'synchronous_malignancy',
 'tissue_or_organ_of_origin',
 'tumor_focality',
 'tumor_grade',
 'tumor_stage'
]
df_clinical_selected = df_clinical.select(clincial_cols).drop_duplicates()
df_clinical_selected.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3.3 Merge metadata and clinical table

# COMMAND ----------

df_clinical_meta = (
  df_clinical_selected
  .join(df_meta_selected,on=df_clinical_selected.case_id0==df_meta_selected.case_id)
  .drop('case_id',)
  .join(df_sample_sheet,on=['file_id','project_id'])
)
df_clinical_meta.count()

# COMMAND ----------

display(df_clinical_meta.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quick glance at expression patterns

# COMMAND ----------

display(df_expression_silver.selectExpr('id','counts_arr[0]','counts_arr[10]','counts_arr[20]','counts_arr[30]'))

# COMMAND ----------

display(df_expression_silver.selectExpr('id','norm_counts_arr[0]','norm_counts_arr[10]','norm_counts_arr[20]','norm_counts_arr[30]'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Write to Delta Silver Table

# COMMAND ----------

df_expression_silver.count()

# COMMAND ----------

(
  df_expression_silver
  .write.format('delta')
  .mode('overwrite')
  .save(os.path.join(delta_silver_path,'expression'))
)

# COMMAND ----------

(
  df_clinical_meta
  .write.format('delta')
  .mode('overwrite')
  .save(os.path.join(delta_silver_path,'clinical-meta'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 5.1 Optimize Tabels

# COMMAND ----------

spark.sql("OPTIMIZE delta.`{}/clinical-meta/`".format(delta_silver_path))

# COMMAND ----------

spark.sql("OPTIMIZE delta.`{}/expression/`".format(delta_silver_path))
