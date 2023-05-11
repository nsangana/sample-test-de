# Databricks notebook source
# MAGIC %md
# MAGIC ### RNA Expression Data Ingest and Analysis
# MAGIC In this notebook we ingest data from [TCGA](https://portal.gdc.cancer.gov/) that has been downloaded using `./gdc-client` [data transfer tools](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import os
import mlflow
# from pyspark.ml.linalg import Vectors, VectorUDT

import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Specify Paths
user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
delta_silver_path = 'dbfs:/home/{}/delta/genomics/rna/silver'.format(user)

# COMMAND ----------

display(dbutils.fs.ls(delta_silver_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Data

# COMMAND ----------

df_expression=spark.read.load(os.path.join(delta_silver_path,'expression'))
df_clinical_meta = spark.read.load(os.path.join(delta_silver_path,'clinical-meta'))


print('expressions with {} rows'.format(df_expression.count()))
print('df_clinical_meta with {} rows'.format(df_clinical_meta.count()))

# COMMAND ----------

# DBTITLE 1,clinical data associated with expressions
display(df_clinical_meta.limit(10))

# COMMAND ----------

display(df_expression.select('id',size('counts_arr'),array_max('counts_arr'),array_max('norm_counts_arr')))

# COMMAND ----------

selected_clinical_cols=[
 'file_id',
 'project_id',
 'ethnicity',
 'gender',
 'race',
 'category',
 'classification',
 'classification_of_tumor',
 'icd_10_code',
 'masaoka_stage',
 'primary_diagnosis',
 'tissue_or_organ_of_origin',
 'tumor_grade',
 'tumor_stage',
]

df_expression_and_clinical=(
  df_expression
  .join(
    df_clinical_meta.select(selected_clinical_cols),on=df_expression.id==df_clinical_meta.file_id)
)

# COMMAND ----------

expression_pdf=df_expression_and_clinical.select('id','norm_counts_arr').toPandas()
clinical_pdf=df_expression_and_clinical.select(selected_clinical_cols).toPandas()

# COMMAND ----------

n=len(expression_pdf.norm_counts_arr[0])
m=expression_pdf.shape[0]

# COMMAND ----------

expression_mat=np.concatenate(expression_pdf.norm_counts_arr.values,axis=0).reshape(m,n)
expression_mat.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Dimensionality Reduction Using UMAP 

# COMMAND ----------

from bokeh.plotting import figure, output_file, show
from bokeh.resources import CDN
from bokeh.embed import file_html

import umap
import umap.plot

# COMMAND ----------

mapper = umap.UMAP().fit(expression_mat)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create an interactive cluster browser

# COMMAND ----------

dbutils.widgets.dropdown(name='label',defaultValue='project_id',choices=selected_clinical_cols)
selected_lablel=dbutils.widgets.get('label')

# COMMAND ----------

hover_data = pd.DataFrame({'index':np.arange(m), 'label':clinical_pdf[selected_lablel]})
p = umap.plot.interactive(mapper, labels=clinical_pdf[selected_lablel], hover_data=hover_data, point_size=2)
html = file_html(p, CDN, "TCGA Expressions")

# COMMAND ----------

displayHTML(html)

# COMMAND ----------

delta_gold_path = 'dbfs:/home/amir.kermany@databricks.com/delta/genomics/rna/gold'
dbutils.fs.mkdirs(delta_gold_path)

# COMMAND ----------

(
  df_expression_and_clinical
  .write.format('delta')
  .mode('overwrite')
  .save(os.path.join(delta_gold_path,'expressions-clinical'))
)
