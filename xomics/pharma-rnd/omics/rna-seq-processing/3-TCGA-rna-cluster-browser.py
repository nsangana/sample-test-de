# Databricks notebook source
# MAGIC %md
# MAGIC # Gene Profile Cluster Exploration with UMAP
# MAGIC ---
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 0px;">
# MAGIC   <img src="https://umap-learn.readthedocs.io/en/latest/_images/single_cell_umap.jpg" width="500">
# MAGIC </div>
# MAGIC
# MAGIC In this notebook we explore RNA profiles by creating an RNA cluster browser. We first use UMAP to create a 2D projection of RNA expressions and then explore clusters based on different classifications of the samples (e.g. icd-10-code, project_id, tissue-or-organ etc). Labels can be selected from a dropdown menue on the top left of this notebook. In addition, users can set UMPA paramters (`min_dist`, `n_neighbors`). All parameters are logged with MLFlow and resulting UMAP plot is also saved as an MLFlow artifact that can be later accessed. 

# COMMAND ----------

# MAGIC %pip install bokeh umap-learn umap-learn[plot]

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import os
import mlflow

import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 0,Specify Paths
user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
delta_silver_path = 'dbfs:/home/{}/delta/genomics/rna/silver'.format(user)
delta_gold_path = 'dbfs:/home/{}/delta/genomics/rna/gold'.format(user)
dbutils.fs.mkdirs(delta_gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Data from Silver Tables

# COMMAND ----------

df_expression=spark.read.format('delta').option("versionAsOf", 0).load(os.path.join(delta_silver_path,'expression'))
df_clinical_meta = spark.read.format('delta').option("versionAsOf", 0).load(os.path.join(delta_silver_path,'clinical-meta'))


print('expressions with {} rows'.format(df_expression.count()))
print('df_clinical_meta with {} rows'.format(df_clinical_meta.count()))

# COMMAND ----------

# DBTITLE 1,clinical data associated with expressions
display(df_clinical_meta.limit(10))

# COMMAND ----------

# DBTITLE 1,Distribution of read counts
display(df_expression.select('id',size('counts_arr'),array_max('counts_arr').alias('max_counts')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Get data ready for clustering and visualization

# COMMAND ----------

selected_clinical_cols=[
 'file_id',
 'project_id',
 'ethnicity',
 'gender',
 'race',
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

dbutils.widgets.text(name='n_neighbors',defaultValue='15')
dbutils.widgets.text(name='min_dist',defaultValue='0.1')
dbutils.widgets.dropdown(name='label',defaultValue='project_id',choices=selected_clinical_cols)

# COMMAND ----------

from bokeh.plotting import figure, output_file, show
from bokeh.resources import CDN
from bokeh.embed import file_html

import umap
import umap.plot

# COMMAND ----------

params ={'n_neighbors':int(dbutils.widgets.get('n_neighbors')),
        'min_dist':float(dbutils.widgets.get('min_dist')),
        'n_components':2,
        }
mapper = umap.UMAP(**params).fit(expression_mat)

mlflow.end_run()
for key,value in mapper.get_params().items():
  mlflow.log_param(key,value)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Interactive Gene Expression Cluster Browser

# COMMAND ----------

assert params['n_components']==2
with mlflow.start_run(nested=True) as run:
  selected_lablel=dbutils.widgets.get('label')
  hover_data = pd.DataFrame({'index':np.arange(m), 'label':clinical_pdf[selected_lablel]})
  p = umap.plot.interactive(mapper, labels=clinical_pdf[selected_lablel], hover_data=hover_data, point_size=2)
  html = file_html(p, CDN, "TCGA Expressions")
  dbutils.fs.put('/tmp/umap.html',html,True)
  mlflow.log_artifact('/dbfs/tmp/umap.html')
  mlflow.log_param('label',selected_lablel)
  displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write tables to delta gold path

# COMMAND ----------

(
  df_expression_and_clinical
  .write.format('delta')
  .mode('overwrite')
  .save(os.path.join(delta_gold_path,'expressions-clinical'))
)

# COMMAND ----------

dbutils.fs.ls(delta_gold_path)
