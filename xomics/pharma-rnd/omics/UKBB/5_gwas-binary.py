# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> + <img src="https://www.regeneron.com/sites/all/themes/regeneron_corporate/images/science/logo-rgc-color.png" alt="logo" width="240"/>
# MAGIC
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> GloWGR: genome-wide association study
# MAGIC
# MAGIC ### Logistic regression
# MAGIC
# MAGIC This notebook shows how to use the `logistic_regression` function in Glow to perform a genome-wide association study for binary traits. We incorporate the whole genome regression predictions to control for population structure and relatedness.

# COMMAND ----------

# MAGIC %pip install bioinfokit==0.8.5

# COMMAND ----------

import glow

import json
import numpy as np
import pandas as pd
import pyspark.sql.functions as fx

from matplotlib import pyplot as plt
from bioinfokit import visuz

spark = glow.register(spark)

# COMMAND ----------

variants_path = "dbfs:/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr21_chr22_full_af_filter.delta"
covariates_path = "/dbfs/mnt/fl60-ukbb-raw/downloaded/genetics/filtered_samples_covariates.csv"
binary_phenotypes_path = "/dbfs/mnt/fl60-ukbb-raw/downloaded/genetics/filtered_samples_phenotypes.csv"

y_hat_path = '/dbfs/mnt/fl60-ukbb-raw/downloaded/genetics/glow/wgr_y_binary_hat_chr21_chr22.csv'
gwas_results_path = 'dbfs:/mnt/fl60-ukbb-raw/downloaded/genetics/glow/wgr_log_reg_results_chr21_chr22.delta'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Load input data

# COMMAND ----------

base_variant_df = spark.read.format('delta').load(variants_path)

# COMMAND ----------

variant_df = (glow.transform('split_multiallelics', base_variant_df)
  .withColumn('values', glow.mean_substitute(glow.genotype_states('genotypes')))
  .filter(fx.size(fx.array_distinct('values')) > 1))

# COMMAND ----------

phenotype_df = pd.read_csv(binary_phenotypes_path, dtype={'sample_id': str}, index_col='sample_id')[['disease_id']]
phenotype_df.index = phenotype_df.index.map(str) #the integer representation of sample_ids was causing issues
phenotype_df

# COMMAND ----------

covariate_df = pd.read_csv(covariates_path, dtype={'sample_id': str}, index_col='sample_id')
covariate_df.index = covariate_df.index.map(str) #the integer representation of sample_ids was causing issues
covariate_df

# COMMAND ----------

offset_df = pd.read_csv(y_hat_path, dtype={'sample_id': str, 'contigName': str}).set_index(['sample_id', 'contigName'])
offset_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Run a genome-wide association study
# MAGIC
# MAGIC Note that Glow can run multiple contigs in a single command. However, for large cohorts, it's more performant to run one contig at a time.

# COMMAND ----------

contigs = ['21', '22']

# COMMAND ----------

for contig in contigs:
  results = glow.gwas.logistic_regression(
    variant_df.where(fx.col('contigName') == contig),
    phenotype_df,
    covariate_df,
    offset_df,
    values_column='values',
    contigs=[contig])
  
  # Write the results to a Delta Lake table partitioned by contigName
  results.write.format('delta').partitionBy('contigName').mode('append').save(gwas_results_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Inspect results

# COMMAND ----------

results_df = spark.read.format('delta').load(gwas_results_path)
display(results_df)

# COMMAND ----------

pdf = results_df.toPandas()

# COMMAND ----------

visuz.marker.mhat(pdf.loc[pdf.phenotype == 'disease_id', :], chr='contigName', pv='pvalue', show=True, gwas_sign_line=True)

# COMMAND ----------

display(results_df.select('contigName', 'start', 'end', 'names', 'pvalue').sort(fx.col("pvalue")))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Gnomad allele count (frequency) for rs775990051:
# MAGIC
# MAGIC 5/31384 (0.000159)
