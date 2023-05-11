# Databricks notebook source
# MAGIC %md
# MAGIC ##### subset delta table to remove samples with missing covariates
# MAGIC
# MAGIC samples with missing (`NA`) phenotypes are removed on the fly during association testing

# COMMAND ----------

import pyspark.sql.functions as fx
import pandas as pd
import os
import glow
spark = spark.newSession()
glow.register(spark)

# COMMAND ----------

phenotypes_path = "/dbfs/FileStore/UKBB_PD_binary_phenotypes.csv"
delta_path = "dbfs:/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr21_chr22_full_af_filter.delta"
covariates_out_path = "/dbfs/mnt/fl60-ukbb-raw/downloaded/genetics/filtered_samples_covariates.csv"
phenotypes_out_path = "/dbfs/mnt/fl60-ukbb-raw/downloaded/genetics/filtered_samples_phenotypes.csv"
os.environ["delta_out_path"] = delta_out_path

# COMMAND ----------

df_sql = spark.sql('''SELECT * FROM ukbb.genotypes_genotyping_process_and_sample_qc''')

# COMMAND ----------

df_sql.count()

# COMMAND ----------

covariate_df = df_sql.select(['eid',
                              'genetic_principal_components_1',
                              'genetic_principal_components_2',
                              'genetic_principal_components_3',
                              'genetic_principal_components_4',
                              'genetic_principal_components_5',
                              'genetic_principal_components_6',
                              'genetic_principal_components_7',
                              'genetic_principal_components_8',
                              'genetic_principal_components_9',
                              'genetic_principal_components_10']). \
                      withColumnRenamed('eid', 'sample_id')

# COMMAND ----------

delta_df = spark.read.format("delta").load(delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### get sample ids
# MAGIC
# MAGIC [source code](https://glow.readthedocs.io/en/latest/api-docs/glowgr.html#glow.wgr.get_sample_ids)

# COMMAND ----------

sample_ids = glow.wgr.get_sample_ids(delta_df) 

# COMMAND ----------

len(sample_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC #### join to covariates

# COMMAND ----------

genotypes_sample_ids = pd.DataFrame(sample_ids, columns=['sample_id'])
genotypes_sample_ids['sample_id'] = genotypes_sample_ids['sample_id'].astype(str)

# COMMAND ----------

genotypes_sample_ids

# COMMAND ----------

covariate_pdf = covariate_df.toPandas()
covariate_pdf['sample_id'] = covariate_pdf['sample_id'].astype(str)

# COMMAND ----------

covariate_pdf

# COMMAND ----------

covariates_subset_pdf = pd.merge(genotypes_sample_ids, covariate_pdf, on='sample_id', how='left')

# COMMAND ----------

covariates_subset_pdf = covariates_subset_pdf.set_index('sample_id')
covariates_subset_pdf

# COMMAND ----------

#make sure no missing values
covariates_subset_pdf = covariates_subset_pdf.fillna(covariates_subset_pdf.mean())
covariates_subset_pdf = (covariates_subset_pdf - covariates_subset_pdf.mean())/covariates_subset_pdf.std()

# COMMAND ----------

covariates_subset_pdf.to_csv(covariates_out_path)

# COMMAND ----------

# MAGIC %sh
# MAGIC wc -l /dbfs/mnt/fl60-ukbb-raw/downloaded/genetics/filtered_samples_covariates.csv

# COMMAND ----------

# MAGIC %md
# MAGIC #### phenotypes

# COMMAND ----------

phenotypes_pdf = pd.read_csv(phenotypes_path)
phenotypes_pdf.rename(columns={'eid': 'sample_id'}, inplace=True)
phenotypes_pdf['sample_id'] = phenotypes_pdf['sample_id'].astype(str)
phenotypes_pdf

# COMMAND ----------

phenotypes_subset_pdf = pd.merge(genotypes_sample_ids, phenotypes_pdf, on='sample_id', how='left')
phenotypes_subset_pdf = phenotypes_subset_pdf.set_index('sample_id')
phenotypes_subset_pdf = phenotypes_subset_pdf.fillna('NA')
phenotypes_subset_pdf.to_csv(phenotypes_out_path)

# COMMAND ----------

phenotypes_subset_pdf

# COMMAND ----------

# MAGIC %sh
# MAGIC wc -l /dbfs/mnt/fl60-ukbb-raw/downloaded/genetics/filtered_samples_phenotypes.csv
