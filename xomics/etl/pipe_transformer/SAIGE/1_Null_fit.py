# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Fit the null logistic mixed model on the driver
# MAGIC
# MAGIC Select a large driver instance with no workers, and adjust the number of threads accordingly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the null fit
# MAGIC
# MAGIC On a single-node 32 cores virtual machine this took 11 minutes

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC cd /opt/SAIGE
# MAGIC
# MAGIC Rscript extdata/step1_fitNULLGLMM.R     \
# MAGIC         --plinkFile=/dbfs/home/william.brandler@databricks.com/data/1kg/omni25_family/family \
# MAGIC         --phenoFile=/dbfs/home/william.brandler@databricks.com/data/1kg/sampleDiabetesPcs.csv \
# MAGIC         --phenoCol=type2Diabetes \
# MAGIC         --covarColList=PC0,PC1 \
# MAGIC         --sampleIDColinphenoFile=sampleId \
# MAGIC         --traitType=binary \
# MAGIC         --outputPrefix=/dbfs/home/william.brandler@databricks.com/data/1kg/step-1/output \
# MAGIC         --nThreads=32 \
# MAGIC         --IsOverwriteVarianceRatioFile=TRUE \
# MAGIC         --LOCO=FALSE

# COMMAND ----------

# DBTITLE 1,check the output
# MAGIC %sh
# MAGIC ls /dbfs/home/william.brandler@databricks.com/data/1kg/step-1
