# Databricks notebook source
from random import sample
import os
import glow
spark = spark.newSession()
glow.register(spark)
from pyspark.sql.functions import *

# COMMAND ----------

contigName = "21"
os.environ["contigName"] = contigName
delta_path = "/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr" + contigName +"_full_v3.delta"
delta_out_path = "/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr" + contigName +"_full_af_filter_v3.delta"
af_cutoff = 0.01

# COMMAND ----------

delta_df = spark.read.format("delta").load(delta_path)

# COMMAND ----------

delta_df.count()

# COMMAND ----------

delta_filter_df = delta_df.select("*", glow.expand_struct(glow.call_summary_stats("genotypes"))). \
                           drop("callRate", "nCalled", "nUncalled", "nHet", "nHomozygous", "nNonRef", "nAllelesCalled", "alleleCounts"). \
                           where(array_min("alleleFrequencies") >= af_cutoff). \
                           drop("alleleFrequencies")

# COMMAND ----------

(
  delta_filter_df
  .write
  .mode("overwrite")
  .format("delta")
  .save(delta_out_path)
)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr$contigName\_v3.delta

# COMMAND ----------

delta_df = spark.read.format("delta").load(delta_out_path)
delta_df.count()

# COMMAND ----------

delta_df.printSchema()

# COMMAND ----------

display(delta_df.select("start", "referenceAllele", "alternateAlleles"))

# COMMAND ----------


