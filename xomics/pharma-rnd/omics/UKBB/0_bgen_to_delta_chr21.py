# Databricks notebook source
import pyspark.sql.functions as fx
import os
import glow
spark = spark.newSession()
glow.register(spark)

# COMMAND ----------

contigName = "21"
os.environ["contigName"] = contigName
bgen_path = "/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr" + contigName +"_v3.bgen"
sample_path = "/mnt/fl60-ukbb-raw/downloaded/genetics/ukb55647_imp_chr" + contigName +"_v3_s487296.sample"
delta_out_path = "/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr" + contigName +"_full_v3.delta"

# COMMAND ----------

bgen_df = spark.read.format("bgen"). \
                     option("useBgenIndex", True). \
                     option("sampleFilePath", sample_path). \
                     load(bgen_path)

# COMMAND ----------

display(bgen_df.drop("genotypes")) #500k genotypes, cannot be displayed!

# COMMAND ----------

(
  bgen_df
  .write
  .mode("overwrite")
  .format("delta")
  .save(delta_out_path)
)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr$contigName\_full_v3.delta

# COMMAND ----------

delta_df = spark.read.format("delta").load(delta_out_path)
delta_df.count()

# COMMAND ----------

display(delta_df.select("start", "referenceAllele", "alternateAlleles"))

# COMMAND ----------


