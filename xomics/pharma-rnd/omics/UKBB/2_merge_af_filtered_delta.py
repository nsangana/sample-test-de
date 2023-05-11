# Databricks notebook source
# MAGIC %md
# MAGIC ##### merge the bgens for chr21 & chr22

# COMMAND ----------

import pyspark.sql.functions as fx
import pandas as pd
import os
import glow
spark = spark.newSession()
glow.register(spark)

# COMMAND ----------

delta_path1 = "/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr21_full_af_filter_v3.delta"
delta_path2 = "/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr22_full_af_filter_v3.delta"
delta_out_path = "dbfs:/mnt/fl60-ukbb-raw/downloaded/genetics/ukb_imp_chr21_chr22_full_af_filter.delta"
os.environ["delta_out_path"] = delta_out_path

# COMMAND ----------

delta_df1 = spark.read.format("delta").load(delta_path1)
delta_df2 = spark.read.format("delta").load(delta_path2)

# COMMAND ----------

delta_df1.count()

# COMMAND ----------

delta_df2.count()

# COMMAND ----------

delta_df3 = delta_df1.unionAll(delta_df2)

# COMMAND ----------

(
  delta_df3
  .write
  .mode("overwrite")
  .format("delta")
  .save(delta_out_path)
)

# COMMAND ----------

spark.read.format("delta").load(delta_out_path).count()

# COMMAND ----------


