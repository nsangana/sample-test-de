# Databricks notebook source
library(sparklyr)

# COMMAND ----------

sc = spark_connect(method = "databricks")

# COMMAND ----------

sdf_sql(sc, "USE CATALOG hive_metastore")
sdf_sql(sc, "USE SCHEMA srijit_nair")
mm <- sdf_sql(sc, "select * from matrix")

# COMMAND ----------

#mm_part <- sdf_repartition( mm, 512)

# COMMAND ----------

mm_r <- collect(mm)

# COMMAND ----------


