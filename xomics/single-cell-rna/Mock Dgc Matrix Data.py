# Databricks notebook source
spark.conf.set("spark.sql.files.maxPartitionBytes", (64 * 1024 * 1024))

# COMMAND ----------

df = spark.sql("select id from range(600000000)")

# COMMAND ----------

from pyspark.sql.functions import col, lit, rand, round

matrix = (df
  .withColumn("reference_id", lit(1))
  .withColumn("gene_seq", col('id')*lit(2))
  .withColumn("barcode_seq", col('id')*lit(3))
  .withColumn("count", round(rand()*1000))
  .drop("id")
)

# COMMAND ----------

display(matrix)

# COMMAND ----------

matrix.createOrReplaceTempView("matrix")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC USE SCHEMA srijit_nair;
# MAGIC
# MAGIC DROP TABLE IF EXISTS srijit_nair.matrix;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS srijit_nair.matrix (
# MAGIC   reference_id BIGINT,
# MAGIC   gene_seq BIGINT,
# MAGIC   barcode_seq BIGINT,
# MAGIC   count BIGINT
# MAGIC );
# MAGIC
# MAGIC
# MAGIC MERGE INTO srijit_nair.matrix AS target USING matrix AS source
# MAGIC ON target.reference_id = source.reference_id 
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM srijit_nair.matrix;

# COMMAND ----------


