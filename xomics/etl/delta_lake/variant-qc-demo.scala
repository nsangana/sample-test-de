// Databricks notebook source
val path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
val df = spark.read.format("com.databricks.vcf")
  .option("flattenInfoFields", true)
  .load(path)

// COMMAND ----------

display(df.drop("genotypes").limit(10))

// COMMAND ----------

import org.apache.spark.sql.functions._
display(df.where(expr("INFO_SVTYPE is not null")).selectExpr("*", "expand_struct(call_summary_stats(genotypes))").drop("genotypes").limit(10))

// COMMAND ----------

display(df.selectExpr("expand_struct(hardy_weinberg(genotypes))").limit(10))

// COMMAND ----------

display(df.selectExpr("expand_struct(dp_summary_stats(genotypes))"))

// COMMAND ----------

display(df.selectExpr("expand_struct(gq_summary_stats(genotypes))"))
