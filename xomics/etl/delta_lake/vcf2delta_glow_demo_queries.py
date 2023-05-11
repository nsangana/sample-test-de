# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC
# MAGIC ### Write VCF files into <img width="175px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">  
# MAGIC
# MAGIC ### using Glow <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> 
# MAGIC
# MAGIC Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC
# MAGIC Delta Lake is applied in the following use cases in genomics:
# MAGIC
# MAGIC 1. joint-genotyping of population-scale cohorts
# MAGIC   - using GATK's GenotypeGVCFs ([blog](https://databricks.com/blog/2019/06/19/accurately-building-genomic-cohorts-at-scale-with-delta-lake-and-spark-sql.html))
# MAGIC   - or custom machine learning algorithms, for example for copy-number variants
# MAGIC 2. managing variant data in cloud data lakes ([blog](https://databricks.com/blog/2019/03/07/simplifying-genomics-pipelines-at-scale-with-databricks-delta.html))
# MAGIC   - once volumes reach thousands of VCF files 
# MAGIC 3. querying and statistical analysis of genotype data
# MAGIC   - once volumes reach billions of genotypes
# MAGIC   
# MAGIC This notebook processes chromosome 22 of the 1000 Genomes project directly from cloud storage into Delta Lake, with the following steps:
# MAGIC
# MAGIC   1. Read in pVCF as a Spark Data Source using Glow's VCF reader
# MAGIC   2. Write out DataFrame as a Delta Lake
# MAGIC   3. Query individual genotypes
# MAGIC   4. Subset the data randomly
# MAGIC   5. Write out as VCF (to show backwards compatibility with flat file formats)
# MAGIC
# MAGIC Tested on Databricks Runtime 6.5 [Genomics](https://docs.databricks.com/applications/genomics/hls-runtime.html)

# COMMAND ----------

import glow
glow.register(spark)
import pyspark.sql.functions as fx
from pyspark.sql.types import *
from random import sample

# COMMAND ----------

# MAGIC %md
# MAGIC #### set up paths

# COMMAND ----------

vcf_path = '/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz'
phenotypes_path = '/databricks-datasets/genomics/1000G/phenotypes.normalized'
delta_output_path = "dbfs:/home/william.brandler@databricks.com/genomics/delta/1kg-delta"
vcf_output_path_prefix = "dbfs:/home/william.brandler@databricks.com/genomics/vcf"
vcf_output_path = vcf_output_path_prefix + "/subset.vcf"

# COMMAND ----------

dbutils.fs.mkdirs(delta_output_path)
dbutils.fs.mkdirs(vcf_output_path_prefix)

# COMMAND ----------

# MAGIC %md
# MAGIC ####![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)  Read in pVCF as a [Spark Data Source](https://spark.apache.org/docs/latest/sql-data-sources.html) using Glow <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="30"/>
# MAGIC
# MAGIC [Glow](https://glow.readthedocs.io/en/latest/) is built into the Databricks Runtime for Genomics

# COMMAND ----------

# MAGIC %md 
# MAGIC Note: if there are multiple files, use a wildcard (*)

# COMMAND ----------

vcf = (
  spark
  .read
  .format("vcf")
  .load(vcf_path)
)

# COMMAND ----------

display(vcf)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Print the VCF schema
# MAGIC
# MAGIC __INFO__ fields are promoted to top level columns by default, with the prefix `INFO_`
# MAGIC
# MAGIC __FILTER__ fields are nested in the `filters` array
# MAGIC
# MAGIC __FORMAT__ fields are nested in the `genotypes` array

# COMMAND ----------

vcf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### explode variants to one genotype per row

# COMMAND ----------

vcf = vcf.withColumn("genotypes", fx.explode("genotypes")). \
          withColumn("sampleId", fx.col("genotypes.sampleId"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write out DataFrame to <img width="175px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC To append to existing Delta Lake use `.mode("append")`

# COMMAND ----------

(
  vcf
  .write
  .format("delta")
  .save(delta_output_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Describe history of the Delta table
# MAGIC
# MAGIC With Delta, you can 
# MAGIC
# MAGIC 1. append more data
# MAGIC 2. perform fine-grained deletes (for GDPR compliance)
# MAGIC 3. time travel back to previous versions of the table

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY delta.`dbfs:/home/william.brandler@databricks.com/genomics/delta/1kg-delta`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)  Read in the Delta Lake and count the number of variants

# COMMAND ----------

delta_vcf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perform point query to retrieve specific genotype
# MAGIC
# MAGIC get genotype for a specific sample at a specific position
# MAGIC
# MAGIC for faster queries, Z-order on contigName and position

# COMMAND ----------

sample_id = "HG00100"
chrom = "22"
start = 16050074

# COMMAND ----------

genotype = delta_vcf.where((fx.col("contigName") == chrom) & 
                           (fx.col("start") == start) &
                           (fx.col("sampleId") == sample_id)
                          ). \
                     select("contigName", "start", "sampleId", "genotypes.calls")

# COMMAND ----------

genotype.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### calculate stats on the data

# COMMAND ----------

delta_vcf.select("contigName", "start", "sampleId", "genotypes.calls"). \
          withColumn("genotype", fx.col("calls")[0].cast(IntegerType()) + fx.col("calls")[1].cast(IntegerType())). \
          groupBy("contigName", "start", "genotype"). \
          count(). \
          collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delete sample using Delta's fine-grained deletes

# COMMAND ----------

# Check to see what data we are going to delete
sample_filter = delta_vcf.where(fx.col("sampleId") == sample_id)

# COMMAND ----------

sample_filter.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### VCFs can be written as plain text or you can specify block gzip [compression](https://docs.databricks.com/applications/genomics/variant-data.html#vcf)

# COMMAND ----------

columns = sample_filter.columns
columns.remove("genotypes")
out_vcf = sample_filter.groupBy(columns). \
                        agg(fx.collect_list("genotypes").alias("genotypes")). \
                        drop("sampleId")

# COMMAND ----------

(
  out_vcf
  .write
  .format("bigvcf")
  .save(vcf_output_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using the [local file API](https://docs.databricks.com/data/databricks-file-system.html#local-file-apis), we can read VCF directly from cloud storage via the shell

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 200 /dbfs/home/william.brandler@databricks.com/genomics/vcf/subset.vcf

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean up

# COMMAND ----------

dbutils.fs.rm(delta_output_path, recurse=True)
dbutils.fs.rm(vcf_output_path, recurse=False)

# COMMAND ----------


