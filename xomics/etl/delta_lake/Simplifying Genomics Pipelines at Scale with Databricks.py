# Databricks notebook source
# MAGIC %md
# MAGIC # Simplifying Genomics Pipelines at Scale with Databricks
# MAGIC
# MAGIC #<img width="800px" src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/UAP4G_architecture_glow2.png">

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook provides an example of how to simplify the operationalizing and productionization of genomics pipelines *at scale* with Databricks.
# MAGIC
# MAGIC ## Even simple questions are hard!
# MAGIC Genomic data is so large that even the following simple questions can takes to answer using legacy single node bioinformatics tools.
# MAGIC
# MAGIC - How many samples have been sequenced?
# MAGIC   - This week? 
# MAGIC   - This month?
# MAGIC - How many mutations are there in each individual?
# MAGIC - What classes of mutation do we observe across the cohort?
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ## Demonstrate genomics piplines at scale
# MAGIC Here we demo exome sequencing data streaming in real-time into Databricks Delta parquets and analyzing it _on the fly_...
# MAGIC
# MAGIC This notebook has been tested on Databricks runtime 4.3 (includes Apache Spark 2.3.1, Scala 2.11), using an i3.2xlarge driver and 8 workers of the same instance type.

# COMMAND ----------

# Import necessary libraries
import pyspark.sql.functions as fx
from pyspark.sql.types import StringType, IntegerType, ArrayType
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Configurations
sns.set(style="white")
spark.conf.set("spark.databricks.delta.preview.enabled", "true")
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# COMMAND ----------

# DBTITLE 0,functions
#
# Parsing functions
#

# Coding Mutations
def get_coding_mutations(df):
  df = df.where(fx.col("proteinHgvs").rlike("^[p\.]")). \
          where((fx.col("mutationType") == "nonsynonymous") | 
                (fx.col("mutationType") == "synonymous") | 
                (fx.col("effect") == "stop_gained"))
  return df

# Parse proteinHgvs 
def parse_proteinHgvs(hgvs):
  """
  parses proteinHgvs string into amino acid substitutions
  :param hgvs: str, proteinHgvs p.[codon1][position][codon2]
  :return: list with two amino acids (* if stop codon)
  """
  hgvs_list = list(hgvs)
  aa1 = "".join(hgvs_list[2:5])
  if (hgvs_list[-1] == "*"): # * = stop codon
    aa2 = hgvs_list.pop(-1)
  else:
    aa2 = "".join(hgvs_list[-3:]) # all other codons use 3-letter abbreviations
  return [aa1, aa2]

# Define parse_proteinHgvs as a UDF
parse_proteinHgvs_udf = fx.udf(parse_proteinHgvs, ArrayType(StringType()))

# Get amino acid substitutions
def get_amino_acid_substitutions(df, hgvs_col):
  """
  parse hgvs notation to get amino acid substitutions in a manageable format
  """
  df = df.withColumn("tmp", parse_proteinHgvs_udf(fx.col(hgvs_col))). \
          withColumn("reference", fx.col("tmp")[0]). \
          withColumn("alternate", fx.col("tmp")[1]). \
          drop("tmp", hgvs_col)
  return df

# Count amino acid substituion combinations
def count_amino_acid_substitution_combinations(df):
  df = df.groupBy("reference", "alternate").count(). \
          withColumnRenamed("count", "substitutions")
  return df

# COMMAND ----------

# MAGIC %md ## Initial setup
# MAGIC
# MAGIC To establish our pipeline, we will initially write a single `sampleId` exome Parquet file into a Databricks Delta table by:
# MAGIC * Creating and establishing the location of the Databricks Delta stream path, i.e. `delta_stream_outpath`
# MAGIC * Create a Spark job to read the single `sampleId` and write it out to the `delta_stream_outpath`

# COMMAND ----------

# Configure delta steam output path
delta_stream_outpath = "/tmp/dnaseq/annotations_etl_delta_stream/"

# Configure source of annotations_etl_parquet
annotations_etl_parquet_path = "/databricks-datasets/genomics/annotations_etl_parquet/"

# Configure single sampleId
single_sampleId_path = annotations_etl_parquet_path + "sampleId=SRS000030_SRR709972"

# Remove if directory exists and create it
dbutils.fs.rm(delta_stream_outpath, True)
dbutils.fs.mkdirs(delta_stream_outpath)

# COMMAND ----------

# Read single sampleId
spark.read.format("parquet").load(single_sampleId_path).write.format("delta").save(delta_stream_outpath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start streaming the Databricks Delta table
# MAGIC
# MAGIC **Important Notes**
# MAGIC * Wait for stream to initialize before running subsequent cells
# MAGIC * At first you will only see information for one sample...
# MAGIC * Later on we will start appending more samples to the Delta table and the tables and figures below will update in real-time

# COMMAND ----------

# MAGIC %md ## Start Answering our Simple Questions
# MAGIC Some of the initial questions we will ask within our Databricks Delta stream include:
# MAGIC * Show the variant counts
# MAGIC * Display the first 1000 rows of our Databricks Delta table
# MAGIC * Review the schema of our table

# COMMAND ----------

# Variant Counts
exomes = spark.readStream.format("delta").load(delta_stream_outpath)
display(exomes.groupBy("sampleId").count().withColumnRenamed("count", "variants"))

# COMMAND ----------

# Display the first 1000 rows of `exomes` Databricks Delta table
display(exomes)

# COMMAND ----------

# Review the schema of `exomes` Databricks Delta table
exomes.printSchema()

# COMMAND ----------

# MAGIC %md ## Determine the Single Nucleotide Variant Count
# MAGIC
# MAGIC To determine the Single Nucleotide Variant (SNV) count, we will:
# MAGIC * Create the `snvs` streaming DataFrame based on the referenceAllele and alternateAllele filtering criteria
# MAGIC * Execute a Spark SQL query to view the data in a bar graph
# MAGIC   * Note that this is a streaming Databricks Delta table thus as new data is added to the underlying `exomes` Databricks Delta table, the bar graph will continously update

# COMMAND ----------

snvs = exomes.where((fx.length(fx.col("referenceAllele")) == 1) & 
                    (fx.length(fx.col("alternateAllele")) == 1))
snvs.createOrReplaceTempView("snvs")

# COMMAND ----------

# MAGIC %sql
# MAGIC select referenceAllele, alternateAllele, count(1) as GroupCount 
# MAGIC   from snvs
# MAGIC  group by referenceAllele, alternateAllele
# MAGIC  order by GroupCount desc

# COMMAND ----------

# MAGIC %md ## What is our Variant Count?
# MAGIC To determine the count of mutation types, you can execute a `GROUP BY` statement against the `exomes` Databricks Delta table; this time let's use a donut chart.

# COMMAND ----------

display(exomes.groupBy("mutationType").count())

# COMMAND ----------

# MAGIC %md
# MAGIC <br/>
# MAGIC **NOTE:** If real-time updates are not necessary for your use-case, you could schedule jobs each day or week to track progress of sequencing runs
# MAGIC
# MAGIC <br/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build an Amino Acid Substitution Heat Map
# MAGIC
# MAGIC To build an Amino Acid Substituition Heat Map, we will use *pandas* and *matplotlib*
# MAGIC
# MAGIC **Important Notes**
# MAGIC * Wait for stream to initialize before running subsequent cells
# MAGIC * Plots using anything other than the built-in display() cannot be run on streaming tables
# MAGIC   * Therefore write the stream out to memory (or to parquet), then manually run the amino acid substitution heat map cell below

# COMMAND ----------

# Build amino acid counts stream
coding = get_coding_mutations(exomes)
aa_substitutions = get_amino_acid_substitutions(coding.select("proteinHgvs"), "proteinHgvs")
aa_counts = count_amino_acid_substitution_combinations(aa_substitutions)
aa_counts. \
  writeStream. \
  format("memory"). \
  queryName("amino_acid_substitutions"). \
  outputMode("complete"). \
  trigger(processingTime='60 seconds'). \
  start()

# COMMAND ----------

# Build Amino Acid Subtitution Heat Map
#   This heatmap uses pandas and matplotlib thus cannot run on streaming tables
amino_acid_substitutions = spark.read.table("amino_acid_substitutions")
max_count = amino_acid_substitutions.agg(fx.max("substitutions")).collect()[0][0]
aa_counts_pd = amino_acid_substitutions.toPandas()
aa_counts_pd = pd.pivot_table(aa_counts_pd, values='substitutions', index=['reference'], columns=['alternate'], fill_value=0)

fig, ax = plt.subplots()
with sns.axes_style("white"):
  ax = sns.heatmap(aa_counts_pd, vmax=max_count*0.4, cbar=False, annot=True, annot_kws={"size": 7}, fmt="d")
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determine Number of Mutations in Databricks Delta Table Over Time
# MAGIC
# MAGIC **Important Notes**
# MAGIC * Wait for stream to initialize before running subsequent cells
# MAGIC * Plots using anything other than the built-in display() cannot be run on streaming tables
# MAGIC   * Therefore write the stream out to memory (or to parquet), then manually run the amino acid substitution heat map cell below

# COMMAND ----------

# Reference: https://docs.databricks.com/spark/latest/structured-streaming/examples.html#foreachbatch-sqldw-example

# Configure
variant_count_test_parquet_path = "dbfs:/tmp/dnaseq/variant_count_test_parquet"

# Write to Delta
def write_to_delta(df, epochId):
  df.write. \
     format("parquet"). \
     mode('append'). \
     save(variant_count_test_parquet_path)

# Set configuration
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Execute query to write stream every 60s
query = (
  exomes.withColumn("mutations", fx.lit("mutations")). \
         select("contigName", "start", "referenceAllele", "alternateAllele", "mutations"). \
         dropDuplicates(). \
         groupBy("mutations"). \
         count(). \
         withColumn('time', fx.lit(fx.current_timestamp())). \
         writeStream. \
         trigger(processingTime='60 seconds'). \
         foreachBatch(write_to_delta). \
         outputMode("update"). \
         start()
    )

# COMMAND ----------

# Number of distinct mutations over time
display(spark.read.parquet(variant_count_test_parquet_path).withColumnRenamed("count", "distinct mutations"))

# COMMAND ----------

# MAGIC %md ## Add Data to our Genomics Pipeline
# MAGIC Now that we have setup our genomics pipeline, we can start adding more samples to our Databricks Delta table
# MAGIC * Because we have setup streaming, all of the preceding graphs and tables will reflect the latest data as it is coming in

# COMMAND ----------

import time
files = dbutils.fs.ls(annotations_etl_parquet_path)
counter=0

# Loop through all of the available Parquet files by sampleId every time interval
time.sleep(10)
for sample in files:
  counter+=1
  annotation_path = sample.path
  if ("sampleId=" in annotation_path):
    sampleId = annotation_path.split("/")[4].split("=")[1]
    variants = spark.read.format("parquet").load(str(annotation_path))
    print("running " + sampleId)
    if(sampleId != "SRS000030_SRR709972"):
      variants.write.format("delta"). \
             mode("append"). \
             save(delta_stream_outpath)
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up
# MAGIC Once you are done, let's clean up the data generated

# COMMAND ----------

dbutils.fs.rm(annotations_etl_parquet_path, True)
dbutils.fs.rm(delta_stream_outpath, True)

# COMMAND ----------


