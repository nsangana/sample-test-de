# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> __+__ <img width="65px" src="https://hail.is/hail-logo-cropped.png">
# MAGIC
# MAGIC ### Convert Hail Matrix Table to <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> Glow compatible VCF

# COMMAND ----------

import hail as hl
hl.init(sc, idempotent=True, quiet=True)

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### create Hail matrix table

# COMMAND ----------

vcf_path = '/databricks-datasets/hail/data-001/1kg_sample.vcf.bgz'
vcf_mt = hl.import_vcf(vcf_path)

mt_path = 'dbfs:/tmp/william.brandler@databricks.com/data/hail/1kg_sample.mt'
vcf_mt.write(mt_path, overwrite = True)

# COMMAND ----------

mt = hl.read_matrix_table(mt_path)

# COMMAND ----------

local_table = mt.localize_entries('sampleFreeGenotypes', 'sampleIds')

# COMMAND ----------

# Sample IDs are stored in a global field
sample_ids = hl.eval(local_table.globals.sampleIds.s)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create expression to aggregate genotypes

# COMMAND ----------

format_aliases = {
  'DP': 'depth',
  'FT': 'filters',
  'GL': 'genotypeLikelihoods',
  'PL': 'phredLikelihoods',
  'GQ': 'conditionalQuality',
  'HQ': 'haplotypeQualities',
  'EC': 'expectedAlleleCounts',
  'MQ': 'mappingQuality',
  'AD': 'alleleDepths'
}

def get_structs(gt_field, format_key):
  if format_key == 'GT':
    # Flatten GT into non-null calls and phased
    return ["'calls'", 'nvl(%s.GT.alleles, array(-1, -1))' % gt_field, "'phased'", 'nvl(%s.GT.phased, false)' % gt_field]
  elif format_key in format_aliases:
    # Renamed aliased genotype fields
    return ["'%s'" % format_aliases[format_key], '%s.%s' % (gt_field, format_key)]
  else:
    return ["'%s'" % format_key, '%s.%s' % (gt_field, format_key)]

def get_struct_expr(gt_field):
  flat_structs = []
  for format_key in list(mt.entry):
    flat_structs += get_structs(gt_field, format_key)
  return ', '.join(flat_structs)

# COMMAND ----------

# Aggregate the genotypes

sample_id_expr = ' ,'.join(["'%s'" % s for s in sample_ids])

gt_df = local_table.to_spark(). \
                    withColumn('genotypes', expr("zip_with(sampleFreeGenotypes, array(%s), (gt, id) -> named_struct('sampleId', id, %s))" % (sample_id_expr, get_struct_expr("gt")))). \
                    drop("sampleFreeGenotypes")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename variant fields to match Glow VCF schema
# MAGIC
# MAGIC Note
# MAGIC 1. Glow is 0-based, while Hail is 1-based
# MAGIC 2. Glow's end field is the start + size of the reference allele
# MAGIC 3. 1000 stands in for the max number of alternate alleles

# COMMAND ----------

base_df = gt_df. \
                withColumn("contigName", col("`locus.contig`")). \
                withColumn("start", (col("`locus.position`") - 1).cast("long")). \
                withColumn("end", (col("start") + length(expr("alleles[0]"))).cast("long")). \
                withColumn("names", col("rsid")). \
                withColumn("referenceAllele", element_at("alleles", 1)). \
                withColumn("alternateAlleles", slice(col("alleles"), 2, 1000))

# COMMAND ----------

# Rename info.* columns to INFO_*

info_renamed_df = base_df
old_info_cols = [c for c in base_df.columns if c.startswith("info.")]

def rename_info_col(col):
  return col.replace("info.", "INFO_")

for old_info_col in old_info_cols:
  info_renamed_df = info_renamed_df.withColumnRenamed(old_info_col, rename_info_col(old_info_col))
new_info_cols = [rename_info_col(c) for c in old_info_cols]

# COMMAND ----------

glow_compatible_df = info_renamed_df.select(
  "contigName",
  "start",
  "end",
  "names",
  "referenceAllele",
  "alternateAlleles",
  "qual",
  "filters",
  *new_info_cols,
  "genotypes"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Confirm we can use the Glow VCF writer on this DataFrame

# COMMAND ----------

vcf_path = "dbfs:/tmp/william.brandler@databricks.com/data/hail/hail-to-glow.vcf"

# COMMAND ----------

glow_compatible_df. \
                   write. \
                   format("bigvcf"). \
                   mode("overwrite"). \
                   save(vcf_path)

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 200 /dbfs/tmp/william.brandler@databricks.com/data/hail/hail-to-glow.vcf

# COMMAND ----------

vcf_df = spark.read.format("vcf").load(vcf_path)

# COMMAND ----------

display(vcf_df)

# COMMAND ----------


