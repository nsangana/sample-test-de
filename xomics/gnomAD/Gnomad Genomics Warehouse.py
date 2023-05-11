# Databricks notebook source
# MAGIC %md
# MAGIC ### Genomics Data Warehousing with Databricks Delta
# MAGIC
# MAGIC __cluster configuration__:
# MAGIC   - 8 i3.xlarge (32 cores)
# MAGIC
# MAGIC __Starting point__:
# MAGIC   - gnomad database in delta
# MAGIC     - 251m records
# MAGIC   - 30 whole exome VCFs in delta
# MAGIC     - 8m records
# MAGIC
# MAGIC __Goals__:
# MAGIC   - Extract all rare variants from gnomad for a gene of interest
# MAGIC     - compare parquet to delta performance
# MAGIC   - Join in exome data to reveal which individuals have rare variants in this gene

# COMMAND ----------

import pyspark.sql.functions as fx

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load gnomad in parquet and delta

# COMMAND ----------

gnomad_parquet = spark.read.format("parquet").load('dbfs:/mnt/dbgenomics.us-west-2/dbgenomics.data/gnomad_parquet_partitioned/')

# COMMAND ----------

display(gnomad_parquet.where(fx.col("contigName") == "1").drop("filters"))

# COMMAND ----------

gnomad_gene_parquet = gnomad_parquet.where((fx.col("gene") == "CDH8") & 
                                   (fx.col("AF") <= 0.001) & 
                                   (fx.col("qual") >= 30) &
                                   (fx.col("mutation_type") == "intron_variant")
                                  )
gnomad_gene_parquet.explain("extended")

# COMMAND ----------

gnomad_gene_parquet.count()

# COMMAND ----------

display(spark.sql("DROP TABLE IF EXISTS gnomad"))
display(spark.sql("CREATE TABLE gnomad USING DELTA LOCATION 'dbfs:/mnt/dbgenomics.us-west-2/dbgenomics.data/gnomad_delta_partitioned/'"))                 
display(spark.sql("OPTIMIZE gnomad ZORDER BY (gene, AF, qual, mutation_type)"))

# COMMAND ----------

gnomad_delta = spark.read.format("delta").load('dbfs:/mnt/dbgenomics.us-west-2/dbgenomics.data/gnomad_delta_partitioned/')

# COMMAND ----------

gnomad_gene = gnomad_delta.where((fx.col("gene") == "BRCA1") & 
                                 (fx.col("AF") <= 0.001) & 
                                 (fx.col("qual") >= 30) &
                                 (fx.col("mutation_type") == "intron_variant")
                                )
gnomad_gene.explain("extended")

# COMMAND ----------

gnomad_gene.count()

# COMMAND ----------

gnomad_pos = gnomad_delta.where((fx.col("contigName") == "19") & (fx.col("names")[0] == "rs429358"))
gnomad_pos.count()

# COMMAND ----------

gnomad_pos = gnomad_delta.where(fx.col("names")[0] == "rs429358")
gnomad_pos.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### load exome data

# COMMAND ----------

exomes_path = 'dbfs:/mnt/dbgenomics.us-west-2/dbgenomics.data/exome_genotypes_delta/'
exomes_delta = spark.read.format("delta").load(exomes_path)

# COMMAND ----------

display(exomes_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### join exome VCF delta to gnomad for gene of interest

# COMMAND ----------

exome_gene_variants = exomes_delta.join(gnomad_gene, ["contigName", "start"], "inner")

# COMMAND ----------

exome_gene_variants.count()

# COMMAND ----------

display(exome_gene_variants)

# COMMAND ----------

display(exome_gene_variants.groupBy("sampleId").count().orderBy("count", ascending=False))
