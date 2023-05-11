# Databricks notebook source
# MAGIC %md # Integrating Variants with Annotations using Glow
# MAGIC
# MAGIC Glow addeds support for loading [GFF3](https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md) files, which are commonly used to store annotations on genomic regions. When combined with variant data, genomic annotations provide context for each change in the genome. Does this mutation cause a change in the protein coding sequence of gene? If so, how does it change the protein? Or is the mutation in a low information part of the genome, also known as “junk DNA”. And everything in between. In this notebook, we demonstrate how to use Glow's APIs to work with annotations from the [RefSeq database](https://www.ncbi.nlm.nih.gov/refseq/) alongside genomic variants from the [1000 Genomes project](https://www.internationalgenome.org/).
# MAGIC
# MAGIC To start, we will download annotations for the GRCh38 human reference genome from RefSeq and define the paths that we will load our data from.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.dataframe import *

# Human genome annotations in GFF3 are available at https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/reference/GCF_000001405.39_GRCh38.p13/
gff_path = "/databricks-datasets/genomics/gffs/GCF_000001405.39_GRCh38.p13_genomic.gff.bgz"

vcf_path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.shapeit2_integrated_snvindels_v2a_27022019.GRCh38.phased.vcf.gz"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load in GFF file
# MAGIC
# MAGIC Here, we load in the GFF file as an Apache Spark dataframe, using [Glow's GFF reader](https://glow.readthedocs.io/en/stable/etl/gff.html). We can then filter down to all annotations that are on `NC_000022.11`, which is the accession for chromosome 22 in the [GRCh38 patch 13 human genome reference build](https://www.ncbi.nlm.nih.gov/nuccore/568815576).

# COMMAND ----------

# DBTITLE 0,Print inferred schema
annotations_df = spark.read \
  .format('gff') \
  .load(gff_path) \
  .filter("seqid = 'NC_000022.11'") \
  .alias('annotations_df')

annotations_df.printSchema()

# COMMAND ----------

# DBTITLE 0,Read in the GFF3 with the inferred schema
display(annotations_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load in variants
# MAGIC
# MAGIC Now, we'll load in genotype data from 1000 Genomes. We will combine this data later with our annotations, so we can connect variants  and gene transcripts.

# COMMAND ----------

variants_df = spark.read \
  .format("vcf") \
  .load(vcf_path) \
  .drop('genotypes') \
  .alias('variants_df')

display(variants_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Resolving parent/child relationships in annotation data
# MAGIC
# MAGIC Annotation data often contains parent/child relationships, for example, an exon (child) is a part of a gene (parent). To resolve these relationships, we can use a self-join in Spark SQL.

# COMMAND ----------

from pyspark.sql.functions import *

parent_child_df = annotations_df \
.join(
  annotations_df.select('id', 'type', 'name', 'start', 'end').alias('parent_df'),
  col('annotations_df.parent')[0] == col('parent_df.id') # each row in annotation_df has at most one parent
) \
.orderBy('annotations_df.start', 'annotations_df.end') \
.select(
  'annotations_df.seqid',
  'annotations_df.type',
  'annotations_df.start',
  'annotations_df.end',
  'annotations_df.id',
  'annotations_df.name',
  col('annotations_df.parent')[0].alias('parent_id'),
  col('parent_df.Name').alias('parent_name'),
  col('parent_df.type').alias('parent_type'),
  col('parent_df.start').alias('parent_start'),
  col('parent_df.end').alias('parent_end')
) \
.alias('parent_child_df')

display(parent_child_df)                             

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can generalize this process with a function that runs the self-join, filters down to a specified parent/child relationship. To link all of the children back to the parent feature, we finally run a `group-by`.

# COMMAND ----------

from pyspark.sql.dataframe import *

def parent_child_summary(parent_child_df: DataFrame, parent_type: str, child_type: str) -> DataFrame:
  return parent_child_df \
    .select(
      'seqid',
      col('parent_id').alias(f'{parent_type}_id'),
      col('parent_name').alias(f'{parent_type}_name'),
      col('parent_start').alias(f'{parent_type}_start'),
      col('parent_end').alias(f'{parent_type}_end'),
      col('id').alias(f'{child_type}_id'),
      col('start').alias(f'{child_type}_start'),
      col('end').alias(f'{child_type}_end'),
    ) \
    .where(f"type == '{child_type}' and parent_type == '{parent_type}'") \
    .groupBy(
      'seqid',
      f'{parent_type}_id',
      f'{parent_type}_name',
      f'{parent_type}_start',
      f'{parent_type}_end'
    ) \
    .agg(
      collect_list(
        struct(
          f'{child_type}_id',
          f'{child_type}_start',
          f'{child_type}_end'
        )
      ).alias(f'{child_type}s')
    ) \
    .orderBy(
      f'{parent_type}_start',
      f'{parent_type}_end'
    ) \
    .alias(f'{parent_type}_{child_type}_df')


# COMMAND ----------

# MAGIC %md With this function, we can get a dataframe containing all transcript variants of a gene.

# COMMAND ----------

gene_transcript_df = parent_child_summary(parent_child_df, 'gene', 'transcript')
display(gene_transcript_df)

# COMMAND ----------

# MAGIC %md Or equivalently, a dataframe pairing individual transcripts with the exons that are spliced together to create them.

# COMMAND ----------

transcript_exon_df = parent_child_summary(parent_child_df, 'transcript', 'exon')
display(transcript_exon_df)

# COMMAND ----------

# MAGIC %md In the end, by joining the gene-transcript, transcript-exon, and variant tables together, we can link each coding variant to the exons, transcripts, and gene they occured in.

# COMMAND ----------

from glow.functions import *

gene_transcript_exploded_df = gene_transcript_df \
  .withColumn('transcripts', explode('transcripts')) \
  .withColumn('transcripts', expand_struct('transcripts')) \
  .alias('gene_transcript_exploded_df')

transcript_exon_exploded_df = transcript_exon_df \
  .withColumn('exons', explode('exons')) \
  .withColumn('exons', expand_struct('exons')) \
  .alias('transcript_exon_exploded_df')


variant_exon_transcript_gene_df = variants_df \
.join(
  transcript_exon_exploded_df, 
  (variants_df.start < transcript_exon_exploded_df.exon_end) &
  (transcript_exon_exploded_df.exon_start < variants_df.end) 
) \
.join(
  gene_transcript_exploded_df, 
  transcript_exon_exploded_df.transcript_id == gene_transcript_exploded_df.transcript_id
) \
.select(
  col('variants_df.contigName').alias('variant_contig'),
  col('variants_df.start').alias('variant_start'),
  col('variants_df.end').alias('variant_end'),
  col('variants_df.referenceAllele'),
  col('variants_df.alternateAlleles'),
  'transcript_exon_exploded_df.exon_id',
  'transcript_exon_exploded_df.exon_start',
  'transcript_exon_exploded_df.exon_end',
  'transcript_exon_exploded_df.transcript_id',
  'transcript_exon_exploded_df.transcript_name',
  'transcript_exon_exploded_df.transcript_start',
  'transcript_exon_exploded_df.transcript_end',
  'gene_transcript_exploded_df.gene_id',
  'gene_transcript_exploded_df.gene_name',
  'gene_transcript_exploded_df.gene_start',
  'gene_transcript_exploded_df.gene_end'
) \
.orderBy(
  'variant_contig',
  'variant_start',
  'variant_end'
)

display(variant_exon_transcript_gene_df)
