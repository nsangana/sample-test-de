// Databricks notebook source
dbutils.widgets.text("nSamples", "", "Number of samples to simulate")
dbutils.widgets.text("contigs", "chr22", "Which contigs to simulate (comma separated)")
dbutils.widgets.text("outputDir", "", "Delta output directory")

// COMMAND ----------

val scaleFactor = dbutils.widgets.get("nSamples").toInt
val contigs = dbutils.widgets.get("contigs").split(",").map(_.trim())
val outputDir = dbutils.widgets.get("outputDir")

// COMMAND ----------

import org.apache.spark.sql.functions._
import io.projectglow.functions._
import io.projectglow.Glow

def generate(chr: String): Unit = {
  println(s"Generating synthetic calls for $chr based on thousand genomes allele frequencies")
  spark.conf.set("spark.hadoop.parquet.block.size.row.check.min", 1)
  spark.conf.set("spark.hadoop.parquet.block.size.row.check.max", 1)
  val vcfPath = s"/mnt/dbgenomics.us-west-2/dbgenomics.data/1kg-vcfs/ALL.${chr}.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
  val inputDf = Glow.transform("split_multiallelics", spark.read.format("vcf").load(vcfPath))
  val withInfoAF = inputDf
    .where("size(INFO_AF) > 0")
    .repartition(sc.defaultParallelism * 4)
    .selectExpr("contigName", "start", "end", "referenceAllele", "alternateAlleles", "qual", "filters", "splitFromMultiAllelic", "INFO_AF[0] as INFO_AF")

  val withGeneratedCalls = withInfoAF
    .withColumn("genotypes", expr(s"""
    transform(
      array_repeat(null, $scaleFactor),
      i -> named_struct(
        'calls', transform(array_repeat(null, 2), i -> if(rand() < INFO_AF, 1, 0)), 
        'phased', false))
    """))
  
  withGeneratedCalls.write.mode("overwrite")
    .format("delta")
    .partitionBy("contigName")
    .option("replaceWhere", s"contigname = '${chr.stripPrefix("chr")}'")
    .save(outputDir)
}

// COMMAND ----------

contigs.foreach(generate)
