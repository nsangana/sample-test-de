// Databricks notebook source
// MAGIC %md
// MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
// MAGIC
// MAGIC ### DNASeq Pipeline
// MAGIC
// MAGIC Test data and manifest file to run this pipeline can be located here: 
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/NA12878_sample/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam`
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/NA12878_sample/manifest.csv`
// MAGIC
// MAGIC The cell below shows the parameters that are currently available for configuration. See the [Databricks documentation](https://docs.databricks.com/applications/genomics/dnaseq-pipeline.html) for more usage details.

// COMMAND ----------

import com.databricks.hls.pipeline.dnaseq._
import com.databricks.hls.pipeline._
import spark.implicits._

val pipeline = new DNASeqPipeline(align = true, callVariants = true, annotate = true)
val visibleParams = pipeline.getVisibleParams
display(visibleParams.toDF())

// COMMAND ----------

visibleParams.foreach { case NotebookParam(name, description, defaultValue, _, _) => 
  dbutils.widgets.text(name, defaultValue.getOrElse(""), label = name)
}

// COMMAND ----------

val output = dbutils.widgets.get("output")
val params = visibleParams.map { case p @ NotebookParam(name, description, defaultValue, _, required) => 
  val value = dbutils.widgets.get(name)
  if (required && value.trim().isEmpty) {
    throw new IllegalStateException(s"Required Parameter $name was not supplied")
  }
  // coerce value to appropriate type
  val coercedValue = p.coerce(value)
  (name, coercedValue)
}.toMap[String, Any]

// COMMAND ----------

pipeline.init(params)
pipeline.execute(spark)

// COMMAND ----------

dbutils.fs.ls(s"$output/")

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### Screenshot example of jobs run for DNASeq pipeline
// MAGIC
// MAGIC ##<img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/genomics_runtime/DNASeq_pipeline_jobs_screenshot.png" alt="logo" width="1000"/> 

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### Note: please edit the cluster configurations as follows
// MAGIC
// MAGIC #### 1. Set the refGenomeId environment variable (for custom reference genomes see [here](https://docs.databricks.com/applications/genomics/secondary/dnaseq-pipeline.html#reference-genomes-1))
// MAGIC #### 2. Set the spark config `spark.sql.shuffle.partitions` to 3x the number of cores on the cluster
// MAGIC
// MAGIC ##<img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/genomics_runtime/DNASeq_pipeline_cluster_config_screenshot.png" alt="logo" width="800"/> 
