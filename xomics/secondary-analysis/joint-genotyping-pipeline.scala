// Databricks notebook source
// MAGIC %md
// MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
// MAGIC
// MAGIC ## Joint Genotyping Pipeline
// MAGIC
// MAGIC Test data and manifest file to run this pipeline can be located here: 
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/jg-sample/HG00096.g.vcf.bgz`
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/jg-sample/HG00097.g.vcf.bgz`
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/jg-sample/manifest.csv`
// MAGIC
// MAGIC The cell below shows the parameters that are currently available for configuration. See the [Databricks documentation](https://docs.databricks.com/applications/genomics/joint-genotyping-pipeline.html) for more usage details

// COMMAND ----------

import com.databricks.hls.pipeline.joint._
import com.databricks.hls.pipeline._

val pipeline = JointGenotypingPipeline
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
  // Coerce value to appropriate type.
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
// MAGIC ### Screenshot example of jobs run for joint-genotyping pipeline
// MAGIC
// MAGIC ##<img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/genomics_runtime/jointGenotyping_pipeline_jobs_screenshot.png" alt="logo" width="1000"/> 
