// Databricks notebook source
// MAGIC %md
// MAGIC ###<img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
// MAGIC
// MAGIC ###SnpEff Annotation Pipeline
// MAGIC
// MAGIC Test data to run this pipeline can be located here: 
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz`
// MAGIC
// MAGIC The cell below shows the parameters that are currently available for configuration. See the [Databricks documentation](https://docs.databricks.com/applications/genomics/snpeff-pipeline.html) for more usage details.

// COMMAND ----------

import com.databricks.hls.pipeline.dnaseq._
import com.databricks.hls.pipeline._
import com.databricks.hls.pipeline.dnaseq.annotation.AnnotationPipeline

val pipeline = AnnotationPipeline
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

display(dbutils.fs.ls(output))
