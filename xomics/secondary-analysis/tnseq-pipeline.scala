// Databricks notebook source
// MAGIC %md
// MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> Tumor-normal somatic variant calling workflow
// MAGIC
// MAGIC Test data and manifest file to run this pipeline can be located here: 
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/tnseq-sample/normal.chr22.bam`
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/tnseq-sample/tumor.chr22.bam`
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/tnseq-sample/manifest.chr22_sampled.csv`
// MAGIC
// MAGIC The cell below shows the parameters that are currently available for configuration. See the [Databricks documentation](https://docs.databricks.com/applications/genomics/tumor-normal-pipeline.html) for more usage details.

// COMMAND ----------

import com.databricks.hls.pipeline.tnseq._
import com.databricks.hls.pipeline._
import spark.implicits._

val pipeline = TNSeqPipeline
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
