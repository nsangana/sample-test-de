// Databricks notebook source
// MAGIC %md
// MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> RNASeq Quantification Workflow
// MAGIC
// MAGIC Test data and manifest file to run this pipeline can be located here: 
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/rnaseq-sample/SRR7218351.bam`
// MAGIC
// MAGIC `dbfs:/databricks-datasets/genomics/rnaseq-sample/manifest.csv`
// MAGIC
// MAGIC Run the cell below to show all parameters that are currently available for configuration.
// MAGIC   

// COMMAND ----------

import com.databricks.hls.pipeline.rnaseq._
import com.databricks.hls.pipeline._
import org.bdgenomics.adam.rdd.ADAMContext._
import spark.implicits._

val pipeline = RNASeqPipeline
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

val aligned = sc.loadAlignments(s"$output/aligned/recordGroupSample*/*")
display(aligned.dataset)

// COMMAND ----------

dbutils.fs.ls(s"$output/")
