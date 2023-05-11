// Databricks notebook source
import com.databricks.hls.pipeline.dnaseq.annotation._
import com.databricks.hls.pipeline._

val pipeline = VEPPipeline
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

display(dbutils.fs.ls(output))

// COMMAND ----------

display(spark.read.format("delta").load(s"$output/annotations"))

// COMMAND ----------

if (dbutils.widgets.get("exportVCF") == "true") {
  display(spark.read.format("com.databricks.vcf").load(s"$output/annotations.vcf.bgz"))
}
