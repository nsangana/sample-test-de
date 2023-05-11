# Databricks notebook source
# MAGIC %md
# MAGIC #### Run series of notebooks in Python and Scala as a notebook workflow
# MAGIC
# MAGIC Set up a Databricks job with the desired cluster setup and attach this notebook to run the workflow

# COMMAND ----------

dbutils.notebook.run("parallel_bgzip", 6000)

# COMMAND ----------

# MAGIC %md
# MAGIC #### pass variables between Python and Scala
# MAGIC
# MAGIC First create a pyspark dataframe, register as a temp table, then convert to a Scala Map

# COMMAND ----------

from pyspark.sql.types import *
exportVCF = dbutils.widgets.get("exportVCF")
manifest = dbutils.widgets.get("manifest")
output = dbutils.widgets.get("output")

customSchema = StructType([StructField("key", StringType()), StructField("value", StringType())])
param_key_value_list = [["exportVCF", exportVCF], ["manifest", manifest], ["output", output]]
param_df = spark.createDataFrame(param_key_value_list, schema = customSchema)
param_df.createOrReplaceTempView("param_df")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = table("param_df")
# MAGIC val keyValueMap = df.map(x=> (x(0).toString, x(1).toString)).collect.toMap
# MAGIC val exportVCF = keyValueMap("exportVCF")
# MAGIC val manifest = keyValueMap("manifest")
# MAGIC val output = keyValueMap("output")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Scala notebook

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.run("DNASeqPipeline", 6000, Map("exportVCF" -> exportVCF, 
# MAGIC                                                "manifest" -> manifest, 
# MAGIC                                                "output" -> output)
# MAGIC                     )
