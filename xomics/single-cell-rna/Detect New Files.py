# Databricks notebook source
# MAGIC %md
# MAGIC This notebook makes use of [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) to detect new files in a specific s3/dbfs location. This pattern can be used to respond to file adds to a specific location. 
# MAGIC
# MAGIC In this example we are collecting the path and filenames that were added and persisting the data into a delta table. This table can then be later read to read the files and process them.
# MAGIC
# MAGIC This pattern can be used to automatically process files coming from DNA Nexus. Please see the `Save Data` notebook to understand the processing pattern
# MAGIC
# MAGIC This notebook can be part of a job that can be scheduled to run automatically at fixed intervals

# COMMAND ----------

import os

@udf
def get_path(filepath):
  return os.path.split(filepath)[0]

@udf
def get_filename(filepath):
  return os.path.split(filepath)[1]


# COMMAND ----------

from pyspark.sql.functions import lit,col
from delta.tables import *

input_path = "dbfs:/FileStore/philsalm/eisai/singlecell/"
checkpoint_location = "/Users/srijit.nair@databricks.com/field_demos/checkpoints/eisai/single_cell/autoloader"

def upsertToDelta(result, batchId):
  (DeltaTable
    .forName(spark, "hive_metastore.single_cell.dna_nexus_files")
    .alias("t")
    .merge(
      result.alias("s"),
      "s.path = t.path")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
  )

ddf = (spark.readStream.format("cloudFiles")
  .format("cloudFiles")
  .option("cloudFiles.format", "binaryFile")
  .option("recursiveFileLookup", "true")
  .load(input_path)
  .drop("content","length","modificationTime")
  .withColumn("filenames", get_filename(col("path")))
  .withColumn("path", get_path(col("path")))
 #.filter.  add filters to get specific files of interes
  .groupBy("path")
  .agg(collect_list("filenames").alias("filenames"))
  .withColumn("processed", lit(False))
  .withColumn("processedTime", lit(None))
  .writeStream
  .option("checkpointLocation", checkpoint_location)
  .trigger(once=True)
  .format("delta")
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  hive_metastore.single_cell.dna_nexus_files ;

# COMMAND ----------


