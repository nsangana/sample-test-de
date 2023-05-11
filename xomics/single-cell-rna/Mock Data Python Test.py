# Databricks notebook source
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

sql("USE CATALOG hive_metastore")
sql("USE SCHEMA srijit_nair")

mm = sql("select * from matrix").repartition(numPartitions=512).cache()

# COMMAND ----------

display(mm)

# COMMAND ----------

#mm2 = mm.collect()
#The above collect takes about 22 minutes to complete... 
mm2 = mm.toPandas()

# COMMAND ----------

mm2.info()

# COMMAND ----------

import sys

print(sys.getsizeof(mm2)/(1024*1024*1024))


# COMMAND ----------

display(mm2[0:10])

# COMMAND ----------

mm2.to_feather("/dbfs/dbfs/srijit.nair/temp/feather")

# COMMAND ----------


