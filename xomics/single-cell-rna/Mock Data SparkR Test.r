# Databricks notebook source
#library(arrow)
library(SparkR)
library(magrittr)
sc <- sparkR.session()


# COMMAND ----------

sql("USE CATALOG hive_metastore")
sql("USE SCHEMA srijit_nair")
mm <- sql("select * from matrix")

# COMMAND ----------

printSchema(mm)

# COMMAND ----------

mm_part <- repartition(mm,512)

# COMMAND ----------

sparkR.session(
  sparkConfig = list(
    spark.driver.maxResultSize = "0g"
  )
)


# COMMAND ----------

mm_r <- collect(mm)

# COMMAND ----------

mm_r <- as.data.frame(mm_part)

# COMMAND ----------

mm_r

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/dbfs/srijit.nair/temp

# COMMAND ----------

mm_f <- arrow::read_feather("/dbfs/dbfs/srijit.nair/temp/feather")

# COMMAND ----------

print(object.size(mm_f), units="Gb")

# COMMAND ----------


