# Databricks notebook source
library(SparkR)
library(magrittr)


# COMMAND ----------

#sparkR.session(
#  sparkConfig = list(
#    spark.sql.execution.arrow.sparkr.enabled = "true"
#  )
#)
sc <- sparkR.session()

# COMMAND ----------

sql("USE CATALOG hive_metastore")
sql("USE SCHEMA srijit_nair")
mm <- sql("select * from matrix")


# COMMAND ----------

cache(mm)
df_size <- count(mm)

# COMMAND ----------

split_size <- 50000000

# COMMAND ----------

mm1<-mm %>%
 withColumn("id", over(row_number(), orderBy(windowPartitionBy("reference_id"),"reference_id" ))) %>%
 withColumn("batch", ( ceiling(.$id/split_size)) ) %>%
 drop("id")

# COMMAND ----------

display(mm1)

# COMMAND ----------

library(foreach)

# COMMAND ----------

key_df <- as.DataFrame(data.frame(batch=5))
r <- key_df %>% join(mm1, .$batch == mm1$batch) %>% collect

# COMMAND ----------

result_list <- foreach(i=1: ceiling(df_size/split_size)) %do% {
  key_df <- as.DataFrame(data.frame(batch=i))
  
  key_df %>% join(mm1, .$batch == mm1$batch) %>% collect
}

# COMMAND ----------


