# Databricks notebook source
library(SparkR)
library(magrittr)


# COMMAND ----------

sparkR.session(
  sparkConfig = list(
    spark.sql.execution.arrow.sparkr.enabled = "false"
  )
)
sc <- sparkR.session()

# COMMAND ----------

sql("USE CATALOG hive_metastore")
sql("USE SCHEMA srijit_nair")
mm <- sql("select * from matrix")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/dbfs/srijit.nair/temp/part*

# COMMAND ----------

library(fst)
tt <- data.frame(list(a=1, b=2))
ff <- "/dbfs/dbfs/srijit.nair/temp/xxx.fst" 
write.fst(tt,ff)

# COMMAND ----------

store_data <- function (df){
    library(feather)
    file_path <- "/dbfs/dbfs/srijit.nair/temp/"  
    file_name <- paste0(file_path, "part", as.numeric(Sys.time()), "_",  round(runif(1, 1,1000)) )
    print(class(df))
    feather::write_feather(df, file_name)
    #saveRDS(df,file=file_name)
    return(data.frame(list(fname=file_name)))
    #return(df)
}

# COMMAND ----------

schema <- structType(structField("fname", "string"))
cc <- dapply(mm, store_data, schema)
#mm_part <- repartition(mm,128)
#cc <- dapply(mm_part, store_data, schema)

# COMMAND ----------

display(cc)

# COMMAND ----------

display(cc)

# COMMAND ----------

files <- collect(cc)

# COMMAND ----------

res_list <- foreach(i=1:length(files)) %do% readRDS(files[[i]])
result <- rbindlist(res_list)
