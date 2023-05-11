# Databricks notebook source
# MAGIC %sql
# MAGIC select * from  hive_metastore.single_cell.dna_nexus_files ;

# COMMAND ----------

# MAGIC %sql
# MAGIC update hive_metastore.single_cell.dna_nexus_files set processed=TRUE, processedTime=CURRENT_TIMESTAMP where path='dbfs:/FileStore/philsalm/eisai/singlecell/G2D2';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from single_cell.matrix;

# COMMAND ----------


