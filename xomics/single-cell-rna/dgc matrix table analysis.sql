-- Databricks notebook source
USE SCHEMA single_cell

-- COMMAND ----------

DESCRIBE DETAIL matrix

-- COMMAND ----------



-- COMMAND ----------

DESCRIBE HISTORY matrix

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC https://docs.databricks.com/delta/tune-file-size.html
-- MAGIC
-- MAGIC https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-tblproperties.html#tblproperties
-- MAGIC
-- MAGIC https://www.databricks.com/blog/2019/03/19/efficient-upserts-into-data-lakes-databricks-delta.html

-- COMMAND ----------

SHOW TBLPROPERTIES matrix

-- COMMAND ----------

SHOW CREATE TABLE matrix

-- COMMAND ----------

SELECT count(count), input_file_name() as f from matrix group by f 

-- COMMAND ----------


