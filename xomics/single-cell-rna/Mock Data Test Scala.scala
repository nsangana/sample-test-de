// Databricks notebook source
sql("USE CATALOG hive_metastore")
sql("USE SCHEMA srijit_nair")

val mm = sql("select * from matrix").collect()

// COMMAND ----------


