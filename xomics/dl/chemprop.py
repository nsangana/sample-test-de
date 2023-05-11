# Databricks notebook source
# MAGIC %conda install -c conda-forge rdkit

# COMMAND ----------

# MAGIC %pip install git+https://github.com/bp-kelley/descriptastorus chemprop

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # run the chemprop train function
# MAGIC
# MAGIC chemprop_train --data_path --dataset_type --save_dir

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC import horovod
# MAGIC
# MAGIC horovod.spark.task.get_available_devices()
