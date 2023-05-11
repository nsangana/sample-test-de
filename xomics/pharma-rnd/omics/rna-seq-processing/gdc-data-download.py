# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Download gene expression profiles and clinical data using [GDC Data Transfer Tool](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool)

# COMMAND ----------

import os

# COMMAND ----------

user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# COMMAND ----------

# MAGIC %md
# MAGIC To download expression data, you first creat a manifest file on [GDC Portal](https://portal.gdc.cancer.gov/) and put the file on [dbfs](https://docs.databricks.com/data/databricks-file-system.html#databricks-file-system-dbfs)

# COMMAND ----------

script_path='/dbfs/home/{}/tmptools'.format(user)
output_path='/dbfs/home/{}/data/genomics/rna/expression'.format(user)
manifest_path='/dbfs/home/{}/data/genomics/rna/gdc_manifest_20200224_192255.txt'.format(user) #change this path to where you put the manifest file
dbutils.fs.mkdirs(script_path.replace('/dbfs/','dbfs:/'))
dbutils.fs.mkdirs(output_path.replace('/dbfs/','dbfs:/'))

# COMMAND ----------

def get_gdc_client(script_path):
  
  command_str = """
  cd {};
  wget https://gdc.cancer.gov/files/public/file/gdc-client_v1.6.0_Ubuntu_x64-py3.7.zip;
  unzip gdc-client_v1.6.0_Ubuntu_x64-py3.7.zip;
  unzip gdc-client_v1.6.0_Ubuntu_x64.zip
  """
  command=command_str.format(script_path)
  print(command)
  os.system(command)

# COMMAND ----------

def download_data(script_path,manifest_path,output_path):
  
  command_str="""
  cd {};
  ./gdc-client download -m {} -d {} -n 72
  """
  
  command = command_str.format(script_path,manifest_path,output_path)
  print(command)
  os.system(command)

# COMMAND ----------

get_gdc_client(script_path)

# COMMAND ----------

download_data(script_path,manifest_path,output_path)

# COMMAND ----------

display(dbutils.fs.ls(output_path.replace('/dbfs/','dbfs:/')))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /home/amir.kermany@databricks.com/data/genomics/rna/

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/home/amir.kermany@databricks.com/data/genomics/rna/
# MAGIC head gdc_manifest_20200224_192255.txt

# COMMAND ----------


