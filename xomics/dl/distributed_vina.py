# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Challenges of Drug Discovery
# MAGIC ## Show picture of end-to-end drug pipeline
# MAGIC ## Show picture of 

# COMMAND ----------

# DBTITLE 1,Install Docking Libraries
# MAGIC %conda install -c bioconda autodock-vina

# COMMAND ----------

# DBTITLE 1,Zinc S3 Bucket Repository
"""
Read data from the Zinc 3D bucket
"""

zinc_s3 = "s3a://zinc3d"
dbutils.fs.ls(zinc_s3)

# COMMAND ----------

# DBTITLE 1,Tool to navigate the Zinc S3 Bucket
"""
The Zinc 3D bucket is split into several paths
"""

# list the contents of tranches

zinc_files = [c.path for a in dbutils.fs.ls('/'.join([zinc_s3, "tranches", ""])) for b in dbutils.fs.ls(a.path) for c in dbutils.fs.ls(b.path)]

# COMMAND ----------

# DBTITLE 1,Download and Compile Concavity to do Protein Surface Probing
# MAGIC %sh
# MAGIC
# MAGIC sudo apt install --assume-yes libglu1-mesa-dev                              ## install libgl headers
# MAGIC wget -nc https://compbio.cs.princeton.edu/concavity/concavity_distr.tar.gz  ## grab concavity source code
# MAGIC tar -xf concavity_distr.tar.gz                                              ## unpack concavity source code
# MAGIC
# MAGIC ## compile concavity source code
# MAGIC mv concavity /tmp/
# MAGIC cd /tmp/concavity_distr
# MAGIC make clean
# MAGIC make DESTDIR=.

# COMMAND ----------

# DBTITLE 1,Distribute Concavity Binary to Worker Nodes
# MAGIC %fs cp /tmp/concavity_distr/bin/x86_64/concavity dbfs:/tmp/concavity
