# Databricks notebook source
# MAGIC %md
# MAGIC #### Install SAIGE across cluster
# MAGIC
# MAGIC The init script downloads SAIGE (and associated dependencies) and installs it across the Spark cluster on Databricks
# MAGIC
# MAGIC To install this init script into DBFS:
# MAGIC 1. Create a cluster with a small instance type (we just need a cluster to run this one command) 
# MAGIC 2. Using that cluster, run this command
# MAGIC
# MAGIC To use this init script:
# MAGIC 1. When creating a cluster, add this init script by providing the path to dbfs
# MAGIC
# MAGIC Note: Cluster node [init scripts](https://docs.databricks.com/clusters/init-scripts.html#cluster-node-initialization-scripts) run during startup for each cluster node before the Spark driver or worker JVM starts

# COMMAND ----------

dbutils.fs.put("dbfs:/home/william.brandler@databricks.com/saige-cluster-init.sh" ,"""
#!/usr/bin/env bash

set -ex
set -o pipefail

# Install SKAT for SAIGE-GENE
sudo apt-get -y install libboost-all-dev autoconf
Rscript -e 'install.packages("SKAT", repos="https://cran.us.r-project.org")'

# Install SAIGE
cd /opt
git clone https://github.com/weizhouUMICH/SAIGE
cd SAIGE
Rscript extdata/install_packages.R
R CMD INSTALL *.tar.gz

#symbolically-link the system-level libraries
sudo ln -sf  /usr/lib/libblas.so /usr/lib/R/lib/libRblas.so
sudo ln -sf  /usr/lib/liblapack.so /usr/lib/R/lib/libRlapack.so

# Install HTSLIB
cd /opt
git clone https://github.com/samtools/htslib
cd htslib
autoheader
autoconf
./configure
make
make install

""", True)

# COMMAND ----------


