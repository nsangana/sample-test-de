# Databricks notebook source
# MAGIC %md
# MAGIC ###<img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
# MAGIC
# MAGIC ### Parallel bgzipping of fastq files
# MAGIC
# MAGIC Batch process a set of gzipped fastq files into bgzipped fastq in parallel from cloud storage
# MAGIC
# MAGIC fastq.gz (cloud storage) -> copy to cluster -> gunzip -> multithreaded bgzip -> fastq.bgz (cloud storage)
# MAGIC
# MAGIC Assumes that the fastq files are all in the same directory and that you want to write back to the same directory

# COMMAND ----------

import pyspark.sql.functions as fx
import subprocess

# COMMAND ----------

# MAGIC %md
# MAGIC ### init script generation
# MAGIC Create this init script and provision a cluster to use it
# MAGIC
# MAGIC It distributes the htslib library to all nodes in the cluster
# MAGIC
# MAGIC An alternative to an init script is using Docker via [Databricks Container Services](https://docs.azuredatabricks.net/clusters/custom-containers.html#customize-containers-with-databricks-container-services)

# COMMAND ----------

# dbutils.fs.put("dbfs:/home/william.brandler@databricks.com/init_scripts/htslib_init.sh", """
# wget https://github.com/samtools/htslib/releases/download/1.9/htslib-1.9.tar.bz2
# tar xjvf htslib-1.9.tar.bz2
# cd htslib-1.9
# ./configure
# make
# make install
# """, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Copy Data
# MAGIC
# MAGIC This is just for testing purposes
# MAGIC
# MAGIC It can be skipped if you already have fastq files in cloud storage

# COMMAND ----------

# MAGIC %sh
# MAGIC wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/HG00096/sequence_read/SRR062634_1.filt.fastq.gz
# MAGIC wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/HG00096/sequence_read/SRR062634_2.filt.fastq.gz

# COMMAND ----------

path_to_fq = "dbfs:/tmp/william.brandler@databricks.com/genomics/" #change path
dbutils.fs.mkdirs(path_to_fq)

# COMMAND ----------

# MAGIC %sh
# MAGIC cp SRR062634_1.filt.fastq.gz /dbfs/tmp/william.brandler@databricks.com/genomics/
# MAGIC cp SRR062634_2.filt.fastq.gz /dbfs/tmp/william.brandler@databricks.com/genomics/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe creation

# COMMAND ----------

df = spark.createDataFrame(dbutils.fs.ls(path_to_fq))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC use local file system API to set up paths for input and output files

# COMMAND ----------

df = df.filter(~fx.col("name").rlike("bgz")). \
        withColumn('path', fx.regexp_replace('path', 'dbfs:', '/dbfs')). \
        withColumn('outpath', fx.regexp_replace('path', 'gz', 'bgz'))
display(df)

# COMMAND ----------

def bgzip_cmd(path, outpath):
        """
        python wrapper to run bgzip from command line in parallel
        """
        fastq = path.replace("fastq.gz", "fastq")
        cmd = ('bash -c \'gunzip {path} && bgzip -@ 1 {fastq} && mv {path} {outpath}\'')\
          .format(path=path, fastq=fastq, outpath=outpath)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        assert s_return == 0, 'Could not bgzip files' + str(s_output) + '\n' + str(s_err)
        return s_return, s_output, s_err

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df2 = df.rdd.map(lambda row: bgzip_cmd(str(row[0]), str(row[3])))

# COMMAND ----------

# MAGIC %md
# MAGIC since Spark executes lazily, run a count on the df to kick off the parallel bgzip

# COMMAND ----------

df2.count()

# COMMAND ----------

display(dbutils.fs.ls(path_to_fq))

# COMMAND ----------


