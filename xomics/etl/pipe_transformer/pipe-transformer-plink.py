# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC
# MAGIC ### Run plink using the <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> [Pipe Transformer](https://glow.readthedocs.io/en/latest/tertiary/pipe-transformer.html)

# COMMAND ----------

import glow
glow.register(spark)
import json
import pyspark.sql.functions as fx

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install plink across cluster
# MAGIC
# MAGIC The init script downloads the binaries and install them across the Spark cluster
# MAGIC
# MAGIC Upload the init script to dbfs with the Databricks file system [CLI](https://docs.databricks.com/dev-tools/databricks-cli.html#dbfs-cli), 
# MAGIC
# MAGIC or run the command below on a different cluster
# MAGIC
# MAGIC Note: Cluster node [init scripts](https://docs.databricks.com/clusters/init-scripts.html#cluster-node-initialization-scripts) run during startup for each cluster node before the Spark driver or worker JVM starts
# MAGIC
# MAGIC Or use [Docker](https://docs.databricks.com/clusters/custom-containers.html) to set up the environment

# COMMAND ----------

dbutils.fs.put("dbfs:/home/william.brandler@databricks.com/init/plink.sh" ,"""
#!/bin/bash

#install plink
wget http://zzz.bwh.harvard.edu/plink/dist/plink-1.07-x86_64.zip
unzip plink-1.07-x86_64.zip
mkdir /mnt/dbnucleus/lib/plink-1.07-x86_64/
cp -r plink-1.07-x86_64/* /mnt/dbnucleus/lib/plink-1.07-x86_64

#install plink 1.9
wget http://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20200616.zip
mkdir /mnt/dbnucleus/lib/plink-1.9-x86_64/
mv plink_linux_x86_64_20200616.zip /mnt/dbnucleus/lib/plink-1.9-x86_64/
cd /mnt/dbnucleus/lib/plink-1.9-x86_64/
unzip plink_linux_x86_64_20200616.zip
""", True)

# COMMAND ----------

# DBTITLE 1,check plink was correctly installed across the cluster
# MAGIC %sh
# MAGIC /mnt/dbnucleus/lib/plink-1.07-x86_64/plink --noweb

# COMMAND ----------

# MAGIC %sh
# MAGIC /mnt/dbnucleus/lib/plink-1.9-x86_64/plink --noweb

# COMMAND ----------

# MAGIC %md
# MAGIC #### load 1000 Genomes VCF and select first 1000 records

# COMMAND ----------

vcf_df = spark.read.format("vcf").load("dbfs:/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz")
vcf_df = sqlContext.createDataFrame(sc.parallelize(vcf_df.take(1000)), vcf_df.schema).cache()

# COMMAND ----------

display(vcf_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### run plink on a VCF file with the pipe transformer!
# MAGIC
# MAGIC Here we are going to convert the VCF to plink and calculate the allele frequency for each SNP

# COMMAND ----------

scriptFile = r"""#!/bin/sh
set -e

#write partitioned VCF
export tmpdir=$(mktemp -d -t vcf.XXXXXX)
cat - > ${tmpdir}/input.vcf

#convert to plink
/mnt/dbnucleus/lib/plink-1.9-x86_64/plink --vcf ${tmpdir}/input.vcf --keep-allele-order --make-bed --silent --out ${tmpdir}/plink

#run plink
/mnt/dbnucleus/lib/plink-1.07-x86_64/plink --bfile ${tmpdir}/plink --freq --noweb --silent --out ${tmpdir}/plink

cat ${tmpdir}/plink.frq | sed -e 's/ \+/,/g' | sed 's/,//'
"""

cmd = json.dumps(["bash", "-c", scriptFile])

# COMMAND ----------

plink_freq_df = glow.transform('pipe', 
                               vcf_df, 
                               cmd=cmd, 
                               input_formatter='vcf',
                               in_vcf_header='infer',
                               output_formatter='csv',
                               out_header='true')

# COMMAND ----------

display(plink_freq_df)

# COMMAND ----------

plink_freq_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run plink using binary ped file as the starting input

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/william.brandler@databricks.com/plink", True)
dbutils.fs.mkdirs("dbfs:/tmp/william.brandler@databricks.com/plink")

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz /dbfs/tmp/william.brandler@databricks.com/plink/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
# MAGIC
# MAGIC gunzip /dbfs/tmp/william.brandler@databricks.com/plink/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
# MAGIC
# MAGIC head -n 10000  /dbfs/tmp/william.brandler@databricks.com/plink/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf > /dbfs/tmp/william.brandler@databricks.com/plink/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.10000.vcf
# MAGIC
# MAGIC /mnt/dbnucleus/lib/plink-1.9-x86_64/plink --vcf /dbfs/tmp/william.brandler@databricks.com/plink/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.10000.vcf --keep-allele-order --make-bed --out /dbfs/tmp/william.brandler@databricks.com/plink/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes

# COMMAND ----------

df_bed = spark.read.format("plink").load("dbfs:/tmp/william.brandler@databricks.com/plink/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.bed")

# COMMAND ----------

display(df_bed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### since the plink and vcf schemas are unified, you can pipe the binary ped dataframe in as a VCF

# COMMAND ----------

plink_bed_freq_df = glow.transform('pipe', 
                                   df_bed.drop("position"), 
                                   cmd=cmd, 
                                   input_formatter='vcf',
                                   in_vcf_header='infer',
                                   output_formatter='csv',
                                   out_header='true')

# COMMAND ----------

display(plink_bed_freq_df)
