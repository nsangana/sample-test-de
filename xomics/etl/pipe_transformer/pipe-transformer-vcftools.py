# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC
# MAGIC ### Run vcftools using the <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> [Pipe Transformer](https://glow.readthedocs.io/en/latest/tertiary/pipe-transformer.html)

# COMMAND ----------

# MAGIC %md
# MAGIC #### create init script
# MAGIC
# MAGIC The init script downloads vcftools and installs it across the Spark cluster
# MAGIC
# MAGIC Upload the init script to dbfs with the Databricks file system [CLI](https://docs.databricks.com/dev-tools/databricks-cli.html#dbfs-cli), 
# MAGIC
# MAGIC or run the command below on a different cluster
# MAGIC
# MAGIC Note: Cluster node [init scripts](https://docs.databricks.com/clusters/init-scripts.html#cluster-node-initialization-scripts) run during startup for each cluster node before the Spark driver or worker JVM starts
# MAGIC
# MAGIC An alternative is to use Databricks Container Services ([DCS](https://docs.databricks.com/clusters/custom-containers.html))

# COMMAND ----------

dbutils.fs.put("dbfs:/mnt/wbrandler/init/vcftools_init.sh" ,"""
#!/bin/bash
sudo apt update
sudo apt -y install autoconf pkg-config libtool
git clone https://github.com/vcftools/vcftools.git
cd vcftools
mkdir /mnt/dbnucleus/lib/vcftools/
cp -r * /mnt/dbnucleus/lib/vcftools/
cd /mnt/dbnucleus/lib/vcftools/
./autogen.sh
./configure
make
make install
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC check vcftools was correctly installed across the cluster,
# MAGIC using the provided init script above

# COMMAND ----------

# MAGIC %sh
# MAGIC vcftools

# COMMAND ----------

import glow
import json

# COMMAND ----------

# MAGIC %md
# MAGIC here we load the example vcf file from `/databricks-datasets/`

# COMMAND ----------

vcf_path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"

# COMMAND ----------

vcf = spark.read.format("com.databricks.vcf"). \
                 option("splitToBiallelic", True). \
                 option("includeSampleIds", True). \
                 load(vcf_path). \
                 limit(1000). \
                 cache()

# COMMAND ----------

display(vcf)

# COMMAND ----------

vcf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### run vcftools on a VCF with the pipe transformer!
# MAGIC
# MAGIC Here we are going to remove all indels

# COMMAND ----------

scriptFile = r"""#!/bin/sh
set -e
#input vcf is stdin, signified by '-'
vcftools --vcf - --remove-indels --recode --recode-INFO-all --stdout
"""

cmd = json.dumps(["bash", "-c", scriptFile])

# COMMAND ----------

vcf_snps = glow.transform(
                'pipe',
                vcf,
                cmd=cmd,
                input_formatter='vcf',
                in_vcf_header='infer',
                output_formatter='vcf'
)

# COMMAND ----------

vcf_snps.count()

# COMMAND ----------

display(vcf_snps)
