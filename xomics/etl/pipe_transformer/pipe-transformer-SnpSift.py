# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC
# MAGIC ### Run SnpSift using the <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> [Pipe Transformer](https://glow.readthedocs.io/en/latest/tertiary/pipe-transformer.html)

# COMMAND ----------

# MAGIC %md
# MAGIC #### create init script
# MAGIC
# MAGIC The init script downloads SnpEff/SnpSift and installs it across the Spark cluster
# MAGIC
# MAGIC Upload the init script to dbfs with the Databricks file system [CLI](https://docs.databricks.com/dev-tools/databricks-cli.html#dbfs-cli), 
# MAGIC
# MAGIC or run the command below on a different cluster
# MAGIC
# MAGIC Note: Cluster node [init scripts](https://docs.databricks.com/clusters/init-scripts.html#cluster-node-initialization-scripts) run during startup for each cluster node before the Spark driver or worker JVM starts
# MAGIC
# MAGIC An alternative is to use Databricks Container Services ([DCS](https://docs.databricks.com/clusters/custom-containers.html))

# COMMAND ----------

dbutils.fs.put("dbfs:/mnt/wbrandler/init/snpsift_init.sh" ,"""
#!/bin/bash
wget http://sourceforge.net/projects/snpeff/files/snpEff_latest_core.zip
unzip snpEff_latest_core.zip
cd snpEff
mkdir /mnt/dbnucleus/lib/snpEff/
cp -r * /mnt/dbnucleus/lib/snpEff/
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC check snpEff/snpSift was correctly installed across the cluster,
# MAGIC using the provided init script above

# COMMAND ----------

# MAGIC %sh
# MAGIC java -jar /mnt/dbnucleus/lib/snpEff/snpEff.jar databases

# COMMAND ----------

import glow
import json

# COMMAND ----------

# MAGIC %md
# MAGIC here we load the example vcf file that has been run through the Databricks [DNASeq pipeline](https://docs.databricks.com/applications/genomics/secondary/dnaseq-pipeline.html)

# COMMAND ----------

vcf_path = "dbfs:/mnt/wbrandler/test/dnaseq/genotypes.vcf/NA12878.vcf"

# COMMAND ----------

vcf = spark.read.format("com.databricks.vcf"). \
                 load(vcf_path). \
                 limit(1000). \
                 cache()

# COMMAND ----------

display(vcf)

# COMMAND ----------

vcf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### run SnpSift on VCF with the pipe transformer!
# MAGIC
# MAGIC Here we are going to annotate with a preannotated VCF (from SnpEff)

# COMMAND ----------

scriptFile = r"""#!/bin/sh
set -e
#input vcf is stdin, signified by '-'
cat - | java -jar /mnt/dbnucleus/lib/snpEff/SnpSift.jar annotate /dbfs/mnt/wbrandler/test/dnaseq/annotations.vcf/NA12878.vcf
"""

cmd = json.dumps(["bash", "-c", scriptFile])

# COMMAND ----------

vcf_ann = glow.transform(
                'pipe',
                vcf,
                cmd=cmd,
                input_formatter='vcf',
                in_vcf_header='infer',
                output_formatter='vcf'
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### The new VCF has the `INFO_ANN` column with the annotations

# COMMAND ----------

display(vcf_ann)

# COMMAND ----------

vcf_ann.printSchema()

# COMMAND ----------


