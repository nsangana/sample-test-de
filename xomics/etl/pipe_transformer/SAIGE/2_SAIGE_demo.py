# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC
# MAGIC ### Run SAIGE using the <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> [Pipe Transformer](https://glow.readthedocs.io/en/latest/tertiary/pipe-transformer.html)
# MAGIC
# MAGIC #### To learn more, see [Databricks SAIGE docs](https://docs.databricks.com/applications/genomics/tertiary/saige.html#saige-notebook)

# COMMAND ----------

import pyspark.sql.functions as fx

# COMMAND ----------

input_vcf = "dbfs:/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
GMMATmodelFile = "/dbfs/home/william.brandler@databricks.com/data/1kg/step-1/output.rda"
varianceRatioFile = "/dbfs/home/william.brandler@databricks.com/data/1kg/step-1/output.varianceRatio.txt"
vcfField = "GT"
chrom = "22"
minMAF = 0.00001
minMAC = 1

# COMMAND ----------

input_df = spark.read.format("com.databricks.vcf").load(input_vcf)

# COMMAND ----------

display(input_df)

# COMMAND ----------

# MAGIC %sh
# MAGIC Rscript /opt/SAIGE/extdata/step2_SPAtests.R --help

# COMMAND ----------

import json

scriptFile = r"""#!/bin/sh
set -e

export tmpdir=$(mktemp -d -t vcf.XXXXXX)
cat - > ${tmpdir}/input.vcf
bgzip -c ${tmpdir}/input.vcf > ${tmpdir}/input.vcf.gz
tabix -p vcf ${tmpdir}/input.vcf.gz

cd /opt/SAIGE
Rscript extdata/step2_SPAtests.R \
    --vcfFile=${tmpdir}/input.vcf.gz \
    --vcfFileIndex=${tmpdir}/input.vcf.gz.tbi \
    --SAIGEOutputFile=${tmpdir}/output.txt \
    --GMMATmodelFile=%(GMMATmodelFile)s \
    --varianceRatioFile=%(varianceRatioFile)s \
    --vcfField=%(vcfField)s \
    --chrom=%(chrom)s \
    --minMAF=%(minMAF)s \
    --minMAC=%(minMAC)s \
    --numLinesOutput=2 \
    >&2
    
cat ${tmpdir}/output.txt
rm -rf ${tmpdir}
""" % {"GMMATmodelFile": GMMATmodelFile, 
       "varianceRatioFile": varianceRatioFile, 
       "vcfField": vcfField, 
       "chrom": chrom, 
       "minMAF": minMAF, 
       "minMAC": minMAC }

cmd = json.dumps(["bash", "-c", scriptFile])

# COMMAND ----------

# DBTITLE 1,Print command just for visual inspection
print( cmd )

# COMMAND ----------

import glow
output_df = glow.transform("pipe", 
                           input_df, 
                           cmd=cmd, 
                           input_formatter='vcf', 
                           in_vcf_header=input_vcf,
                           output_formatter='csv', 
                           out_header='true', 
                           out_delimiter=' ')

# COMMAND ----------

display(output_df.sort(fx.asc("`p.value`")))

# COMMAND ----------

output_df.count()

# COMMAND ----------


