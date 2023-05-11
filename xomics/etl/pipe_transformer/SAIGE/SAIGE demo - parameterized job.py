# Databricks notebook source
# MAGIC %md
# MAGIC ### From https://docs.databricks.com/applications/genomics/tertiary/saige.html#saige-notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup widgets
# MAGIC
# MAGIC Widgets are notebook-level parameters and can be set when scheduling a job.  
# MAGIC
# MAGIC For creating widgets in a notebook, see: https://docs.databricks.com/notebooks/widgets.html
# MAGIC
# MAGIC For setting parameters when scheduling a job, see: https://docs.databricks.com/jobs.html#create-a-job

# COMMAND ----------

# You only run this code once, to add the widgets to the top of the notebook, once they are there, you can remove this cell.
# When creating a widget, you can provide a default value.
# dbutils.widgets.text( "input_vcf"        , "" )
# dbutils.widgets.text( "sampleFile"       , "" )
# dbutils.widgets.text( "GMMATmodelFile"   , "" )
# dbutils.widgets.text( "varianceRatioFile", "" )
# dbutils.widgets.text( "vcfField"         , "GT"     )
# dbutils.widgets.text( "chrom"            , "22"     )
# dbutils.widgets.text( "minMAF"           , "0.0001" )
# dbutils.widgets.text( "minMAC"           , "1"      )
# dbutils.widgets.text( "output_delta_path", "" )

# COMMAND ----------

# If you added a widget, then want to change it, or remove it, you can do so with
#   dbtuils.widgets.remove( "...widget name..." )
# or remove all widgets with
#   dbutils.widgets.removeAll()

# COMMAND ----------

# Get the parameter values to be used in the following cells
input_vcf         = dbutils.widgets.get( "input_vcf"         )
sampleFile        = dbutils.widgets.get( "sampleFile"        )
GMMATmodelFile    = dbutils.widgets.get( "GMMATmodelFile"    )
varianceRatioFile = dbutils.widgets.get( "varianceRatioFile" )
vcfField          = dbutils.widgets.get( "vcfField"          )
chrom             = dbutils.widgets.get( "chrom"             )
minMAF            = dbutils.widgets.get( "minMAF"            )
minMAC            = dbutils.widgets.get( "minMAC"            )
output_delta_path = dbutils.widgets.get( "output_delta_path" )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### From here, the rest of the notebook proceeds as before
# MAGIC
# MAGIC We just use the variables to fill in values when we create the `scriptfile`

# COMMAND ----------

input_df = spark.read.format( "com.databricks.vcf" ).load( input_vcf )

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
    --sampleFile=%(sampleFile)s \
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
""" % { "sampleFile": sampleFile, "GMMATmodelFile": GMMATmodelFile, "varianceRatioFile": varianceRatioFile, "vcfField": vcfField, "chrom": chrom, "minMAF": minMAF, "minMAC": minMAC }

cmd = json.dumps(["bash", "-c", scriptFile])

# COMMAND ----------

# DBTITLE 1,Print command just for visual inspection
print( cmd )

# COMMAND ----------

import glow
output_df = glow.transform( "pipe", input_df, cmd=cmd, input_formatter='vcf', in_vcf_header=input_vcf, output_formatter='csv', out_header='true', out_delimiter=' ')

# COMMAND ----------

output_df.write.format( "delta" ).mode( "overwrite" ).save( output_delta_path )
