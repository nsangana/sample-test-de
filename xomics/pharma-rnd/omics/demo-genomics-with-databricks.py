# Databricks notebook source
# MAGIC %md
# MAGIC # Unifying multi-omics data together on <img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="180"/>
# MAGIC  
# MAGIC Healthcare, life sciences, and agricultural companies are generating petabytes of data, whether through genome sequencing,
# MAGIC electronic health records, imaging systems, or the Internet of Medical Things. The value of these datasets grows when we are able
# MAGIC to blend them together, such as integrating genomics and EHR-derived phenotypes for target discovery, or blending IoMT data with
# MAGIC medical images to predict patient disease severity. 

# COMMAND ----------

# MAGIC %pip install bokeh umap-learn umap-learn[plot]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linking genomics with disparate data sources is the key to unlocking precision medicine
# MAGIC
# MAGIC <img src="https://amir-hls.s3.us-east-2.amazonaws.com/public/use_cases.png" width="800"/>
# MAGIC
# MAGIC <!-- <img src="https://databricks.com/wp-content/uploads/2020/10/at-risk-ar-og.png" width="800"/>  -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Glow

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variant Analysis and Genome Wide Association Study
# MAGIC
# MAGIC <img src="https://glow.readthedocs.io/en/latest/_images/glow_ref_arch_genomics.png" width="400"/><br>
# MAGIC Genomics data has been doubling every seven months globally. It has now reached a scale where genomics has become a big data problem.<br>
# MAGIC However, most of the tools for working with genomics data are built to work on single nodes and will not scale. Furthermore, it has become challenging for scientists to manage storage of public data.
# MAGIC Glow solves these problems by bridging bioinformatics and the big data ecosystem.<br>
# MAGIC It enables bioinformaticians and computational biologists to leverage best practices<br>
# MAGIC used by data engineers and data scientists across industry.<br>
# MAGIC
# MAGIC **GloWGR: [Whole Genome Regression](https://demo.cloud.databricks.com/#notebook/7639496/command/7639497)**
# MAGIC
# MAGIC <img src="https://glow.readthedocs.io/en/latest/_images/wgr_runtime.png" width="400"/><br>
# MAGIC <!-- <img src="https://amir-hls.s3.us-east-2.amazonaws.com/public/gloWGR.png" width="800"/><img src="https://glow.readthedocs.io/en/latest/_images/wgr_runtime.png" width="400"/><br> -->

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examples

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Ingest public genotype data into a [data lake](https://demo.cloud.databricks.com/#notebook/7639333/) that acts as a single source of truth

# COMMAND ----------

import glow
from pyspark.sql.functions import *
vcf_path = '/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz'
vcf_df = spark.read.format("vcf").option('includeSampleIds',True).load(vcf_path)
vcf_df.createOrReplaceTempView('genomics_view')
display(vcf_df.limit(2))

# COMMAND ----------

vcf_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC perform point queries

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from genomics_view where referenceAllele='A'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run [quality control](https://demo.cloud.databricks.com/#notebook/7639386) and [statistical analysis](https://demo.cloud.databricks.com/#notebook/7639328)

# COMMAND ----------

# DBTITLE 0,Show per-sample QC metrics
display(
  vcf_df
  .selectExpr("sample_call_summary_stats(genotypes, referenceAllele, alternateAlleles) as qc")
  .selectExpr("explode(qc) as per_sample_qc")
  .selectExpr("expand_struct(per_sample_qc)")
  .cache()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Make it easity to perform advanced analytics on genomic data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example: Perform dimensionality reduction to inspect population structure with UMAP
# MAGIC It is easy to perform routine ML workloads on genomics data once the data is ingested into the lakehouse. For example, 
# MAGIC lets take a look at the population structure based on a 2d projection of a subset of genomics variants.

# COMMAND ----------

from glow.wgr import get_sample_ids
selected_variants=vcf_df.sample(0.1,False).cache()
sample_ids = get_sample_ids(selected_variants)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
def get_genotypes(arr):
  genotypes = [int(m[0]+m[1]) for m in arr]
  return(genotypes)

get_genotypes_udf = udf(get_genotypes, ArrayType(IntegerType()))
gt_df=selected_variants.select(get_genotypes_udf(col("genotypes.calls")).alias("calls"),"genotypes.sampleId")

# COMMAND ----------

variant_major_gt_array_pd = gt_df.select("calls").toPandas()

# COMMAND ----------

selected_variants.count()

# COMMAND ----------

import pandas as pd
import numpy as np
gt_features=np.concatenate(variant_major_gt_array_pd['calls'],axis=0)
n_samp=len(variant_major_gt_array_pd['calls'][0])
n_loci=int(gt_features.shape[0]/n_samp)
gt_features = gt_features.reshape(n_loci,n_samp)
gt_features_tr = gt_features.transpose()

# COMMAND ----------

panel_info = pd.read_csv("/dbfs/databricks-datasets/samples/adam/panel/integrated_call_samples_v3.20130502.ALL.panel",sep="\t")
sample_pops=panel_info[["sample","pop","super_pop"]]
samples_pd=pd.DataFrame(sample_ids,columns=["sample"])
samples_pops_pd=samples_pd.merge(sample_pops)

# COMMAND ----------

from bokeh.plotting import figure, output_file, show
from bokeh.resources import CDN
from bokeh.embed import file_html

import umap
import umap.plot

# COMMAND ----------

params ={'n_neighbors':15,
        'min_dist':0.1,
        'n_components':2,
        }
mapper = umap.UMAP(**params).fit(gt_features_tr)

# COMMAND ----------

assert params['n_components']==2
selected_lablel='super_pop'
hover_data = pd.DataFrame({'index':np.arange(n_samp), 'label':samples_pops_pd[selected_lablel]})
p = umap.plot.interactive(mapper, labels=samples_pops_pd[selected_lablel], hover_data=hover_data, point_size=2)
html = file_html(p, CDN, "1KG Populations")
displayHTML(html)

# COMMAND ----------


