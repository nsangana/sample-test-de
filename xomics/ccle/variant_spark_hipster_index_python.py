# Databricks notebook source
# MAGIC %md ---
# MAGIC title: Polygenic Risk Scoring with 'VariantSpark'
# MAGIC
# MAGIC authors:
# MAGIC - William Brandler
# MAGIC - CSIRO Bioinformatics Australia
# MAGIC
# MAGIC tags:
# MAGIC - HLS
# MAGIC - machine-learning
# MAGIC - scikit-learn
# MAGIC - random-forest
# MAGIC - support-vector-machine
# MAGIC - python
# MAGIC - spark-sklearn
# MAGIC - mlflow
# MAGIC - genomics
# MAGIC - variant-spark
# MAGIC
# MAGIC created_at: 2019-05-10
# MAGIC
# MAGIC updated_at: 2019-05-10
# MAGIC
# MAGIC tldr: Training a classifier to predict polygenic risk for being a 'hipster' using genetic data
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Links
# MAGIC - AWS demo.cloud: [https://demo.cloud.databricks.com/#notebook/3122638](https://demo.cloud.databricks.com/#notebook/3122638)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Polygenic Risk Scoring with 'VariantSpark'
# MAGIC
# MAGIC ![Hipster-Index](https://s3.us-east-2.amazonaws.com/csiro-graphics/HipsterSignatureGraphic-new.png)
# MAGIC
# MAGIC * Machine learning for real-time genomic data analysis 
# MAGIC   * VCF data read into Spark Dataframes using Databricks' [VCF reader](https://docs.databricks.com/applications/genomics/variant-data.html#vcf)
# MAGIC   * Implements **Random Forests** and **SVMs**, using Apache Spark for hyperparameter tuning
# MAGIC   * Tracks models with mlFlow
# MAGIC   * Authored in Python, and inspired by the team at [CSIRO Bioinformatics](http://bioinformatics.csiro.au/) Australia
# MAGIC   
# MAGIC * The purpose of the demo is to find the most *important* variants attributing to a phenotype of interest 
# MAGIC   * Uses a synthetic phenotype called *HipsterIndex* 
# MAGIC   * Includes a dataset with a subset of the samples and variants 
# MAGIC   * Uses VCF format - from the 1000 Genomes Project

# COMMAND ----------

# MAGIC %md
# MAGIC ## About the Hipster Index
# MAGIC The synthetic HipsterIndex was created using the following genotypes and formula:
# MAGIC
# MAGIC | ID |SNP ID     | chromosome | position | phenotype | reference |
# MAGIC |---:|----------:|----:|-------:|-----:|----------:|------:|
# MAGIC | B6 |[rs2218065](https://www.ncbi.nlm.nih.gov/projects/SNP/snp_ref.cgi?rs=2218065) | chr2 | 223034082 | monobrow | [Adhikari K, et al. (2016) Nat Commun.](https://www.ncbi.nlm.nih.gov/pubmed/?term=26926045) |
# MAGIC | R1 |[rs1363387](https://www.ncbi.nlm.nih.gov/projects/SNP/snp_ref.cgi?rs=1363387) | chr5 | 126626044 | Retina horizontal cells (checks) | [Kay, JN et al. (2012) Nature](https://www.ncbi.nlm.nih.gov/pubmed/?term=22407321)
# MAGIC | B2 |[rs4864809](https://www.ncbi.nlm.nih.gov/projects/SNP/snp_ref.cgi?rs=4864809) | chr4 |  54511913 | beard | [Adhikari K, et al. (2016) Nat Commun.](https://www.ncbi.nlm.nih.gov/pubmed/?term=26926045)
# MAGIC | C2 |[rs4410790](https://www.ncbi.nlm.nih.gov/projects/SNP/snp_ref.cgi?rs=4410790)  |chr7 |17284577| coffee consumption        | [Cornelis MC et al. (2011) PLoS Genet.](https://www.ncbi.nlm.nih.gov/pubmed/?term=21490707) |
# MAGIC
# MAGIC `HipsterIndex = ((2 + GT[B6]) * (1.5 + GT[R1])) + ((0.5 + GT[C2]) * (1 + GT[B2]))`
# MAGIC
# MAGIC   GT stands for the genotype at this location with *homozygote* reference encoded as 0, *heterozygote* as 1 and *homozygote alternative* as 2. We then label individuals with a HipsterIndex score above 10 as hipsters, and the rest non-hipsters. By doing so, we created a binary annotation for the individuals in the 1000 Genome Project.
# MAGIC
# MAGIC In this notebook, we demonstrate the usage of VariantSpark to **reverse-engineer** the association of the selected SNPs to the phenotype of insterest (i.e. being a hipster).

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Setup -- Databricks Spark cluster using the HLS Runtime:  
# MAGIC
# MAGIC **Note**: The `knowledge-repo-cluster-hls` cluster is already set up accordingly. You can use it if available.
# MAGIC
# MAGIC
# MAGIC 1. Click 'Clusters' on the left toolbar on this page
# MAGIC 2. Click the blue button on the top left to create a cluster
# MAGIC 3. When on create cluster page, go to View -> Developer -> JavaScriptConsole (or Ctr+Shift+J on Windows)
# MAGIC 4. Paste `window.prefs.set("enableCustomSparkVersions", true)`` in the console, and press enter
# MAGIC 5. Refresh Page
# MAGIC 6. Now you should see a box for Custom Spark Version, in there enter:
# MAGIC   - `5.4.x-hls-scala2.11`
# MAGIC 7. Under Advanced Options -> Spark -> Spark Config paste:
# MAGIC   - `spark.sql.execution.arrow.enabled true`
# MAGIC   - this speeds up conversion of spark dataframes to pandas dataframes
# MAGIC 8. Attach **spark-sklearn** & **mlflow** libraries
# MAGIC   - Libraries -> Install New -> Library Source -> PyPi -> Package -> [enter library name]

# COMMAND ----------

vcf_dbfs_path = "dbfs:/mnt/databricks-datasets-private/HLS/variant-spark/hipster-index/hipster.vcf.bz2"
labels_dbfs_path = "dbfs:/mnt/databricks-datasets-private/HLS/variant-spark/hipster-index/hipster_labels.txt"

# COMMAND ----------

# DBTITLE 1,Import libraries
import pyspark.sql.functions as fx
from pyspark.sql.types import StringType

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from sklearn import grid_search, datasets, svm
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from spark_sklearn import GridSearchCV

import mlflow
import mlflow.sklearn

mlflow.set_tracking_uri("databricks")

# COMMAND ----------

# DBTITLE 1,Helper functions
def explode_genotypes(vcf):
  """
  explode vcf to one sample one genotype per row
  """
  return vcf.withColumn("genotypes", fx.explode(fx.col("genotypes")))

def etl_genotypes(vcf):
  """
  transform vcf to get unique identifiers for each variant (rsId & variant)
  also get genotypes as an integer (instead of an array such as '0,1')
  """
  return vcf.withColumn("rsId", fx.col("names")[0]). \
            withColumn("variant", fx.concat(fx.col("contigName"), fx.lit("_"), fx.col("start").cast(StringType()))). \
            withColumn("sampleId", fx.col("genotypes.sampleId")). \
            withColumn("genotype", fx.col("genotypes.genotype.calls")[0] + fx.col("genotypes.genotype.calls")[1]). \
            select("variant", "rsId", "sampleId", "genotype")

def collect_list_genotypes(vcf):
  """
  collect_list so each variant has all the associated genotypes in an array
  """
  return vcf.groupBy("rsId", "variant").agg(fx.collect_list("sampleId").alias("sampleId"), 
                                                    fx.collect_list("genotype").alias("genotypes")
                                                   ). \
                   where((fx.col("rsId") != "rs536696373") & (fx.col("rsId") != "rs145741270"))

def transpose_features(df, col_name):
  """
  transpose features into np_array for ml with sklearn
  """
  return np.vstack(np.asarray(df[col_name], order = 'C')).T

def column_to_list(vcf, col_name="rsId"):
  """
  get column from pandas dataframe and convert to python list
  TODO: use a join instead to create dataframe (this won't scale)
  """
  return vcf.toPandas()[col_name].tolist()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Load variants and prepare them for machine learning with scikit-learn

# COMMAND ----------

vcf = spark.read.format("com.databricks.vcf"). \
                 option("includeSampleIds", "true"). \
                 load(vcf_dbfs_path)
display(vcf)

# COMMAND ----------

vcf = explode_genotypes(vcf)
vcf = etl_genotypes(vcf)
vcf = collect_list_genotypes(vcf)

# COMMAND ----------

labels = spark.read.option("header", True).csv(labels_dbfs_path).drop("score")
labels = labels.toPandas()
display(labels)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Train Random Forest
# MAGIC
# MAGIC Using [spark-sklearn](https://github.com/databricks/spark-sklearn) to train and evaluate multiple scikit-learn models in parallel

# COMMAND ----------

X = transpose_features(vcf.toPandas(), "genotypes")
y = np.array(labels['label'])

# COMMAND ----------

param_grid = {"max_depth": [3, None],
              "max_features": [0.1, 0.3, 1.0],
              "min_samples_split": [0.2, 0.3, 1.0],
              "min_samples_leaf": [0.1, 0.3, 0.5],
              "bootstrap": [True, False],
              "criterion": ["gini", "entropy"],
              "n_estimators": [10, 20, 100, 200]}
clf = GridSearchCV(sc, RandomForestClassifier(), param_grid=param_grid)
clf.fit(X, y)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get best Random Forest Estimator, and log params and metrics with mlFlow 

# COMMAND ----------

with mlflow.start_run():
  best_clf = clf.best_estimator_
  for param in best_clf.estimator_params:
    mlflow.log_param(param, getattr(best_clf, param))
  
  best_clf.fit(X, y)
  scores = cross_val_score(best_clf, X, y, cv=5)
  mlflow.log_metric("accuracy", scores.mean())
  mlflow.log_metric("95CI", scores.std() * 2)
  mlflow.sklearn.log_model(best_clf, "random_forest")

# COMMAND ----------

rsId_list = column_to_list(vcf, "rsId")
variant_list = column_to_list(vcf, "variant")
feature_importances_list = best_clf.feature_importances_
list_of_tuples = list(zip(rsId_list, 
                          variant_list, 
                          feature_importances_list)
                     )
feature_importances = pd.DataFrame(list_of_tuples, 
                                   columns = ['rsId', 'variant', 'importance']
                                  ). \
                         sort_values('importance', ascending=False)

display(feature_importances)

# COMMAND ----------

ax = feature_importances.head(10).plot(x="variant", 
                                       y="importance",
                                       lw=3,
                                       colormap='Reds_r',
                                       title='Importance in Descending Order', 
                                       fontsize=9)
ax.set_xlabel("variable")
ax.set_ylabel("importance")
plt.xticks(rotation=12)
plt.grid(True)
plt.show()
display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Train SVM

# COMMAND ----------

parameters = {'kernel':('linear', 'rbf'), 
              'C':[1, 10], 
              'gamma':[1, 10, 100],
              'tol':[0.01, 0.1, 1]
             }
svr = svm.SVC()
clf = GridSearchCV(sc, svr, parameters)
clf.fit(X,y)

# COMMAND ----------

with mlflow.start_run():
  best_clf = clf.best_estimator_
  for key, value in best_clf.get_params().items():
    mlflow.log_param(key, value)
  
  best_clf.fit(X, y)
  scores = cross_val_score(best_clf, X, y, cv=5)
  mlflow.log_metric("accuracy", scores.mean())
  mlflow.log_metric("95CI", scores.std() * 2)
  mlflow.sklearn.log_model(best_clf, "svm")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Click 'Runs' tab on top-right of notebook to view mlFlow runs

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC The plot above shows that VariantSpark has recovered the correct genotypes of this multivariate phenotype with interacting features (multiplicative and additive effects). 
# MAGIC
# MAGIC 1. A SNP encoding for the MEGF10 gene (__chr5_126626044__), which is involved in Retina horizontal cell formation   
# MAGIC    as the second most important marker, explaining why hipsters prefer checked shirts
# MAGIC 2. __chr2_223034082__ (rs2218065) encoding for monobrow
# MAGIC 3. __chr4_54511913__ (rs4864809) the marker for beards is third
# MAGIC 4. __chr7_17284577__ (rs4410790) the marker for increased coffee consuption had no influence on the random forest model
# MAGIC
# MAGIC The last variant had no influence compared to what we expect from the formula of the HipsterIndex, however with a low weight it may have been difficult to pick out above the noise.
# MAGIC
# MAGIC Finally, the above Random Forest learned to ignore SNPs that correlate with the associated variant (based on proximity), but that have no direct effect on the trait of interest. This correlation between SNPs is known as [linkage disequilibrium](https://en.wikipedia.org/wiki/Linkage_disequilibrium)
