# Databricks notebook source
# MAGIC %md ---
# MAGIC title: Part 3, Train a disease classifier on RNA-seq data using Spark-ml
# MAGIC authors:
# MAGIC - William Brandler
# MAGIC - Wes Dias
# MAGIC tags:
# MAGIC - machine-learning
# MAGIC - random forests
# MAGIC - spark-ml
# MAGIC - python
# MAGIC - mlflow
# MAGIC - HLS
# MAGIC - genomics
# MAGIC - RNASeq
# MAGIC created_at: 2019-10-08
# MAGIC updated_at: 2021-04-26
# MAGIC tldr: Compare and contrast spark-ml with scikit-learn
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Links
# MAGIC - AWS demo.cloud: [https://demo.cloud.databricks.com/#notebook/4193271](https://demo.cloud.databricks.com/#notebook/4193271)

# COMMAND ----------

# MAGIC %md
# MAGIC ## <img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/>
# MAGIC
# MAGIC ## Train a disease classifier on RNA-seq data
# MAGIC
# MAGIC ## With tracking from <img width="90px" src="https://mlflow.org/docs/0.7.0/_static/MLflow-logo-final-black.png">
# MAGIC
# MAGIC ## Part 3: Model training with ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Spark-ml
# MAGIC
# MAGIC Here we train a Random Forest classifier on RNA-seq data from the [cancer cell line encyclopedia (CCLE)](https://portals.broadinstitute.org/ccle), and track the resulting models with MLflow
# MAGIC
# MAGIC Note: `cell lines` are immortalized human cells, which are grown in petri dishes and used to conduct cell biology experiments
# MAGIC
# MAGIC Note: the data volume here is not large enough to need spark-ml, and the performance is slower than using spark-sklearn
# MAGIC
# MAGIC [Part 1](https://demo.cloud.databricks.com/#notebook/4193227/command/4193230) | [Part 2](https://demo.cloud.databricks.com/#notebook/4192936/command/4192939) | [**Part 3**](https://demo.cloud.databricks.com/#notebook/4193271/command/4193274)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setup
# MAGIC
# MAGIC - Tested on Databricks Runtime 8.0 ML

# COMMAND ----------

import mlflow
import mlflow.spark

from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.window import *

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data prepared in Part 1

# COMMAND ----------

features_path = "dbfs:/mnt/databricks-datasets-private/HLS/rnaseq/rnaseq_features_with_metadata.parquet"

# COMMAND ----------

features_with_metadata = spark.read.parquet(features_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Map disease class to integer
# MAGIC
# MAGIC `pyspark.ml` library has a `StringIndexer` class that will map our disease name into an integer index

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="Histology", outputCol="diseaseIdx")
disease_id = indexer.fit(features_with_metadata)
features_with_disease_id = disease_id.transform(features_with_metadata)

display(features_with_disease_id.select("cell_line", "Histology", "diseaseIdx"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Convert array of gene expression per sample into `pyspark.ml`'s vector representation
# MAGIC
# MAGIC Use a PySpark user defined function (UDF) to do this remapping

# COMMAND ----------

list_to_vector_udf = udf(lambda l: Vectors.dense([x["tpkm"] for x in l]), VectorUDT())

features_as_vector = features_with_disease_id.withColumn("features", list_to_vector_udf(features_with_disease_id.features)).drop("expression_vector")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Train a `pyspark.ml` Random Forest classifier
# MAGIC
# MAGIC Please refer to the [pyspark.ml documentation](https://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html) for more details

# COMMAND ----------

rf = RandomForestClassifier(labelCol="diseaseIdx").setSeed(42)
pipeline = Pipeline(stages=[rf])

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) ParamGrid
# MAGIC
# MAGIC Use Spark's [ParamGridBuilder](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.ParamGridBuilder) to find the optimal hyperparameters
# MAGIC
# MAGIC For simplicity, here we are just going to tune on:
# MAGIC
# MAGIC 1. number of trees 
# MAGIC 2. maximum depth of those trees

# COMMAND ----------

paramGrid = ParamGridBuilder(). \
    addGrid(rf.numTrees, [2, 10, 50]). \
    addGrid(rf.maxDepth, [2, 5, 25]). \
    build()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Cross Validation
# MAGIC
# MAGIC We are also going to use 3-fold cross validation to identify the optimal maxDepth and numTrees combination
# MAGIC
# MAGIC ![crossValidation](http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/301/CrossValidation.png)
# MAGIC
# MAGIC With 3-fold cross-validation, we train on 2/3 of the data, and evaluate with the remaining (held-out) 1/3. We repeat this process 3 times, so each fold gets the chance to act as the validation set. We then average the results of the three rounds

# COMMAND ----------

evaluator = MulticlassClassificationEvaluator().setLabelCol("diseaseIdx")
cv = CrossValidator(estimator=pipeline,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator,
                    numFolds=3
                   )

# COMMAND ----------

train_df, validation_df = features_as_vector.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

with mlflow.start_run():
  cvModel = cv.fit(train_df)
  mlflow.set_tag('dataset', 'CCLE') # Logs user-defined tags
  predictions_df = cvModel.transform(validation_df)
  f1 = evaluator.evaluate(predictions_df)
  mlflow.log_metric('f1', f1) # Logs additional metrics
  mlflow.spark.log_model(spark_model=cvModel.bestModel, sample_input=validation_df, artifact_path='best-model')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Use built in display() function to assess how well the classifier matches up with our labels

# COMMAND ----------

display(predictions_df.select("cell_line", "Histology", "diseaseIdx", "prediction"))

# COMMAND ----------

display(features_with_metadata.groupBy("Histology").count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### View results of notebook experiment run in MLflow
# MAGIC
# MAGIC Note, the best performing models have the highest `maxDepth` and `numTrees`
# MAGIC
# MAGIC (further investigation is required to assess for overfitting)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/parallel_coordinates_rnaseq_mlflow.png" width="1200" />
