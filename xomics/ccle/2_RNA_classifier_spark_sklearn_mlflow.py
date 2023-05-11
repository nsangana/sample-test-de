# Databricks notebook source
# MAGIC %md ---
# MAGIC title: Part 2, Train a disease classifier on RNA-seq data using Hyperopt-Spark-MLflow integration
# MAGIC authors:
# MAGIC - William Brandler
# MAGIC - Wes Dias
# MAGIC tags:
# MAGIC - machine-learning
# MAGIC - random forests
# MAGIC - hyperopt
# MAGIC - scikit-learn
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
# MAGIC - AWS demo.cloud: [https://demo.cloud.databricks.com/#notebook/4192936](https://demo.cloud.databricks.com/#notebook/4192936)

# COMMAND ----------

# MAGIC %md
# MAGIC ## <img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/>
# MAGIC
# MAGIC ## Train a disease classifier on RNA-seq data
# MAGIC
# MAGIC ## With tracking from <img width="90px" src="https://mlflow.org/docs/0.7.0/_static/MLflow-logo-final-black.png">
# MAGIC
# MAGIC ## Part 2: Model training with ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Hyperopt-Spark-MLflow integration
# MAGIC
# MAGIC Here we train a Random Forest classifier on RNA-seq data from the [cancer cell line encyclopedia (CCLE)](https://portals.broadinstitute.org/ccle), and track the resulting models automatically with MLflow
# MAGIC
# MAGIC Note: `cell lines` are immortalized human cells, which are grown in petri dishes and used to conduct cell biology experiments
# MAGIC
# MAGIC [Part 1](https://demo.cloud.databricks.com/#notebook/4193227/command/4193230) | [**Part 2**](https://demo.cloud.databricks.com/#notebook/4192936/command/4192939) | [Part 3](https://demo.cloud.databricks.com/#notebook/4193271/command/4193274)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setup
# MAGIC
# MAGIC - Tested on Databricks Runtime 8.0 ML
# MAGIC - scikit-learn is optimized for in memory processing
# MAGIC - Note, we are operating on downsampled data, the full dataset requires a larger instance type

# COMMAND ----------

import os

import mlflow
import mlflow.sklearn

from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.window import *

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from sklearn import preprocessing
# from sklearn.ensemble.forest import RandomForestClassifier # library package changed for scikit-learn 0.24.2
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

import hyperopt as hp
from hyperopt.pyll.base import scope
from hyperopt import fmin, rand, tpe, hp, Trials, exceptions, space_eval, STATUS_FAIL, STATUS_OK
from hyperopt import SparkTrials

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Load data from Part 1

# COMMAND ----------

transcripts_index_path = "dbfs:/mnt/databricks-datasets-private/HLS/rnaseq/transcripts_index.parquet"
features_path = "dbfs:/mnt/databricks-datasets-private/HLS/rnaseq/rnaseq_features_with_metadata.parquet"

# COMMAND ----------

features_with_metadata = spark.read.parquet(features_path).withColumn("tpkm", col("features.tpkm"))
splits = features_with_metadata.randomSplit([0.8, 0.2], 42)
train_df = splits[0]
val_df = splits[1]

# COMMAND ----------

display(train_df.select("cell_line", "Histology", "features").limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Prep for scikit-learn
# MAGIC
# MAGIC Convert `toPandas()` then to numpy arrays, encode labels and transform features

# COMMAND ----------

def features_to_np_array(df, col_name):
  """
  convert column to numpy array and stack arrays row-wise 
  to prepare for ml with sklearn
  """
  return np.vstack(np.asarray(df[col_name], order = 'C'))

def get_labels_features(df_pd):
  """
  given a pandas dataframe, return features and labels and numpy arrays 
  """
  le = preprocessing.LabelEncoder()
  le.fit(df_pd['Histology'])
  X = features_to_np_array(df_pd, "tpkm")
  y = np.array(le.transform(df_pd['Histology']))
  return X, y

# COMMAND ----------

x_train, y_train = get_labels_features(train_df.toPandas())
x_val, y_val = get_labels_features(val_df.toPandas())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write numpy arrays
# MAGIC
# MAGIC Files are written to the high performance `/dbfs/` FUSE mount
# MAGIC
# MAGIC Data will then be re-read in later on Spark workers during distributed hyperparameter tuning

# COMMAND ----------

temp_path = "/dbfs/ml/tmp/KnowledgeRepo/distributed_hyperopt/"
dbutils.fs.mkdirs("dbfs:/ml/tmp/KnowledgeRepo/distributed_hyperopt")

x_train_path = os.path.join(temp_path, 'x_train.npy')
y_train_path = os.path.join(temp_path, 'y_train.npy')
x_val_path = os.path.join(temp_path, 'x_val.npy')
y_val_path = os.path.join(temp_path, 'y_val.npy')

np.save(x_train_path, x_train)
np.save(y_train_path, y_train)
np.save(x_val_path, x_val)
np.save(y_val_path, y_val)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train Random Forest
# MAGIC
# MAGIC Using hyperopt-spark-mlflow [integration](https://docs.databricks.com/applications/machine-learning/automl/hyperopt/hyperopt-spark-mlflow-integration.html) to train and evaluate multiple scikit-learn models in parallel

# COMMAND ----------

def get_model(params):
  """
  This function creates a random forest model using the given hyperparameters.
  
  :param params: This dict of parameters specifies hyperparameters
  :return: an untrained model 
  """
  criterion = params['criterion']
  n_estimators = int(params['n_estimators'])
  max_depth = int(params['max_depth'])
    
  model = RandomForestClassifier(bootstrap=True, class_weight=None, criterion=criterion,
            max_depth=max_depth, max_features='auto', max_leaf_nodes=None,
            min_samples_leaf=1,
            min_samples_split=2, min_weight_fraction_leaf=0.0,
            n_estimators=n_estimators, n_jobs=1, oob_score=True,
            verbose=0, warm_start=False)
  
  return model

def train(params):
  """
  This function trains and evaluates a model using the given hyperparameters.
  
  :param params: This dict of parameters specifies hyperparameters.
  :return: a trained model and the resulting validation loss
  """
  
  X = np.load(x_train_path)
  y = np.load(y_train_path)
  model = get_model(params)
  model.fit(X, y)
  
  return model, 1 - model.oob_score_

def evaluate_hyperparams(params):
  """
  This method will be passed to `hyperopt.fmin()`.  It fits and evaluates the model using the given hyperparameters to get the validation loss.
  
  :param params: This dict of parameters specifies hyperparameter values to test.
  :return: dict with fields 'loss' (scalar loss) and 'status' (success/failure status of run)
  """
  
  # Train the model
  model, score = train(params)
  mlflow.log_param('impurity', params['criterion']) #hp.choice logs indices, so need to explicitly log here
  mlflow.sklearn.log_model(model, "test")
      
  return {'loss': score, 'status': STATUS_OK}

# COMMAND ----------

criteria = ['gini', 'entropy']
search_space = {
  'max_depth': scope.int(hp.quniform('max_depth', 2, 25, 1)),
  'n_estimators': scope.int(hp.quniform('n_estimators', 2, 25, 1)),
  'criterion': hp.choice('criterion', criteria)
}

# COMMAND ----------

# The algoritm to perform the parameter search
algo=tpe.suggest  # Tree of Parzen Estimators (a "Bayesian" method)
#algo=random.suggest  # Random search

# Configure parallelization
spark_trials = SparkTrials(parallelism=2)

# Execute the search with MLFlow support
with mlflow.start_run():
  argmin = fmin(
    fn=evaluate_hyperparams,
    space=search_space,
    algo=algo,
    max_evals=20,
    trials=spark_trials)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Recreate the best model

# COMMAND ----------

argmin

# COMMAND ----------

# hp.choice returns the indices for gini and entropy to mlflow, but we can retrieve them
argmin['criterion'] = criteria[argmin['criterion']]

model, loss = train(argmin)

print("Validation loss: {}".format(loss))

# COMMAND ----------

model

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Validate on the test data

# COMMAND ----------

# Read the data and create a test datagenerator
X = np.load(x_val_path)
y = np.load(y_val_path)

predictions = model.predict(X)

# Output some metrics
print("Accuracy: {}".format(accuracy_score(y, predictions)))
print(classification_report(y, predictions))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize most important features

# COMMAND ----------

feature_importances_list = model.feature_importances_

# COMMAND ----------

transcripts_index_df = spark.read.parquet(transcripts_index_path).toPandas()
feature_importances = pd.DataFrame(feature_importances_list, columns = ['importance'])
feature_importances_df = pd.concat([feature_importances, transcripts_index_df], axis=1). \
                            sort_values('importance', ascending=False)

# COMMAND ----------

ax = feature_importances_df.head(10).plot(x="gene_id", 
                                          y="importance",
                                          lw=3,
                                          colormap='Reds_r',
                                          title='Importance in Descending Order', 
                                          fontsize=9)
ax.set_xlabel("gene")
ax.set_ylabel("importance")
top_genes = feature_importances_df.head(10)["gene_id"].tolist()
plt.xticks(np.arange(10), top_genes, rotation=12)
plt.grid(True)
plt.tight_layout()
plt.show()
display()

# COMMAND ----------

dbutils.fs.rm("dbfs:/ml/tmp/KnowledgeRepo/distributed_hyperopt", True)
