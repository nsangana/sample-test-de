# Databricks notebook source
# MAGIC %md 
# MAGIC # Using MLFLow to develop models from docked results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Step
# MAGIC <img src="https://raw.githubusercontent.com/bluerider/distributed_qsar/master/QSAR%20Architecture_mlflow.png" width = "100%">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup dataset

# COMMAND ----------

# DBTITLE 1,Vina Results Dataset
# MAGIC %sql
# MAGIC
# MAGIC SELECT SMILES, 
# MAGIC        ZINC_ID,
# MAGIC        mwt,
# MAGIC        logp,
# MAGIC        reactive,
# MAGIC        purchasable,
# MAGIC        tranche_name,
# MAGIC        name as receptor,
# MAGIC        bounding_box,
# MAGIC --        vina_results.docked_conformer_pdbqt
# MAGIC        vina_results.docked_conformer_vina_results
# MAGIC FROM VINA_RESULTS;

# COMMAND ----------

# DBTITLE 1,Featurize Dataset
# MAGIC %python
# MAGIC
# MAGIC from pyspark.ml.feature import VectorAssembler, StringIndexer
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC """
# MAGIC Prepare data for decision tree training
# MAGIC """
# MAGIC
# MAGIC # setup some variables for column filtering
# MAGIC _string_columns = ("SMILES", "ZINC_ID", "TRANCHE_NAME")
# MAGIC _filtered_columns = _string_columns + ("MWT", "LOGP", "REACTIVE")
# MAGIC _indexed_columns = ("SMILES_INDEX", "ZINC_ID_INDEX", "TRANCHE_NAME_INDEX")
# MAGIC _features = _indexed_columns + ("MWT", "LOGP", "REACTIVE")
# MAGIC
# MAGIC # load the vina results into a dataframe
# MAGIC _df = spark.read\
# MAGIC            .format("delta")\
# MAGIC            .table("vina_results")\
# MAGIC            .withColumn("binding_affinity", col("vina_results").docked_conformer_vina_results["binding_affinity"])\
# MAGIC            .select(*_string_columns,
# MAGIC                    col("MWT").cast("float"),
# MAGIC                    col("LOGP").cast("float"),
# MAGIC                    col("REACTIVE").cast("float"),
# MAGIC                    "binding_affinity")
# MAGIC         
# MAGIC # index the strings in columns
# MAGIC _indexer = StringIndexer(inputCols = _string_columns,
# MAGIC                          outputCols = _indexed_columns)
# MAGIC _df_indexed = _indexer.fit(_df).transform(_df)
# MAGIC
# MAGIC # generate the feature vectors for ML
# MAGIC _assembler = VectorAssembler(inputCols = _features,
# MAGIC                              outputCol = "features")
# MAGIC _dataset = _assembler.transform(_df_indexed)
# MAGIC
# MAGIC # dataset for training
# MAGIC display(_dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup the 70:30 split

# COMMAND ----------

# DBTITLE 1,Setup the Train/Test Split
# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Setup the testing and training data
# MAGIC """
# MAGIC
# MAGIC _training_df, _test_df = _dataset.randomSplit([0.7, 0.3])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Training and Testing

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC import mlflow
# MAGIC
# MAGIC """
# MAGIC Setup MLFlow Experiment ID to allow usage in Job Batches
# MAGIC """
# MAGIC
# MAGIC current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC
# MAGIC mlflow.set_experiment(current_notebook_path+"_experiment")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Training Function

# COMMAND ----------

# DBTITLE 1,Create the Training Run Function
import mlflow
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame
from typing import Tuple

def training_run(p_max_depth : int,
                 training_data : DataFrame,
                 test_data : DataFrame) -> Tuple[int, float]:
  with mlflow.start_run() as run:
    # log some parameters
    mlflow.log_param("Maximum_depth", p_max_depth)
    mlflow.log_metric("Training Data Rows", training_data.count())
    mlflow.log_metric("Test Data Rows", test_data.count())
    _dtClassifier = DecisionTreeRegressor(labelCol="binding_affinity", featuresCol="features")
    _dtClassifier.setMaxDepth(p_max_depth)
    _dtClassifier.setMaxBins(20) # This is how Spark decides if a feature is categorical or continuous
    
    # Train the model
    _model = _dtClassifier.fit(training_data)
    
    # Forecast
    _df_predictions = _model.transform(test_data)
    
    # Evaluate the model
    _evaluator = RegressionEvaluator(labelCol="binding_affinity", 
                                                   predictionCol="prediction")
    _accuracy = _evaluator.evaluate(_df_predictions)
    
    # Log the accuracy
    mlflow.log_metric("Accuracy", _accuracy)
    
    # Log the feature importances
    mlflow.log_param("Feature Importances", _model.featureImportances)
    
    # Log the model
    mlflow.spark.log_model(_model, f"Decision_tree_{p_max_depth}")
    
    return (p_max_depth, _accuracy)
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hyperparameter Search

# COMMAND ----------

# DBTITLE 1,Simple Hyperparameter Search
# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Tune the max depth of the Decision tree
# MAGIC """
# MAGIC
# MAGIC _results = [training_run(i, _training_df, _test_df) for i in range(10)]
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize the Elbow Plot

# COMMAND ----------

# DBTITLE 1,Elbow Plot
# MAGIC %python
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC """
# MAGIC Visualize the effect of tuning 
# MAGIC """
# MAGIC
# MAGIC _tuning_df = sc.parallelize(_results).toDF()\
# MAGIC                .withColumnRenamed("_1", "Max Depth")\
# MAGIC                .withColumnRenamed("_2", "Accuracy")\
# MAGIC                .withColumn("Loss", 1 / col("Accuracy"))\
# MAGIC                .select("Max Depth", "Loss")
# MAGIC display(_tuning_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Visualize the Decision Tree with the optimal depth performance

# COMMAND ----------

# DBTITLE 1,Load the Decision Tree
# MAGIC %python
# MAGIC
# MAGIC import mlflow
# MAGIC
# MAGIC """
# MAGIC Load the current optimal model from MLFlow registry
# MAGIC """
# MAGIC
# MAGIC _model = mlflow.spark.load_model("dbfs:/databricks/mlflow-tracking/8844350/51ebb0bff3fd4ee69136403c4cc2067b/artifacts/Decision_tree_3")
# MAGIC
# MAGIC display(_model.stages[0])
