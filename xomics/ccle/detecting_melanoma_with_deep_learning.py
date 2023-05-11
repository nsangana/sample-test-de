# Databricks notebook source
# MAGIC %md ---
# MAGIC title: Detecting Melanoma with Deep Learning
# MAGIC authors:
# MAGIC - Richard Garris
# MAGIC - William Brandler
# MAGIC tags:
# MAGIC - HLS
# MAGIC - machine-learning
# MAGIC - deep-learning
# MAGIC - image-classification 
# MAGIC - python
# MAGIC - transfer-learning
# MAGIC - tensorflow
# MAGIC - tensorboard
# MAGIC - melanoma
# MAGIC - cancer
# MAGIC created_at: 2019-05-06
# MAGIC updated_at: 2020-10-01
# MAGIC tldr: Applying deep learning for image classification in order to learn how to detect melanoma
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Links
# MAGIC - AWS demo.cloud: [https://demo.cloud.databricks.com/#notebook/2980883](https://demo.cloud.databricks.com/#notebook/2980883)

# COMMAND ----------

# MAGIC %pip install mxnet

# COMMAND ----------

# MAGIC %md
# MAGIC # Detecting Melanoma with Transfer Learning
# MAGIC
# MAGIC <img src="https://cdn-images-1.medium.com/max/1800/0*mH17FEz4LHpaJrPb.png" alt="drawing" width="800"/>
# MAGIC
# MAGIC Melanoma is one of the mostly deadliest forms of skin cancer with over 75,000 cases in the US each year.
# MAGIC
# MAGIC Melanoma is also hard to detect as not all skin moles and lesions are cancerous.
# MAGIC
# MAGIC Here we will use transfer learning to train an image classifier for melanoma. 
# MAGIC It uses feature vectors computed by Inception V3 trained on ImageNet
# MAGIC
# MAGIC This demo is based on the [ISIC 2017](https://challenge.kitware.com/#challenge/583f126bcad3a51cc66c8d9a): Skin Lesion Analysis Towards Melanoma Detection Contest Sponsored by the *International Skin Imaging Collaboration*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Labels from a CSV File

# COMMAND ----------

labels = spark.read.\
  option("header", True).option("inferSchema", True).\
  csv("/mnt/databricks-datasets-private/HLS/melanoma/training/meta/labels/labels.csv")
display(labels)

# COMMAND ----------

display(labels.groupBy("melanoma").count())

# COMMAND ----------

melanoma = labels.where("melanoma = 1")
benign = labels.where("melanoma = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cache Data to the SSD

# COMMAND ----------

import os
import shutil

def cacheFilesAndReturn(images, subdir):
  file_dir = '/tmp/training/'+subdir+'/'
  try:
    os.makedirs(str(file_dir))
  except:
    pass
  for image_id in images:
    shutil.copyfile("/dbfs/mnt/databricks-datasets-private/HLS/melanoma/training/data/%s.jpg" % image_id, str(file_dir)+"%s.jpg" % image_id)
    
cacheFilesAndReturn(melanoma.select("image_id").rdd.map(lambda x: x[0]).collect(), "melanoma")
cacheFilesAndReturn(benign.select("image_id").rdd.map(lambda x: x[0]).collect(), "benign")

# COMMAND ----------

# MAGIC %fs ls dbfs:/home/alex.barreto@databricks.com/hls

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the Dataset

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.image as img
import mxnet
import tensorflow as tf
import os

# COMMAND ----------

melanomaImg = "/tmp/training/melanoma/" + os.listdir("/tmp/training/melanoma/")[0]
benignImg = "/tmp/training/benign/" + os.listdir("/tmp/training/benign/")[0]
print(melanomaImg)
print(benignImg)

# COMMAND ----------

plt.imshow(mxnet.image.imdecode(open(melanomaImg, 'rb').read()).asnumpy())
plt.title("Melanoma")
display(plt.show())

# COMMAND ----------

plt.imshow(mxnet.image.imdecode(open(benignImg, 'rb').read()).asnumpy())
plt.title("Benign")
display(plt.show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train the Model using a Convolution Neural Network

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Download library for transfer learning
# MAGIC These steps create melanoma.pb and associated training logs. The result is already available though.
# MAGIC ```
# MAGIC # wget https://raw.githubusercontent.com/tensorflow/tensorflow/c565660e008cf666c582668cb0d0937ca86e71fb/tensorflow/examples/image_retraining/retrain.py
# MAGIC wget https://raw.githubusercontent.com/tensorflow/hub/master/examples/image_retraining/retrain.py
# MAGIC /databricks/python/bin/python3 -u ./retrain.py --image_dir "/tmp/training"  --output_graph "/tmp/melanoma.pb"
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC dbutils.fs.cp("dbfs:/mnt/databricks-datasets-private/HLS/melanoma/melanoma.pb", "file:/tmp/melanoma.pb", True)
# MAGIC dbutils.fs.cp('dbfs:/mnt/databricks-datasets-private/HLS/melanoma/train', 'file:/tmp/retrain_logs', True)
# MAGIC ```

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/alexxx-db/hub/master/examples/image_retraining/retrain.py

# COMMAND ----------

# MAGIC %sh 
# MAGIC cat ./retrain.py | grep compat

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install tensorflow-hub
# MAGIC /databricks/python/bin/python3 -u ./retrain.py --image_dir "/tmp/training"  --output_graph "/tmp/melanoma.pb"

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls dbfs:/mnt/databricks-datasets-private/HLS/melanoma

# COMMAND ----------

# MAGIC %fs cp -r dbfs:/mnt/databricks-datasets-private/HLS/melanoma/ dbfs:/home/alex.barreto@databricks.com/hls/melanoma/

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget https://raw.githubusercontent.com/alexxx-db/hub/master/examples/image_retraining/retrain.py
# MAGIC /databricks/python/bin/python3 -u ./retrain.py --image_dir "/tmp/training"  --output_graph "/tmp/melanoma.pb"

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/databricks-datasets-private/HLS/melanoma/melanoma.pb", "file:/tmp/melanoma.pb", True) 
dbutils.fs.cp('dbfs:/mnt/databricks-datasets-private/HLS/melanoma/train', 'file:/tmp/retrain_logs', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Images using a Convolution Neural Network

# COMMAND ----------

with tf.compat.v1.gfile.FastGFile("/tmp/melanoma.pb", 'rb') as f:
    graph_def = tf.compat.v1.GraphDef()
    graph_def.ParseFromString(f.read())
    _ = tf.import_graph_def(graph_def, name='')

# COMMAND ----------

def displayPrediction(img_path, label):
  image_data = tf.compat.v1.gfile.FastGFile(img_path, 'rb').read()
  with tf.compat.v1.Session() as sess:
    # Feed the image_data as input to the graph and get first prediction
    softmax_tensor = sess.graph.get_tensor_by_name('final_result:0')
    
    predictions = sess.run(softmax_tensor, \
             {'DecodeJpeg/contents:0': image_data})
    
    # Sort to show labels of first prediction in order of confidence
    #top_k = predictions[0].argsort()[-len(predictions[0]):][::-1]
    plt.imshow(mxnet.image.imdecode(open(img_path, 'rb').read()).asnumpy())
    plt.title(label)
    plt.figtext(0,0,'Model Prediction: Not Cancer: %.5f, Cancer: %.5f' % (predictions[0][1], predictions[0][0]))
    display(plt.show())
    plt.close()

# COMMAND ----------

displayPrediction(melanomaImg, "Melanoma")

# COMMAND ----------

displayPrediction(benignImg, "Benign")

# COMMAND ----------

# MAGIC %load_ext tensorboard

# COMMAND ----------

# MAGIC %fs ls dbfs:/home/alex.barreto@databricks.com/HLS/tmp/retrain_logs

# COMMAND ----------

# %tensorboard --logdir file:/tmp/retrain_logs
%tensorboard --logdir dbfs:/home/alex.barreto@databricks.com/HLS/tmp/retrain_logs

# COMMAND ----------

# MAGIC %tensorboard stop
