# Databricks notebook source
# MAGIC %md ---
# MAGIC title: Health Data Science - Activity Tracker Data
# MAGIC authors:
# MAGIC - John Sheaffer
# MAGIC tags:
# MAGIC - etl
# MAGIC - pyspark
# MAGIC - sql
# MAGIC - exploratory-data-analysis
# MAGIC - eda
# MAGIC - exploratory
# MAGIC - visualization
# MAGIC - cleaning
# MAGIC - data-preparation
# MAGIC - tutorial
# MAGIC - beginner
# MAGIC - csv
# MAGIC - hls
# MAGIC - health
# MAGIC - data-science
# MAGIC - iot
# MAGIC - nhanes
# MAGIC - health-data
# MAGIC - statistics
# MAGIC - stats
# MAGIC created_at: 2019-07-22
# MAGIC updated_at: 2019-07-22
# MAGIC tldr: This notebook shows an example of loading, cleaning, and then exploring data from activity trackers worn by individuals participating in health studies.
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Links
# MAGIC - AWS demo.cloud: [https://demo.cloud.databricks.com/#notebook/3574205](https://demo.cloud.databricks.com/#notebook/3574205)

# COMMAND ----------

# MAGIC %md # NHANES Physical Activity Monitoring Study
# MAGIC <img src="https://www.cdc.gov/nchs/nhanes/wcms-inc/VP4_935_NHANES.png" />
# MAGIC
# MAGIC This notebook shows an example of **loading**, **cleaning**, and then **exploring** data from activity trackers worn by individuals participating in health studies.
# MAGIC
# MAGIC From the data's documentation site: https://wwwn.cdc.gov/Nchs/Nhanes/2005-2006/PAXRAW_D.htm
# MAGIC
# MAGIC >The NHANES examined samples 6 years of age and over received physical activity monitors to wear at home for 7 consecutive days. Subjects who used wheelchairs and subjects with other impairments that prevented them from walking or wearing the PAM device were not given a monitor.
# MAGIC
# MAGIC This notebook is derived from the materials provided for the UCSB spring 2018 quarter Health Data Science Module led by Evidation Health.  
# MAGIC https://github.com/evidation-health/EvidationDataScienceModule

# COMMAND ----------

# MAGIC %md # Data Loading

# COMMAND ----------

# MAGIC %md This notebook uses data from 2005-2006 found here: https://wwwn.cdc.gov/Nchs/Nhanes/2005-2006/PAXRAW_D.ZIP
# MAGIC
# MAGIC The data provided by NHANES is stored in the XPORT file format used by the US government for publishing data sets. Converting the data to CSV format is time consuming, so we've already converted it and stored it in S3 for analysis. See the Appendix at the end of the notebook for the steps we took to download and convert the data to CSV.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets-private/HLS/health_data_science_-_activity_tracker_data/

# COMMAND ----------

df_csv = spark.read.csv("dbfs:/databricks-datasets-private/HLS/health_data_science_-_activity_tracker_data/paxraw_d.csv", header="true", inferSchema='true')
print("Observations in CSV file: {}".format(df_csv.count()))

# COMMAND ----------

# MAGIC %md We have over 74 Million data points to work with in 2005-2006

# COMMAND ----------

# MAGIC %md #Data Transformation
# MAGIC
# MAGIC Let's format the data and add a feature to make our data cleansing efforts easier.
# MAGIC
# MAGIC First, let's adjust the schema.

# COMMAND ----------

df_orig = (df_csv
  .withColumn("SEQN", col("SEQN").cast(DoubleType()))
  .withColumn("PAXSTAT", col("PAXSTAT").cast(IntegerType()))
  .withColumn("PAXCAL", col("PAXCAL").cast(IntegerType()))
  .withColumn("PAXDAY", col("PAXDAY").cast(IntegerType()))
  .withColumn("PAXHOUR", col("PAXHOUR").cast(IntegerType()))
  .withColumn("PAXMINUT", col("PAXMINUT").cast(IntegerType()))
  .withColumn("PAXINTEN", col("PAXINTEN").cast(IntegerType()))
  .withColumn("PAXSTEP", col("PAXSTEP").cast(DoubleType()))
  .withColumn("PAXN", col("PAXN").cast(IntegerType()))
)
display(df_orig)

# COMMAND ----------

# MAGIC %md And let's add a new column called `MINUTE_OF_DAY` which will be a combination of the `PAXHOUR` and `PAXMINUTE` columns to give us an incrementing value for the minute of each day.

# COMMAND ----------

df_minute = df_orig.withColumn("MINUTE_OF_DAY", col("PAXHOUR") * 60 + col("PAXMINUT"))
display(df_minute)

# COMMAND ----------

# MAGIC %md Let's take the ID of just one user (i.e. `SEQN`) for one day and plot it to see if it makes sense. This will help us ensure we've loaded the data correctly so far.

# COMMAND ----------

# DBTITLE 1,Take the ID of just one user to look at the data
test_id = 32210
print("Test ID: {}".format(test_id))

# COMMAND ----------

# MAGIC %md Plot the activty for the user by hour.
# MAGIC
# MAGIC Note: We didn't plot by `MINUTE_OF_DAY` since there are 1440 minutes in a day, and the `display()` function limits to the first 1000 records.

# COMMAND ----------

df_single_user = df_minute.where("SEQN = {} AND PAXDAY=1".format(test_id))
display(df_single_user)

# COMMAND ----------

# MAGIC %md This looks very reasonable as most of the activity is between 8:00 and 19:00 (aka 8am to 7pm)

# COMMAND ----------

# MAGIC %md #Data Cleansing
# MAGIC
# MAGIC Now let's move on to data cleansing. This data comes from activity trackers, which is a for of IoT device. IoT devices are notorious for sending inconsistent data.
# MAGIC
# MAGIC Since we have a decent amount of data, we will take the approach of removing the observations from users (aka `SEQN`) that have missing or inconsistent data.

# COMMAND ----------

# MAGIC %md ###Uncalibrated Devices
# MAGIC
# MAGIC First, let us filter out uncalibrated devices
# MAGIC
# MAGIC In the data's doc page, it states the `PAXCAL` feature indicates if the device was calibrated or not. The possible values are:
# MAGIC
# MAGIC 1 = Calibrated  
# MAGIC 2 = Not Calibrated
# MAGIC
# MAGIC Therefore we will simply filter out the uncalibrated device observations.

# COMMAND ----------

print("Total observations, including calibrated and uncalibrated devices: {}".format(df_minute.count()))

# COMMAND ----------

df_calibrated = df_minute.where("PAXCAL == 1")     # Keep only the observations where the device was calibrated
df_calibrated = df_calibrated.drop(col("PAXCAL"))  # We can now drop the PAXCAL column

print("Number of observations after removing uncalibrated device entries: {}".format(df_calibrated.count()))

# COMMAND ----------

# MAGIC %md ### Outliers
# MAGIC
# MAGIC Next, let's look at the `PAXSTEP` feature more closely.

# COMMAND ----------

display(df_calibrated.describe('PAXSTEP'))

# COMMAND ----------

# MAGIC %md Looking at the mean (16.23) and the max (32767) have some outliers in the PAXSTEP feature.
# MAGIC
# MAGIC And thinking about it, being able to perform 32,767 steps in any given minute is not very possible! It's likely an error in the data reporting or collection.
# MAGIC
# MAGIC We can make a reasonalble assumption that a person could do no more than about 300 steps per minute. This is our judgement only for now. We could do additional analysis on this column to confirm or refine this estimate. But for now we'll just set 300 steps/min as the upper threshold that we consider to be valid observations.

# COMMAND ----------

df_no_outliers = df_calibrated.where("PAXSTEP <= 300")
print("Number of observations with outliers removed: {}".format(df_no_outliers.count()))

# COMMAND ----------

# MAGIC %md ### Inadequate Wear Time
# MAGIC
# MAGIC Not all of the devices were worn consistently. For our purposes, we want to look only at devices that were worn most of the day.
# MAGIC
# MAGIC We will make an inclusion rule that devices we care about have been worn at least 10 hours a day. We don't expect people to wear the devices when they sleep, shower, etc. So 10 hours should be a reasonable assumption.
# MAGIC
# MAGIC Ten hours equals 600 minutes. So we're looking for devices that have at least 600 minutes per day of data.

# COMMAND ----------

# Before filtering by 10 hour users
num_before_10hr = df_no_outliers.groupBy("SEQN").count().count()

print("All devices so far including ones not worn >= 10 hours: {}".format(num_before_10hr))

# COMMAND ----------

df_user_with_10_hours = df_no_outliers.where("PAXINTEN > 0").groupBy("SEQN").count().where("count >= 600")
num_after_10hr = df_user_with_10_hours.count()

print("Devices with 10+ hours of usage: {}".format(num_after_10hr))

# COMMAND ----------

# MAGIC %md Remove the devices without 10 hours of usage using a join with the list of 10+ hour devices created above

# COMMAND ----------

df_10_hour_data = df_no_outliers.join(df_user_with_10_hours, "SEQN")
print("Number of observations from devices with 10+ hours of usage each day: {}".format(df_10_hour_data.count()))

# COMMAND ----------

# MAGIC %md Good, we still have over **67 Million data points** and over **6700 devices** in our cleansed data set.

# COMMAND ----------

# MAGIC %md # Explore the Data
# MAGIC
# MAGIC Now that we have a clean data set, let's do some basic data exploration. Once we are comfortable with the data set, we can publish it to our downstream users for more detailed exploration, and machine learning projects.

# COMMAND ----------

# DBTITLE 1,Distribution of Steps-per-day
df_user_steps_per_day = df_10_hour_data.groupBy("SEQN", "PAXDAY").agg(F.sum("PAXSTEP").alias("steps")).groupBy("SEQN").agg(F.avg("steps").alias("steps_per_day"))
display(df_user_steps_per_day)

# COMMAND ----------

# MAGIC %md This distribution looks like what we would expect, which builds our confidence in the quality of this data set.
# MAGIC
# MAGIC We see that the mode is around 7,000 steps per day with a small group of users making up to 40,000 steps per day. This seems completely reasonable.
# MAGIC
# MAGIC We can display this distribution using a box plot also.

# COMMAND ----------

display(df_user_steps_per_day)

# COMMAND ----------

# MAGIC %md # Appendix

# COMMAND ----------

# MAGIC %md ### Downloading and converting the source data
# MAGIC If you want to download and convert the data directly from the NHANES site, you can follow the steps below.  These steps will run on the driver. Once the `paxraw_d.csv` file is created, you can move it to an S3 location. 
# MAGIC
# MAGIC **Note:** To run the commands, delete the `#` on each line.

# COMMAND ----------

# MAGIC %sh
# MAGIC #mkdir -p /local_disk0/data
# MAGIC #cd /local_disk0/data
# MAGIC #wget -qc https://wwwn.cdc.gov/Nchs/Nhanes/2005-2006/PAXRAW_D.ZIP 
# MAGIC #unzip /dbfs/mnt/j4-databricks/datasets/nhanes/PAXRAW_D.ZIP
# MAGIC #pip install xport
# MAGIC #python -m xport paxraw_d.xpt > paxraw_d.csv
