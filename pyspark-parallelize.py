# Databricks notebook source
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd=spark.sparkContext.parallelize([1,2,3,4,5])

rddCollect = rdd.collect()

# COMMAND ----------


print("Number of Partitions: "+str(rdd.getNumPartitions()))
print("Action: First element: "+str(rdd.first()))
print(rddCollect)



# COMMAND ----------


emptyRDD = spark.sparkContext.emptyRDD()
emptyRDD2 = spark.sparkContext.parallelize([])

print(""+str(emptyRDD2.isEmpty()))
print("Number of Partitions: "+str(emptyRDD.getNumPartitions()))


# COMMAND ----------

