# Databricks notebook source
# -*- coding: utf-8 -*-
'''
Created on Sat Jan 11 19:38:27 2020

@author: sparkbyexamples.com
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])
df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
df.printSchema()


# COMMAND ----------


df1 = spark.sparkContext.parallelize([]).toDF(schema)
df1.printSchema()


# COMMAND ----------


df2 = spark.createDataFrame([], schema)
df2.printSchema()


# COMMAND ----------


df3 = spark.emptyDataFrame()
df3.printSchema()

# COMMAND ----------

