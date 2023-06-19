# Databricks notebook source
# -*- coding: utf-8 -*-
'''
Created on Sat Jan 11 19:38:27 2020

@author: sparkbyexamples.com
'''

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()
dfFromRDD1.show()
type(dfFromRDD1)

# COMMAND ----------


dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.show()
dfFromRDD1.printSchema()
type(dfFromRDD1)

# COMMAND ----------



dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.printSchema()
dfFromRDD2.show()
type(dfFromRDD2)


# COMMAND ----------


dfFromData2 = spark.createDataFrame(data).toDF(*columns)
dfFromData2.printSchema()
type(dfFromData2)


# COMMAND ----------

    

rowData = map(lambda x: Row(*x), data) 
dfFromData3 = spark.createDataFrame(rowData,columns)
dfFromData3.printSchema()
dfFromData3.show()

# COMMAND ----------

