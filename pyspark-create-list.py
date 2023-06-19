# Databricks notebook source
# -*- coding: utf-8 -*-
'''
Created on Sat Jan 11 19:38:27 2020

@author: sparkbyexamples.com
'''

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Using List
dept = [("Finance",10), 
        ("Marketing",20), 
        ("Sales",30), 
        ("IT",40) 
      ]

deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


# COMMAND ----------


deptSchema = StructType([       
    StructField('Dept_name', StringType(), True),
    StructField('Dept_ID', StringType(), True)
])

deptDF1 = spark.createDataFrame(data=dept, schema = deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)


# COMMAND ----------


# Using list of Row type
dept2 = [Row("Finance",10), 
        Row("Marketing",20), 
        Row("Sales",30), 
        Row("IT",40) 
      ]

deptDF2 = spark.createDataFrame(data=dept2, schema = deptColumns)
deptDF2.printSchema()
deptDF2.show(truncate=False)

# Convert list to RDD
rdd = spark.sparkContext.parallelize(dept)
rdd.collect()

