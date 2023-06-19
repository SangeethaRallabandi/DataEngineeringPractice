# Databricks notebook source
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


# COMMAND ----------


dataCollect = deptDF.collect()
print(dataCollect)


# COMMAND ----------

dataCollect2 = deptDF.select("dept_name").collect()
print(dataCollect2)




# COMMAND ----------

for row in dataCollect:
    print(row['dept_name'] + "," +str(row['dept_id']))


# COMMAND ----------

