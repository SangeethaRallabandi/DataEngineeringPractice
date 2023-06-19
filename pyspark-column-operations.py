# Databricks notebook source
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""
from pyspark.sql import SparkSession,Row
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
"""
builder - A class attribute having a Builder to construct SparkSession instances.
appName - app name 
"""

data=[("James",23),("Ann",40)]
df=spark.createDataFrame(data).toDF("name.fname","age")
"""
toDF - Returns a new DataFrame that with new specified column names
printSchema - 	Prints out the schema in the tree format.
show([n, truncate, vertical]) - Prints the first n rows to the console.
"""

df.printSchema()
df.show()


# COMMAND ----------


from pyspark.sql.functions import col
df.select(col("`name.fname`")).show()
df.select(df["`name.fname`"]).show()
df.withColumn("new_col",col("`name.fname`").substr(1,2)).show()
df.filter(col("`name.fname`").startswith("A")).show()
new_cols=(column.replace('.', '_') for column in df.columns)
df2 = df.toDF(*new_cols)
df2.show()

""" 
select(*cols) - Projects a set of expressions and returns a new DataFrame.
withColumn(colName, col) - Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
filter(condition) - Filters rows using the given condition.
"""


# COMMAND ----------



# Using DataFrame object
df.select(df.age).show()
df.select(df["age"]).show()
#Accessing column name with dot (with backticks)
df.select(df["`name.fname`"]).show()


# COMMAND ----------


#Using SQL col() function
from pyspark.sql.functions import col
df.select(col("age")).show()
#Accessing column name with dot (with backticks)
df.select(col("`name.fname`")).show()

#Access struct column
data=[Row(name="James",prop=Row(hair="black",eye="blue")),
      Row(name="Ann",prop=Row(hair="grey",eye="black"))]
df=spark.createDataFrame(data)
df.printSchema()

df.select(df.prop.hair).show()
df.select(df["prop.hair"]).show()
df.select(col("prop.hair")).show()
df.select(col("prop.*")).show()


# COMMAND ----------


# Column operators
data=[(100,2,1),(200,3,4),(300,4,4)]
df=spark.createDataFrame(data).toDF("col1","col2","col3")
df.printSchema()
df.show()
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show() 
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
df.select(df.col1 % df.col2).show()

df.select(df.col2 > df.col3).show()
df.select(df.col2 < df.col3).show()
df.select(df.col2 == df.col3).show()

# COMMAND ----------

