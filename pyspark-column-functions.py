# Databricks notebook source
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')] 
columns=["fname","lname","id","gender"]
df=spark.createDataFrame(data,columns)
df.show()
df.printSchema()


# COMMAND ----------


#alias
from pyspark.sql.functions import expr
df.select(df.fname.alias("first_name"), \
          df.lname.alias("last_name"), \
          expr(" fname ||','|| lname").alias("fullName") \
   ).show()


# COMMAND ----------


#asc, desc
df.sort(df.fname.asc()).show()
df.sort(df.fname.desc()).show()

#cast
df.select(df.fname,df.id.cast("int")).printSchema()

#between
df.filter(df.id.between(100,300)).show()

#contains
df.filter(df.fname.contains("Cruise")).show()

#startswith, endswith()
df.filter(df.fname.startswith("T")).show()
df.filter(df.fname.endswith("Cruise")).show()


# COMMAND ----------


#eqNullSafe

#isNull & isNotNull
df.filter(df.lname.isNull()).show()
df.filter(df.lname.isNotNull()).show()


# COMMAND ----------

#like , rlike
df.select(df.fname,df.lname,df.id).filter(df.fname.like("%om%")).show()

# COMMAND ----------


#over

#substr
df.select(df.fname.substr(1,2).alias("substr")).show()


# COMMAND ----------


#when & otherwise
from pyspark.sql.functions import when
df.select(df.fname,df.lname,when(df.gender=="M","Male") \
              .when(df.gender=="F","Female") \
              .when(df.gender.isNull(),"Empty") \
              .when(df.gender=="","empty") \
              .otherwise(df.gender).alias("new_gender") \
    ).show()


# COMMAND ----------


#isin
li=["100","200"]
df.select(df.fname,df.lname,df.id) \
  .filter(df.id.isin(li)) \
  .show()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,ArrayType,MapType
data=[(("James","Bond"),["Java","C#"],{'hair':'black','eye':'brown'}),
      (("Ann","Varsa"),[".NET","Python"],{'hair':'brown','eye':'black'}),
      (("Tom Cruise",""),["Python","Scala"],{'hair':'red','eye':'grey'}),
      (("Tom Brand",None),["Perl","Ruby"],{'hair':'black','eye':'blue'})]

schema = StructType([
        StructField('name', StructType([
            StructField('fname', StringType(), True),
            StructField('lname', StringType(), True)])),
        StructField('languages', ArrayType(StringType()),True),
        StructField('properties', MapType(StringType(),StringType()),True)
     ])
df=spark.createDataFrame(data,schema)
df.printSchema()

# COMMAND ----------

#getItem()
df.select(df.languages.getItem(1).alias("Selectedlanguages")).show()
#getItem() For Map Data 
df.select(df.properties.getItem("hair")).show() 
df.select(df.name.getItem("fname")).show() 

#getField from Struct or Map
df.select(df.properties.getField("hair")).show()

df.select(df.name.getField("fname")).show()

# COMMAND ----------



#dropFields
from pyspark.sql.functions import col
df.withColumn("name1",col("name").dropFields("fname")).show()

# withField
from pyspark.sql.functions import lit
df.withColumn("name",df.name.withField("fname",lit("AA"))).show()


# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.functions import lit
df = spark.createDataFrame([Row(a=Row(b=1, c=2))])
df.show()
df.printSchema()

# COMMAND ----------


df.withColumn('a', df['a'].withField('b', lit(3))).select('a.b', 'a.c').show()

# COMMAND ----------


        
from pyspark.sql import Row
from pyspark.sql.functions import col, lit
df = spark.createDataFrame([
Row(a=Row(b=1, c=2, d=3, e=Row(f=4, g=5, h=6)))])
df.show()
df.printSchema()
df.withColumn('a', df['a'].dropFields('e')).show()


