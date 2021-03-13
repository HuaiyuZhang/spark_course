from pyspark.sql import SparkSession
from pyspark.sql import functions as func
# Routine
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# the dataset has a header line. Let spark determine the parsing/schema
# if the csv file have a header (column names in the first row) then set header=true
# The schema refered to here are the column types.
# See here for an explaination: https://stackoverflow.com/questions/56927329/spark-option-inferschema-vs-header-true
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")
# compute the average # of friends for each age and sort
people.select("age", "friends").groupBy("age").avg("friends").sort("age").show()

people.select("age", "friends").groupBy("age").agg(func.round(func.avg("friends"),2).alias("avg_friends")).sort("age").show()

spark.stop()