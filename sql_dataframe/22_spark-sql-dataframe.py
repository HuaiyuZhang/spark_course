# process the data in dataframe style. 

from pyspark.sql import SparkSession
# Routine
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# the dataset has a header line. Let spark determine the parsing/schema
# if the csv file have a header (column names in the first row) then set header=true
# The schema refered to here are the column types.
# See here for an explaination: https://stackoverflow.com/questions/56927329/spark-option-inferschema-vs-header-true
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("SparkCourse/fakefriends-header.csv")
# type(people)
# <class 'pyspark.sql.dataframe.DataFrame'>

print("Here is our inferred schema:")
people.printSchema()    

# below are pandas-like grammer. Treat the data as a regular dataframe.
print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()