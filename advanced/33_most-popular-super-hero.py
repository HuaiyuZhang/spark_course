from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("mostpopularhero").getOrCreate()

schema = StructType([StructField("id", IntegerType(), True),\
                        StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel-names.txt")

lines = spark.read.text("Marvel-graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
                .withColumn("connection", func.size(func.split(func.col("value"), " "))-1)\
                .groupBy("id").agg(func.sum(func.col("connection")).alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

# Exercise: find the heroes with only one connection
singleConnections = connections.filter(func.col("connections") == 1)
singleConnectionsName = singleConnections.join(names, "id")
singleConnectionsName.select("name").show()


