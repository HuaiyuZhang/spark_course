from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopMovies").getOrCreate()

schema = StructType([\
    StructField("userID", IntegerType(), True),\
    StructField("movieID", IntegerType(), True),\
    StructField("rating", IntegerType(), True),\
    StructField("timestamp", LongType(), True)])

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("ml-100k/u.data")

# order by count in descending order
topMovieIDs = moviesDF.groupby("movieID").count().orderBy(func.desc("count"))
topMovieIDs.show(10)
spark.stop()