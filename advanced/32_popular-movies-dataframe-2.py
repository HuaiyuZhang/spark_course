# broadcast a dictionary to executors. 
# the broadcast object usually should be in small size, like a lookup table.
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

# create the dictionary mapping movie ID to movie names.
def loadMovieNames():
    movieNames = {}
    with codecs.open("ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopMovies").getOrCreate()

# create the broadcast object
nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([\
    StructField("userID", IntegerType(), True),\
    StructField("movieID", IntegerType(), True),\
    StructField("rating", IntegerType(), True),\
    StructField("timestamp", LongType(), True)])

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("ml-100k/u.data")
movieCounts =  moviesDF.groupby("movieID").count()

def lookupName(movieID):
    return nameDict.value[movieID]
# register the function so it can be recognized by spark sql
lookupNameUDF = func.udf(lookupName)

# add a new column of movie names
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))


sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# the process takes some time.
sortedMoviesWithNames.show(10, False)


spark.stop()