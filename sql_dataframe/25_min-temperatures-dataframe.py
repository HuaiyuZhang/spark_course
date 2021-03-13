from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemp").getOrCreate()

schema = StructType([\
                    StructField("stationID", StringType(), True), \
                    StructField("date", IntegerType(), True), \
                    StructField("measure_type", StringType(), True), \
                    StructField("temperature", FloatType(), True)])
df = spark.read.schema(schema).csv("1800.csv")
df.printSchema()

df.head()
# min temp for each station
min_temp_by_station = df.groupBy("stationID").min("temperature")
min_temp_by_station.show()

# convert to Fahrenheit and sort
def c_to_f(c):
    return c * 0.1 * 9.0/5.0 + 32.0
# use withColumn, col
min_f = min_temp_by_station.withColumn("temperature", \
                                func.round(c_to_f(func.col("min(temperature)"))))
min_f.select('stationID','temperature').sort('temperature').show()