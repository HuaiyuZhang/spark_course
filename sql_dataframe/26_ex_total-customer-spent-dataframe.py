from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("customer").getOrCreate()

schema = StructType([ \
                     StructField("customer_id", StringType(), True), \
                     StructField("product_id", StringType(), True), \
                     StructField("spent", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("customer-orders.csv")
df.printSchema()

total = df.groupBy('customer_id').agg(func.round(func.sum('spent'),2).alias("total")).sort('total')
total.show(total.count())