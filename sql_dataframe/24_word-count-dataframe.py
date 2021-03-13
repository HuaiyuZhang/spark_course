from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("book.txt")
# inputDF.head()
# Row(value='Self-Employment: Building an Internet Business of One')
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

sorted = words.select(func.lower(words.word).alias("word")).groupBy("word").count().sort("count")

# sorted.show() does not show all columns

sorted.show(sorted.count())

# only show the high frequency words
wc = words.select(func.lower(words.word).alias("word")).groupBy("word").count()
wc.filter(wc['count']> 100).sort("count").show()