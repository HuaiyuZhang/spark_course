# in this section, there are four examples for importing data

# 1. Read in as RDD then convert to dataframe
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3])
# read as RDD first
lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# 2. spark.read.csv using options
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("SparkCourse/fakefriends-header.csv")

# 3. spark.read.text
spark.read.text("book.txt")

# 4. spark.read.schema().csv(). need to define schema explicitly.
df = spark.read.schema(schema).csv("1800.csv")