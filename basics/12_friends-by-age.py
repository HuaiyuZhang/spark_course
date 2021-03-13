from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


lines = sc.textFile("/Projects/SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
# the mapValues only map the value of key-value pair. 
# `x: (x, 1)`, the first is to sum, second is to count
totalByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
averageByAge = totalByAge.mapValues(lambda x: x[0]/x[1])

results = averageByAge.collect()

for result in results:
    print(result)