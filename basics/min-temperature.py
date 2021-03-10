from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("min-temperature")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    # convert to Fahrenheit
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("C:/Projects/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# only keep the station and temperature
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# reduce to get the minimum for the station.
minTemps = stationTemps.reduceByKey(lambda x,y: min(x,y))
results = minTemps.collect()

for result in results:
    print("Station: " + result[0] + "\t{:.2f}F".format(result[1]))