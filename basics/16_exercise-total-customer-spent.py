from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("customer-spending")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    id = fields[0]
    amount = float(fields[2])
    return (id, amount)

spendRaw = sc.textFile("C:/Projects/SparkCourse/customer-orders.csv")
spend = spendRaw.map(parseLine)
spendByCustomer = spend.reduceByKey(lambda x,y: x+y)
sortedByCustomer = spendByCustomer.map(lambda x: (x[1], x[0])).sortByKey().collect()

print("ID: amount")
for result in sortedByCustomer:
    print("{}: {:.2f}".format(result[1], result[0]))