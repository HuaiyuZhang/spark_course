import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("word-count")
sc = SparkContext(conf = conf)

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("/Projects/SparkCourse/book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x,y:x+y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode("ascii",'ignore')
    if word:
        print(word.decode() + ':\t\t' + count)