import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("../data/book.txt")
words = input.flatMap(normalizeWords)
# ["word", "word", ...]

wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
# x and y are both 1 because (k,v) is (word,1)
# {"word":count, "word": ...}

wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey() 
# change (k,v) to (v,k) because there is no "sortByValue()" function
results = wordCountsSorted.collect()
# [(biggest v, )]
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
