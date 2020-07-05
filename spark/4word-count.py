from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("../data/book.txt")
# ["The quick brown", "fox", ...]
words = input.flatMap(lambda x: x.split()) # RDD transformation
print(words.collect())
# ["The", "quick", ...]
wordCounts = words.countByValue() # RDD action
# {"The":4, "quick": ...}

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
