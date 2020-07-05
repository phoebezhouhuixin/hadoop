# RDDs can hold key-value pairs 
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    # "id,name,age,numfriends"
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends) # (k,v)

lines = sc.textFile("../data/friendsbyage.csv")
rdd = lines.map(parseLine)
# e.g. [(21, numfriends), (21, numfriends), (30, numfriends), ...]

# If using kv data, use mapValues() and flatMapValues() if your transformation does not affect the keys, because it is more efficient.
# Note that the mapValues() still returns kv pairs (the key remains unchanged)
intermediate = rdd.mapValues(lambda numfriends: (numfriends, 1)) #print(intermediate.collect())
# Map every value, i.e. numfriends, in the RDD to (numfriends,1).
# intermediate is [(21, (numfriends,1)), ...] 

totalsByAge = intermediate.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) 
# reduceByKey() groups by key, then
# performs an operation on all the values that are found for that key.
# x and y represent two different VALUES of the same key, 
# e.g. x is the (5,1) of the kv pair (21,(5,1)),
#  and y is (3,1) of the kv pair (21,(3,1)) . 
# totalsByAge is [ (21,(8,2)) , ...]

# What else can we do with kv pairs that are output by mapValues()?
# Instead of reduceByKey(), there is also just groupByKey() and sortByKey().
# We can use keys() and values() to create a new RDD out of just the keys or just the values. We can also do SQL-style joins on two key-value RDDs.

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
