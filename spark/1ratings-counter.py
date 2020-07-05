from pyspark import SparkConf, SparkContext
# SparkConf is needed to configure SparkContext (e.g. how many machines)
# SparkContext is needed to create RDDs
import collections # to sort the resutls later

# Set up the spark context
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# Run locally, not on a cluster. Just one node for now.
# Set the job name as ratings histogram
sc = SparkContext(conf = conf)

# load the data file 
lines = sc.textFile("../data/ml-100k/u.data")
# Lines is an RDD.
# Every line of text in the input data file corresponds to one value in the RDD.
# Each value of the RDD is "userid movieid rating timestamp"

# process each value of the RDD
# newRDD = oldRDD.map(lambda x: # define what you want to do for each value x of the old RDD)
ratings = lines.map(lambda x: x.split()[2]) 
# Take the rating value for every line, e.g. output is [3,4,1,5,...]
# Original RDD remains unchanged. ratings is a new RDD

# execute an action on the RDD
result = ratings.countByValue() 
# The output of this is a dictionary of {rating: count, rating: count, ...}
# result1 = ratings.collect()
# print(result1)

# the rest of the code is just python
sortedResults = collections.OrderedDict(sorted(result.items())) # dict.items()
# sorted() sorts by key of the dictionary
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
