# Broadcasting additional info to the cluster using sc.broadcast()
from pyspark import SparkConf, SparkContext
import codecs

def loadMovieNames():
    movieNames = {}
    # with open("../data/ml-100k/u.item") as f:
    with codecs.open("../data/ml-100k/u.item", 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1] # {id:name,...}
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)
nameDict = sc.broadcast(loadMovieNames()) # Broadcast a read-only variable to the cluster
lines = sc.textFile("../data/ml-100k/u.data") # userid itemid rating timestamp

movies = lines.map(lambda x: (int(x.split()[1]), 1)) # [(itemid,1), ...]
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda x: (nameDict.value[x[1]], x[0]))
# Each x is (count,movie)
# nameDict.value is {id:name...}, so we are actually calling dict[key] 
# print(nameDict.value)

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)
