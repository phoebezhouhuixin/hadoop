from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

names = sc.textFile("../data/marvelnames.txt") # Each line is id "name"
def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8")) # (id, name)
namesRdd = names.map(lambda x: parseNames(x))

# another rdd
lines = sc.textFile("../data/marvelgraph.txt") # Each line is targetid friendid friendid friendid ...
def countFriends(line): # 
    elements = line.split()
    return (int(elements[0]), len(elements) - 1) # (targetid, numFriends)
pairings = lines.map(countFriends)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda x : (x[1], x[0])) # so that we can sort people by numFriends

# Use the output of the second rdd to lookup in the first rdd
mostPopular = flipped.max() # maximum key (i.e. numFriends) --> returns (the max numFriends, the id)
mostPopularName = namesRdd.lookup(mostPopular[1])[0] # lookup a key-value rdd by key

print(str(mostPopularName, "utf-8") + " is the most popular superhero, with " + str(mostPopular[0]) + " co-appearances.")
