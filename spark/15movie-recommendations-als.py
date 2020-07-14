import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating

def loadMovieNames():
    movieNames = {}
    with open("../data/ml-100k/u.item", errors = "ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendationsALS")
sc = SparkContext(conf = conf)
sc.setCheckpointDir('checkpoint')

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("../data/ml-100k/u.data")
ratings = data.map(lambda l: l.split()).map(lambda l: 
                    Rating(int(l[0]), int(l[1]), float(l[2]))).cache() 
# i.e. Rating(userid, movieid, rating)

# Build the recommendation model using Alternating Least Squares
print("\nTraining recommendation model...")
rank = 10 # number of features to use
numIterations = 6
model = ALS.train(ratings, rank, numIterations)

userID = int(sys.argv[1]) 
# run using e.g.  spark-submit 15movie-recommendations-als.py 4

print("\nTop 10 recommendations:")
recommendations = model.recommendProducts(userID, 10) # returns a list of Rating objects
for recommendation in recommendations:
    print(nameDict[int(recommendation[1])] +" score " + str(recommendation[2]))



# See what movies that user has watched before.
# These are the features that ALS has predicted upon.

print("\nRatings for user ID " + str(userID) + ":")
userRatings = ratings.filter(lambda l: l[0] == userID) # l[0] is Rating[0] is userid
# In this case, the test set does not have to be "unseen", 
# because we are just recommending movies to an existing user,
# so we just can use a subset of the training rdd
for rating in userRatings.collect(): 
    print(nameDict[int(rating[1])] + ": " + str(rating[2])) 
    # In the ratings rdd, we specified that Rating[1] is movieid, Rating[2] is rating


