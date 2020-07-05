# To run locally:
# !python MovieSimilarities.py --items=ml-100k/u.item ml-100k/u.data > sims.txt

# To run on a single EMR node:
# !python MovieSimilarities.py -r emr --items=ml-100k/u.item ml-100k/u.data # takes even longer than running on our own computer
# To run on 4 EMR nodes:
#!python MovieSimilarities.py -r emr --num-core-instances=4 --items=ml-100k/u.item ml-100k/u.data

# Troubleshooting EMR jobs (subsitute your job ID):
# !python -m mrjob.tools.emr.fetch_logs --find-failure j-1NXMMBNEQHAFT
# paste your job id j-XXXXXX there ^


from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from itertools import combinations


class MovieSimilarities(MRJob):

    def configure_args(self):
        super(MovieSimilarities, self).configure_args()
        self.add_file_arg('--items', help='Path to u.item')

    def load_movie_names(self):
        # Load database of movie names.
        self.movieNames = {}

        with open("u.item", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                # {id: movieName, id: movieName, ...}
                self.movieNames[int(fields[0])] = fields[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                   reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                   reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                   mapper_init=self.load_movie_names,
                   reducer=self.reducer_output_similarities)]


    # FIRST MR
    
    def mapper_parse_input(self, key, line):
        # Outputs userID: (movieID, rating)
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield userID, (movieID, float(rating))

    def reducer_ratings_by_user(self, user_id, itemRatings):
        # Group (movieID, rating) pairs by userID (i.e. every movie that the user saw)
        ratings = []
        for movieID, rating in itemRatings:
            ratings.append((movieID, rating))
        yield user_id, ratings
        # userID: [(movieID, rating), (movieID,rating),...],  userID: [(movieID, rating), (movieID,rating),...], ...

        # why not just yield user_id, itemRatings?
        # since the reducer automatically groups by key (userID) for us? OR DOES IT

    # SECOND MR

    def mapper_create_item_pairs(self, user_id, itemRatings):
        # "combinations" finds every possible pair of movies
        # from the list of movies this user viewed.
        for itemRating1, itemRating2 in combinations(itemRatings, 2):
            movieID1 = itemRating1[0]
            rating1 = itemRating1[1]
            movieID2 = itemRating2[0]
            rating2 = itemRating2[1]

            # Produce both orders so that similarities are bi-directional
            yield (movieID1, movieID2), (rating1, rating2)
            yield (movieID2, movieID1), (rating2, rating1)
            # i.e. For EACH user id, yield
            # (movie1userX, movie2userX): (rating1userX, rating2userX),
            # (movie1userX, movie3userX): (rating1userX, rating3userX),
            # (movie2userX, movie3): (rating2, rating3), ...
            # and we have many users, each one of whom gives their own version of this

            # Hence the output for all users is grouped by the mapper as:
            # (movie1, movie2): [(rating1user1, rating2user1), (rating1user2, rating2user2),...]

    def reducer_compute_similarity(self, moviePair, ratingPairs):
        ''' The input to the reducer (coming from mapper_create_item_pairs()) is of the format:
            (movie1, movie2): [(rating1user1, rating2user1), (rating1user2, rating2user2),...]
            And the reducer computes the similarity score using the RHS of that^ , i.e.
            [(rating1user1, rating2user1), (rating1user2, rating2user2),...]

            So the output of the reducer is
            (movie1, movie2): (similarity score, number of users who rated that pair),
            (movie1, movie3): ...
        '''
       
        score, numPairs = self.cosine_similarity(ratingPairs) # see below for defintion of cosine_similarity()
        # Enforce a minimum score and minimum number of co-ratings to ensure quality
        if (numPairs > 10 and score > 0.95):
            yield moviePair, (score, numPairs)

    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two rating vectors.
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            # ratingX and ratingY are just scalars (ratings of different movies by the same user)
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy  # x dot y
        denominator = sqrt(sum_xx) * sqrt(sum_yy)  # |x||y|
        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))
        return (score, numPairs)



    # THIRD MR

    def mapper_sort_similarities(self, moviePair, scores):
        '''The input is of the format:
        (movie1, movie2): (similarityscore12, number of users who rated that pair),
        (movie1, movie3): ...'''

        # Split up the (movie1, movie2) pair into output key and output value
        score, n = scores
        movie1, movie2 = moviePair
        yield (self.movieNames[int(movie1)], score), (self.movieNames[int(movie2)], n)

    def reducer_output_similarities(self, movieScore, similarN):
        # The input is of the format: 
        # (movie1name, score12): (movie2name, number of co-ratings),
        # (movie1name, score13): (movie3name, number of co-ratings), ...
        # The output is of the format:
        # movie1name: (mostsimilarmoviename, score, number of co-ratings),
        # movie1name: (2ndmostsimilarmoviename, score, number of co-ratings), ...

        movie1, score = movieScore
        for movie2, n in similarN: # reducer(self, key, values) --> "values" is always a list even though it might be only one element
            yield movie1, (movie2, score, n) 
            # how does this make sure that the results are sorted by score (i.e. sorted by similarity to movie1?)
            # oh because the mapper_sort_similarities has (moviename, score) as the output key,
            # and mappers always group and sort by output key automatically before passing data to the reducer

if __name__ == '__main__':
    MovieSimilarities.run()

'''
Learning activity:
- discard bad ratings, only recommend good movies
- try different similarity metrics (pearson correlation, jaccard similarity, conditional probability)
- invent a new similarity metric that takes the number of co-ratings into account
- use genre infromation in u.items to boost the similarity score of movies in the same genre
'''