from mrjob.job import MRJob
# find the number of ratings, by number of stars given 
class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t') 
        # each item in the line is separated by the tab character
        yield rating, 1 # key, value

    def reducer(self, rating, occurences):
        yield rating, sum(occurences)
'''
rating is the :param key: A key which was yielded by the mapper
occurences is the :param value: A generator which yields all values yielded by the
                mapper which correspond to ``key``.
'''
if __name__ == '__main__':
    MRRatingCounter.run() # from class MRJob