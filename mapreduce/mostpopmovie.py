from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key) # every kv pair has a k of None
        # If we just yield key, sum(values)
        # Then the output of the reducer will be sorted by key (which is movieID)
        # But we want to sort by sum(value) (so as to find the most-rated movie).
        # Same concept: apply two MRs in sequence. 
       
    # This mapper does nothing; it's just here to avoid a bug in some
    # versions of mrjob related to "non-script steps." Normally this
    # wouldn't be needed.
    def mapper_passthrough(self, key, value):
        yield key, value
    
    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
