from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):

    def configure_args(self):
        ''' From MRJob:
        def configure_args(self):
            # Define arguments for this script. Called from :py:meth:`__init__()`.
            # Re-define to define custom command-line arguments or pass through existing ones::

            def configure_args(self):
                super(MRYourJob, self).configure_args()
                self.add_passthru_arg(...)
                self.add_file_arg(...)
                self.pass_arg_through(...)
                ...
        '''

        super(MostPopularMovie, self).configure_args()
        self.add_file_arg('--items', help='Path to u.item') # command line argument

    def steps(self): 
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper = self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_init(self):
        '''
        def reducer_init(self):
            # Re-define this to define an action to run before the reducer
            # processes any input.
            # One use for this function is to initialize reducer-specific helper structures.
        '''
        self.movieNames = {}

        with open("u.ITEM", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1] # add movieID: movieName to dict

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), self.movieNames[key]) 
        # None, [(number of ratings for movie 1, name of movie 1), 
        #        (number of ratings for movie 2, name of movie 2), ...]

    #This mapper does nothing; it's just here to avoid a bug in some
    #versions of mrjob related to "non-script steps." Normally this
    #wouldn't be needed.
    def mapper_passthrough(self, key, value):
        yield key, value 

    def reducer_find_max(self, key, values): # Each value from the values generator
        yield max(values) # is of the format (number of ratings for movie x, name of movie x)

if __name__ == '__main__':
    MostPopularMovie.run()
