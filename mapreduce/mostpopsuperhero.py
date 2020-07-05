from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularSuperHero(MRJob):

    def configure_args(self):
        super(MostPopularSuperHero, self).configure_args()
        self.add_file_arg('--names', help='Path to Marvel-names.txt')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_friends_per_line,
                   reducer=self.reducer_combine_friends),
            MRStep(mapper=self.mapper_prep_for_sort,
                   mapper_init=self.load_name_dictionary,
                   reducer = self.reducer_find_max_friends)
        ]
        # mapper_init() happens before mapper()

    def mapper_count_friends_per_line(self, _, line):
        fields = line.split()
        heroID = fields[0]
        numFriends = len(fields) - 1
        yield int(heroID), int(numFriends)

    def reducer_combine_friends(self, heroID, friendCounts):
        yield heroID, sum(friendCounts)

    def mapper_prep_for_sort(self, heroID, friendCounts):
        heroName = self.heroNames[heroID] # the heroNames dict is from mapper_init
        yield None, (friendCounts, heroName)

    def reducer_find_max_friends(self, key, value):
        yield max(value)

    def load_name_dictionary(self):
        self.heroNames = {} # must say self, if not heroNames dict will only have function scope

        with open("marvelnames.txt", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('"')
                # ('1 "Spiderman"').split('"') --> returns ['1 ', 'Spiderman', '']
                heroID = int(fields[0])
                self.heroNames[heroID] = fields[1] # add to the heroNames dict

if __name__ == '__main__':
    MostPopularSuperHero.run()
