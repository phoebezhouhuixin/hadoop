from mrjob.job import MRJob

# average number of friends, by age of different users
class MRFriendsByAge(MRJob):
    def mapper(self, _, line):
        (ID, name, age, numFriends) = line.split(',')
        yield age, float(numFriends)

    def reducer(self, age, numFriends):
        total = 0
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1
        yield age, total / numElements

if __name__ == '__main__':
    MRFriendsByAge.run() # from the MRJob parent class