from mrjob.job import MRJob
# One million rows in our input data - use EMR to split the job up among workers. However,
# since our job got sent out to different reducer nodes,
# and the list of most similar movies is not sorted AS A WHOLE in the 16 blocks of results,  
# but we only want to search for ONE movie,
# we can instead run the job on our own computer despite there being one million movie ratings. 
class StarWarsSims(MRJob):
    def mapper(self, key, line):
        (movieName, value) = line.split('\t')
        if (movieName == '"Star Wars: Episode IV - A New Hope (1977)"'):
            (bracket, simName, values) = value.split('"')
            (empty, score, coraters) = values.split(',')
            cleanCoraters = coraters.split(']')
            numCoraters = int(cleanCoraters[0])
            if (numCoraters > 500): # enforce a minimum number of coraters to ensnure reliability 
                yield float(score), (simName, numCoraters) 
        
    def reducer(self, key, values):
        for value in values:
            yield key, value
        
if __name__ == '__main__':
    StarWarsSims.run()
    