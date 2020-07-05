from mrjob.job import MRJob
from mrjob.step import MRStep

class customerHasSpent(MRJob):
    def steps(self):
        return [MRStep(mapper = self.mapper, reducer = self.reducer),
                MRStep(mapper = self.mapper1, reducer = self.reducer1)]
    def mapper(self, _, line):
        customer = line.split(",")
        yield customer[0],float(customer[2]) # must say float(), else will be string
    def reducer(self, key, values):
        yield key, sum(values)
    def mapper1(self, cust, amtSpent):
        #yield amtSpent, cust
        yield "%04.02f"%amtSpent, cust
    def reducer1(self, amtSpent, custs):
        for cust in custs:
            yield amtSpent, cust
if __name__ == "__main__":
    customerHasSpent.run()
'''
Why is it not properly sorted?
Your job probably got broken up into two executors. 
Unfortunately Hadoop does not combine all of the results together at the end for you; 
it just sorts the results within each node.

To sort everything together, you need to either force everything into a 
single key at the end, write some sort of script that combines the results 
from each node and sorts them after the fact, or - more commonly - 
output your data into some sort of data store that can sort it for you.'''