from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word = unicode(word, "utf-8", errors="ignore") #avoids issues in mrjob 5.0
            yield word.lower(), 1

    def combiner(self, key, values):
        yield key, sum(values) # exactly what the reducer function does!
        # The combiner runs within the confines of the mapper node BEFORE being sent to the reducer.
        # But the combiner might not even be called 
        # therefore we shouldn't do things that are not done by the reducer.
        # Hadoop now has the option of doing the reduction work within the mapper nodes
        # without affecting the overall MR output,
        # just how it works backend when we are running elastic mapreduce on the cloud/ using more than one computer to evaluate the output.

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
