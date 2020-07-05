from mrjob.job import MRJob
# WORD_REGEXP = re.compile(r"[\w]+") # whitespace

class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        words = line.split() 
        # OR words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1
            # yield 1, word.lower()
            ''' If we use yield 1, word.lower()
            the mapper() will return
            1	"self-employment:"
            1	"building"
            1	"an"
            1	"internet"
            1	"business"

            But then the reducer won't work, because the generator of values doesn't generate
            TypeError: 'int' object is not iterable
            because we use sum(key), which means the reducer() tries to sum(1) for EVERY SINGLE kv pair,
            i.e. it doesnt sum all the 1s for "building", all the 1s for "an", ...
            '''
    def reducer(self, key, values):
        yield key, sum(values)
        # yield sum(key), values

if __name__ == '__main__': # when you run wordpreq.py, 
    MRWordFrequencyCount.run() # run the class
