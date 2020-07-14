# assignment qn
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("AmountByCustomer")
sc = SparkContext(conf = conf)

original = sc.textFile("../data/customer-orders.csv")
def read_data(line):
    linelist = line.split(",")
    return (linelist[0], float(linelist[2]))
cleaned = original.map(lambda x: read_data(x))
#print(cleaned.collect())
reduced = cleaned.reduceByKey(lambda x,y: x+y)
reduced_cleaned = reduced.mapValues(lambda x: round(x,2))

swapped = reduced_cleaned.map(lambda x: (x[1], x[0]))
sorted_output = swapped.sortByKey()
final_output = sorted_output.map(lambda x: (x[1], x[0]))
print(final_output.collect())