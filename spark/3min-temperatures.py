# min temp by location- filtering RDDs
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    # "stationid, date, TMAX or TMIN, temp "
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)

# filter the RDD
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1]) 
# x is each RDD value, i.e. (stationID, TMIN or TMAX, temperature)
# The lambda function returns a boolean. 
# rdd.filter() takes the boolean as input

stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
