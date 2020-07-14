from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

# Create a SparkSession 
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=fields[1], age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("../data/friendsbyage.csv")
people = lines.map(mapper)

# Infer the schema, and register the df as a temporary view
schemaPeople = spark.createDataFrame(people).cache() 
# Note that we cache the df after creating it! (not cache the rdd)
schemaPeople.createOrReplaceTempView("people")

# Run SQL on view
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
for teen in teenagers.collect(): # The results of SQL queries are RDDs!
  print(teen) 
  # prints the Row that we stored in the people rdd,
  # e.g. Row(ID=21, name='Miles', age=19, numFriends=268)
  # Note that the resultdf is not sorted, but just a subset of the original df

# We can also use functions instead of SQL queries:
# e.g. number of people of each age group
schemaPeople.groupBy("age").count().orderBy("age").show() 
# The query function returns a <class 'pyspark.sql.dataframe.DataFrame'> that we can .show()
spark.stop()
