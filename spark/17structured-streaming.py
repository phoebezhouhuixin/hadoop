from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract

# Create a SparkSession 
spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

# Monitor the logs_streaming_example directory for new log data, 
# and read in the raw lines as a dataframe called "accessLines".
# Note: Make the logs_streaming_example directory empty at first. Then, 
# run this py file in the command line. 
# Once it is running, drop the access_logs.txt file
# into the logs_streaming_example directory. The program should kick into action.
# Once you see the output of the first batch, copy paste the access_logs.txt
# into the directory again as access_logs_1.txt
# The second batch of output will have double the result of the first batch of output!
# But it only works if new .txt files are added to the directory, not if 
# txt files are deleted, or existing txt files are changed :/

accessLines = spark.readStream.text("../logs_streaming_example")
print(type(accessLines)) # DataFrame

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

# create a dataframe from the text file
logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Keep a running count of every access by status code
statusCountsDF = logsDF.groupBy(logsDF.status).count()

# Kick off our streaming query, dumping results to the console
query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Now the statusCountsDF will keep changing over time
# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

