from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression

if __name__ == "__main__":

    # Create a SparkSession
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

    # Load up our data and convert it to the format MLLib expects.
    inputLines = spark.sparkContext.textFile("../data/regression.txt")
    data = inputLines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))

    # print(data.take(10))
    # data is [(-1.74, DenseVector([1.66])), ...]

    # Convert this RDD to a DataFrame
    colNames = ["label", "features"]
    df = data.toDF(colNames)

    # Note, there are lots of cases where you can avoid going from an RDD to a DataFrame.
    # Perhaps you're importing data from a real database. 
    # Or you are using structured streaming to get your data.

    # Let's split our data into training data and testing data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create and fit our linear regression model
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    model = lir.fit(trainingDF)

    # Now see if we can predict values in our test data.
  
    # Generate predictions for all records of the test set
    fullPredictions = model.transform(testDF).cache()
    fullPredictions.show() # print(type(fullPredictions)) --> DataFrame
    '''
    # DataFrame with 3 columns: label, features, and prediction
    +-----+--------+-------------------+
    |label|features|         prediction|
    +-----+--------+-------------------+
    |-2.54|  [2.39]|-1.7214160559883884| ...

    '''
    # Extract the predictions and the "known" correct labels.
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])
    # print(type(predictions)) # RDD
    # Zip two RDDs together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    # for prediction in predictionAndLabel:
    #   print(prediction)


    # Stop the session
    spark.stop()
