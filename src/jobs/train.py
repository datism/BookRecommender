import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.types import *
from pyspark.sql import functions as F
sys.path.append('.\src')
from shared.load import load_ratings

def run_job(spark, config):
    seed = 1800083193

    rating_df = load_ratings(spark)

    # Change ids from strings to integers
    w = Window.partitionBy(F.lit(1)).orderBy('ISBN')
    bookid_df = rating_df.select('ISBN').distinct().withColumn('Book-ID', F.row_number().over(w))
    
    rating_df = rating_df.join(bookid_df, 'ISBN')

    # Split dataset
    (training_df, validation_df, test_df) = rating_df.randomSplit([0.6, 0.2, 0.2], seed = seed)

    print('Training: {0}, validation: {1}, test: {2}\n'
        .format(training_df.count(), validation_df.count(), test_df.count()))

    # Initialize ALS learner
    als = ALS()

    # Set the parameters for the method
    als.setMaxIter(2)\
        .setItemCol("Book-ID")\
        .setRatingCol("Book-Rating")\
        .setUserCol("User-ID")\
        .setNonnegative(True)\
        .setImplicitPrefs(False)\
        .setColdStartStrategy("drop")
        
    # Now let's compute an evaluation metric for our test dataset
    # We Create an RMSE evaluator using the label and predicted columns
    reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="Book-Rating", metricName="rmse")
    
    ranks = [4, 8, 12, 16]
    regParams = [0.1, 0.15, 0.2, 0.25]
    errors = [[0]*len(ranks)]*len(regParams)
    models = [[0]*len(ranks)]*len(regParams)
    min_error = float('inf')
    i = 0

    for regParam in regParams:
        j = 0
        for rank in ranks:
            # Set the rank here:
            als.setParams(rank = rank, regParam = regParam)
            # Create the model with these parameters.
            model = als.fit(training_df)
            # Run the model to create a prediction. Predict against the validation_df.
            predict_df = model.transform(validation_df)

            # Remove NaN values from prediction (due to SPARK-14489)
            predicted_ratings_df = predict_df.filter(predict_df.prediction != float('nan'))
            predicted_ratings_df = predicted_ratings_df.withColumn("prediction", F.abs(F.round(predicted_ratings_df["prediction"],0)))
            # Run the previously created RMSE evaluator, reg_eval, on the predicted_ratings_df DataFrame
            error = reg_eval.evaluate(predicted_ratings_df)
            errors[i][j] = error
            models[i][j] = model
            print('For rank %s, regularization parameter %s the RMSE is %s' % (rank, regParam, error))
            if error < min_error:
                min_error = error
                best_params = [i,j]
            j += 1
        i += 1

    als.setRegParam(regParams[best_params[0]])
    als.setRank(ranks[best_params[1]])
    print('The best model was trained with regularization parameter %s' % regParams[best_params[0]])
    print('The best model was trained with rank %s' % ranks[best_params[1]])
    best_model = models[best_params[0]][best_params[1]]

    # Test model
    test_df = test_df.withColumn("Book-Rating", test_df["Book-Rating"].cast(DoubleType()))
    predict_df = best_model.transform(test_df)

    # Remove NaN values from prediction (due to SPARK-14489)
    predicted_test_df = predict_df.filter(predict_df.prediction != float('nan'))

    # Round floats to whole numbers
    predicted_test_df = predicted_test_df.withColumn("prediction", F.abs(F.round(predicted_test_df["prediction"],0)))
    # Run the previously created RMSE evaluator, reg_eval, on the predicted_test_df DataFrame
    test_RMSE = reg_eval.evaluate(predicted_test_df)

    print('The model had a RMSE on the test set of {0}'.format(test_RMSE))

    avg_ratings_df = training_df.groupBy().avg('Book-Rating').select(F.round('avg(Book-Rating)'))
    avg_ratings_df.show(3)
    # Extract the average rating value.
    training_avg_ratings = avg_ratings_df.collect()[0][0]

    print('The average ratings in the dataset is {0}'.format(training_avg_ratings))

    # Add a column with the average rating
    test_for_avg_df = test_df.withColumn('prediction', F.lit(training_avg_ratings))

    # Run the previously created RMSE evaluator, reg_eval, on the test_for_avg_df DataFrame
    test_avg_RMSE = reg_eval.evaluate(test_for_avg_df)

    print("The RMSE on the average set is {0}".format(test_avg_RMSE))

    # Save model
    best_model.write().overwrite().save(config.get("MODEL_PATH"))

    return

with open("./src/config.json", "r") as config_file:
    config = json.load(config_file)

spark = SparkSession.builder.appName(config.get("APP_NAME")).getOrCreate()

run_job(spark, config)
