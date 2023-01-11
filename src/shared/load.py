import json
from pyspark.sql import SparkSession

with open("app/src/config.json", "r") as config_file:
    config = json.load(config_file)

def load_book_metadata(spark):
    bookFilePath = config.get("DATA_PATH") + "BX-Books.csv"
    return spark.read.options(inferSchema="true", header="true", delimiter=';').csv(bookFilePath)

def load_user_info(spark):
    userFilePath = config.get("DATA_PATH") + "BX-Users.csv"
    return spark.read.options(inferSchema="true", header="true", delimiter=';').csv(userFilePath)

def load_ratings(spark):
    ratingsFilePath = config.get("DATA_PATH") + "BX-Book-Ratings.csv"
    return spark.read.options(inferSchema="true", header="true", delimiter=';').csv(ratingsFilePath)

# spark = SparkSession.builder.appName(config.get("APP_NAME")).getOrCreate()
# bookDf = load_book_metadata(spark)
# bookDf.show(10)
# userDf = load_user_info(spark)
# userDf.show(10)
# ratingDf = load_ratings(spark)
# ratingDf.show(10)