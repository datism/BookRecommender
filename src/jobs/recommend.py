import sys
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.window import Window
import pyspark.sql.functions as F
sys.path.append('.\src')
from shared.load import load_ratings, load_book_metadata 

def run_job(spark, config):
    user_id = config.get("user")

    books_df = load_book_metadata(spark)
    rating_df = load_ratings(spark)

    # change ids from strings to integers
    w = Window.partitionBy(F.lit(1)).orderBy('ISBN')
    bookid_df = rating_df.select('ISBN').distinct().withColumn('Book-ID', F.row_number().over(w))
    rating_df = rating_df.join(bookid_df, 'ISBN')

    # select where user id
    read_books_df = rating_df.filter(F.col('User-ID') == user_id)\
                            .join(books_df, 'ISBN')\

    print('Books user has read:')
    read_books_df.select('ISBN','Book-Title','Year-Of-Publication','Publisher', 'Book-Rating').show()

    # load als model
    model = ALSModel.load(config.get("MODEL_PATH"))

    # transform
    predicted_ratings = model.recommendForUserSubset(read_books_df, 10)
 
    print('Predicted rating:')
    # convert to into interpretable format
    predicted_ratings = predicted_ratings.withColumn('rec', F.explode('recommendations'))\
                                        .select(F.col('rec.Book-ID'), F.col('rec.rating'))\
                                        .join(rating_df, 'Book-ID')\
                                        .join(books_df, 'ISBN')\
                                        .select('ISBN', 'Book-Title', 'rating')\
                                        .distinct()\
                                        .show(truncate=False)

    # Run recommend job
    return

# with open("./src/config.json", "r") as config_file:
#     config = json.load(config_file)
# config.update({'user': 11198})

# spark = SparkSession.builder.appName(config.get("APP_NAME")).getOrCreate()

# run_job(spark, config)
