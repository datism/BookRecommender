from flask import Flask
import pandas
import json
import requests
import numpy
from flask_cors import CORS, cross_origin

bookFilePath = '../data/BX-Books.csv'
ratingFilePath = '../data/BX-Book-Ratings.csv'
bestBookFilePath = '../data/Best-Books.csv'

bookDf = pandas.read_csv(bookFilePath, encoding='ISO-8859-1', on_bad_lines='skip', sep=';', low_memory=False)
ratingDf = pandas.read_csv(ratingFilePath, encoding='ISO-8859-1', on_bad_lines='skip', sep=';', low_memory=False)
bestBookDf = pandas.read_csv(bestBookFilePath, encoding='ISO-8859-1', on_bad_lines='skip', sep=';', low_memory=False)

def retrive_books(user):
    retrival_data = json.dumps({"inputs": [user]})

    response = requests.post('http://localhost:8501/v1/models/retrival_model:predict', retrival_data)
    retrival_output = response.json()

    return retrival_output["outputs"]["output_2"][0]

def rank_books(user, books):
    instances = []
    for book in books:
        instances.append({"user_id": user, "isbn": book})
    ranking_data = json.dumps({"instances": instances})

    response = requests.post('http://localhost:8501/v1/models/ranking_model:predict', data=ranking_data)
    ranking_output = response.json()

    ranking_predict = ranking_output['predictions']
    return [ranks[0] for ranks in ranking_predict]

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.get("/<user>/recbooks")
@cross_origin()
def getBooks(user):
    user = int(user) 

    userReadBook = ratingDf[ratingDf['User-ID'] == user]

    if userReadBook.shape[0] == 0:
        rec_books = bestBookDf["ISBN"][:20]
        rec_books_df = bookDf[bookDf["ISBN"].isin(rec_books)]
    else:
        rec_books = retrive_books(user)
        rankings = rank_books(user, rec_books)

        idx = numpy.argsort(rankings)
        sorted_rec_books = numpy.array(rec_books)[idx]

        rec_books_df = bookDf[bookDf["ISBN"].isin(sorted_rec_books)]

    return json.loads(rec_books_df.to_json(orient="records"))
