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

