# coding: utf-8
import os
from analysis.analysis import concept_analysis, emotion_analysis, sentiment_analysis
from stock.stocks import get_ticker, get_avg_stock_quote, get_avg_qtr_stock_quote
os.chdir('mining')
from cache import download
from retrieve_10k import SGML_to_files, get_risk_factors
from retrieve_index import get_index
os.chdir('..')


#year = 2016
#quarter = 3

def get_viable_index(form_index):
    ticker = ''
    while ticker == '':
        index_info = next(form_index)
        company_name = index_info['Company Name']
        ticker = get_ticker(company_name)
    return (company_name, ticker, index_info['Filename'])

def get_training_data(year, quarter, n): #n is the number of records you want 
    form_index = get_index(year, quarter) # we might want to turn this into a generation function
    training_data = []
    for i in range(n):
        company = get_viable_index(form_index)
        training_data += [(company[0], year, quarter, get_training_features(year,quarter, company[1], company[2]))]
    return training_data

def get_predict_data(year, quarter):
    form_index = get_index(year, quarter)
    company = get_viable_index(form_index)
    return (company[0], year, quarter, get_predict_features(year,quarter, company[1], company[2]))
    

def get_last_qtr_stock_quote(ticker, year, qtr):
    last_qtr = ((qtr - 2) % 4) + 1
    if last_qtr == 4:
       year -= 1
    return get_avg_qtr_stock_quote(ticker, year, last_qtr)

def get_predict_features(year, quarter, ticker, path):
    risk_factors = get_risk_factors(path)
    emotions = emotion_analysis(risk_factors)
    sentiment = sentiment_analysis(risk_factors)
    qtr_stock_price = get_avg_qtr_stock_quote(ticker,year,quarter)
    features = {'anger': emotions['anger'],
                'disgust': emotions['disgust'],
                'fear': emotions['fear'],
                'joy': emotions['joy'],
                'sadness': emotions['sadness'],
                'sentiment': sentiment,
                'sentiment_type': (lambda x : -1 if x < -0.25 else 1 if x > 0.25 else 0)(sentiment)} #(lambda x : 'negatve' if x < -0.25 else 'positive' if x > 0.25 else 'neutral')
    return features


def get_training_features(year, quarter, ticker, path): #we want to predict stock improvement
    risk_factors = get_risk_factors(path)
    emotions = emotion_analysis(risk_factors)
    sentiment = sentiment_analysis(risk_factors)
    qtr_stock_price = get_avg_qtr_stock_quote(ticker,year,quarter)
    improvement = qtr_stock_price - get_last_qtr_stock_quote(ticker,year,quarter)
    features = {'anger': emotions['anger'],
                'disgust': emotions['disgust'],
                'fear': emotions['fear'],
                'joy': emotions['joy'],
                'sadness': emotions['sadness'],
                'sentiment': sentiment,
                'sentiment_type': (lambda x : -1 if x < -0.25 else 1 if x > 0.25 else 0)(sentiment), #(lambda x : 'negatve' if x < -0.25 else 'positive' if x > 0.25 else 'neutral')
                'stock_improvement': improvement}
    return features
