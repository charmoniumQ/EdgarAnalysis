# coding: utf-8
import os
from analysis.analysis import concept_analysis, emotion_analysis, sentiment_analysis
from stock.stocks import get_ticker, get_avg_stock_quote, get_avg_qtr_stock_quote
os.chdir('mining')
from cache import download
from retrieve_10k import SGML_to_files, get_risk_factors
from retrieve_index import get_index
os.chdir('..')
import sys
from raw_data import retrieve_raw_data
import json
import pymysql


def getAuthDict(file):
    with open(file,'r') as fp:
        authdict = json.load(fp)
    return authdict

def connect_to_db():
    auth = getAuthDict('auth.json')
    conn = pymysql.connect(auth['DBhost'],auth['DBuser'],auth['DBpass'],port=int(auth['DBport']),use_unicode=True)
    conn.encoding='utf-8'
    conn.select_db('Edgar')
    return conn

def get_viable_index(form_index):
    ticker = ''
    for index_info in form_index:
        company_name = index_info['Company Name']
        ticker = get_ticker(company_name)
        if ticker != '':
            yield (company_name, ticker, index_info['Filename'])

def training_data_generator(year, quarter, n=1, offset=0): #n is the number of records you want 
    form_index = get_index(year, quarter) # we might want to turn this into a generation function
    record_no = 1
    num_retrieved = 0
    for company in get_viable_index(form_index):
        if offset > 0:
            offset -= 1
        else:
            if (num_retrieved == n):
                raise StopIteration
            try:    
                features = get_training_features(year,quarter,company[1],company[2])
                yield {'name':company[0], 'year':year, 'quarter':quarter, 'ticker':company[1],
                    'anger':features['anger'], 
                    'disgust':features['disgust'],
                    'fear':features['fear'],
                    'joy':features['joy'],
                    'sadness':features['sadness'],
                    'sentiment':features['sentiment'],
                    'sentiment_type':features['sentiment_type'],
                    'stock_improvement':features['stock_improvement']}
                num_retrieved += 1
                print("record " + str(record_no) + " " + company[0])
            except KeyboardInterrupt:
                raise StopIteration
            #except:
                #print("skipping " + company[0] + " due to " + str(sys.exc_info()[0]))
        record_no += 1

def get_training_data(year, quarter, n=1, offset=0): #n is the number of records you want 
    training_data = []
    for data in training_data_generator(year,quarter,n,offset):
        training_data += [data]
    return training_data

def prediction_data_generator(year, quarter, n=1, offset=0):
    form_index = get_index(year, quarter) # we might want to turn this into a generation function
    record_no = 1
    num_retrieved = 0
    for company in get_viable_index(form_index):
        if offset > 0:
            offset -= 1
        else:
            if (num_retrieved == n):
                raise StopIteration
            try:    
                features = get_predict_features(year,quarter,company[1],company[2])
                yield {'name':company[0], 'year':year, 'quarter':quarter, 'ticker':company[1],
                    'anger':features['anger'], 
                    'disgust':features['disgust'],
                    'fear':features['fear'],
                    'joy':features['joy'],
                    'sadness':features['sadness'],
                    'sentiment':features['sentiment'],
                    'sentiment_type':features['sentiment_type']}
                num_retrieved += 1
                print("record " + str(record_no) + " " + company[0])
            except KeyboardInterrupt:
                raise StopIteration
            except:
                print("skipping 1 due to " + str(sys.exc_info()[0]) + ", " + str(sys.exc_info()[1]))
        record_no += 1

def get_prediction_data(year, quarter, n):
    predict_data = []
    for data in prediction_data_generator(year,quarter,n,offset):
        predict_data += [data]
    return predict_data

def extract_x(features):
    return [features['anger'], 
            features['disgust'],
            features['fear'],
            features['joy'],
            features['sadness'],
            features['sentiment'],
            features['sentiment_type']]    

def extract_y(features):
    return [features['stock_improvement']]

def get_x_array(feature_array):
    return [extract_x(x) for x in feature_array]

def get_y_array(feature_array):
    return [extract_y(y) for y in feature_array]

def get_last_qtr_stock_quote(ticker, year, qtr):
    last_qtr = ((qtr - 2) % 4) + 1
    if last_qtr == 4:
       year -= 1
    return get_avg_qtr_stock_quote(ticker, year, last_qtr)

def get_next_qtr_stock_quote(ticker, year, qtr):
    next_qtr = (qtr % 4) + 1
    if next_qtr == 1:
       year += 1
    return get_avg_qtr_stock_quote(ticker, year, next_qtr)

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
    risk_factors = get_risk_facors(path)
    emotions = emotion_analysis(risk_factors)
    sentiment = sentiment_analysis(risk_factors)
    qtr_stock_price = get_avg_qtr_stock_quote(ticker,year,quarter)
    improvement = get_next_qtr_stock_quote(ticker,year,quarter) - qtr_stock_price
    features = {'anger': emotions['anger'],
                'disgust': emotions['disgust'],
                'fear': emotions['fear'],
                'joy': emotions['joy'],
                'sadness': emotions['sadness'],
                'sentiment': sentiment,
                'sentiment_type': (lambda x : -1 if x < -0.25 else 1 if x > 0.25 else 0)(sentiment), #(lambda x : 'negatve' if x < -0.25 else 'positive' if x > 0.25 else 'neutral')
                'stock_improvement': improvement}
    return features

def extract_features_from_raw(data):
    risk_factors = data['risk_factors']
    emotions = emotion_analysis(risk_factors)
    sentiment = sentiment_analysis(risk_factors)
    improvement = data['stock_improvement']
    features = {'name': data['name'],
                'ticker': data['ticker'],
                'year': data['year'],
                'quarter': data['quarter'],
                'anger': emotions['anger'],
                'disgust': emotions['disgust'],
                'fear': emotions['fear'],
                'joy': emotions['joy'],
                'sadness': emotions['sadness'],
                'sentiment': sentiment,
                'sentiment_type': (lambda x : -1 if x < -0.25 else 1 if x > 0.25 else 0)(sentiment), #(lambda x : 'negatve' if x < -0.25 else 'positive' if x > 0.25 else 'neutral')
                'stock_improvement': improvement}
    return features

def extract_features_from_raw_array(raw_array):
    for x in raw_array:
        #try:
            yield extract_features_from_raw(x)
            print('analyzing ' + x['name'])
        #except KeyError:
        #    print('skipping 1 due to KeyError')

def store_feature_row_into_db(conn, data):
    try:
        sql = "INSERT INTO features (name, year, quarter, improvement, anger, disgust, fear, joy, sadness, sentiment, sentiment_type, ticker) VALUES ('{name}', {year}, {quarter}, {stock_improvement}, {anger}, {disgust}, {fear}, {joy}, {sadness}, {sentiment}, {sentiment_type}, '{ticker}')".format(name=conn.escape_string(data['name']),year=data['year'],quarter=data['quarter'],stock_improvement=data['stock_improvement'],anger=data['anger'],disgust=data['disgust'],fear=data['fear'],joy=data['joy'],sadness=data['sadness'],sentiment=data['sentiment'],sentiment_type=data['sentiment_type'],ticker=data['ticker']) 
        conn.query(sql)
    except pymysql.IntegrityError:
        print('Ignoring IntegrityError')
        pass
    
def store_feature_array_into_db(data_array):
    conn = connect_to_db()
#    try:
    for data in data_array:
        store_feature_row_into_db(conn, data)
        conn.commit()
#    except:
#        print("stopping due to " + str(sys.exc_info()[0]) + ", " + str(sys.exc_info()[1]))
#        pass
    conn.close()

def retrieve_feature_data(limit,year=None,offset=0):
    conn = connect_to_db()
    if not year is None:
        sql = "SELECT name, year, quarter, improvement, anger, disgust, fear, joy, sadness, sentiment, sentiment_type, ticker FROM features WHERE year = {year} AND UseBit = 1 LIMIT {limit} OFFSET {offset}".format(year=str(year),limit=str(limit),offset=str(offset))
    else:
        sql = "SELECT name, year, quarter, improvement, anger, disgust, fear, joy, sadness, sentiment, sentiment_type, ticker FROM features AND UseBit = 1 LIMIT {limit} OFFSET {offset}".format(limit=str(limit),offset=str(offset))
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{'name':row[0], 'year':row[1], 'quarter':row[2], 'stock_improvement':row[3], 'anger':row[4], 'disgust':row[5], 'fear':row[6], 'joy':row[7], 'sadness':row[8], 'sentiment':row[9], 'sentiment_type':row[10], 'ticker':row[11]} for row in rows]

def analyze_and_store_raw_raw_data(year=None, quarter=None, limit=99999, offset=0):
    store_feature_array_into_db(extract_features_from_raw_array(retrieve_raw_data(year, quarter, limit, offset)))    

def retrieve_features_for_company(name,year):
    ticker = get_ticker(name)
    conn = connect_to_db()
    sql = "SELECT name, year, quarter, improvement, anger, disgust, fear, joy, sadness, sentiment, sentiment_type, ticker FROM features WHERE LOWER(REPLACE(REPLACE(REPLACE(CONCAT('%',name,'%'),' ','%'),'\\'',''),',','')) LIKE LOWER(REPLACE(REPLACE(CONCAT('%','{name}','%'),' ','%'),',','')) AND year = {year} AND ticker = '{ticker}'".format(name=name,year=str(year),ticker=ticker)
    cur = conn.cursor()
    num_rows = cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    conn.close()
    if num_rows == 0:
        print('Cannot find "{name}" in the database for {year}'.format(name=name,year=year))
        return None
    return {'name':row[0], 'year':row[1], 'quarter':row[2], 'stock_improvement':row[3], 'anger':row[4], 'disgust':row[5], 'fear':row[6], 'joy':row[7], 'sadness':row[8], 'sentiment':row[9], 'sentiment_type':row[10], 'ticker':row[11]}
    
