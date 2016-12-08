# coding: utf-8
import os
from stock.stocks import get_ticker, get_avg_stock_quote, get_avg_qtr_stock_quote
os.chdir('mining')
from cache import download
from retrieve_10k import SGML_to_files, get_risk_factors
from retrieve_index import get_index
os.chdir('..')
import sys
import pymysql
import json

def get_viable_index(form_index):
    ticker = ''
    for index_info in form_index:
        company_name = index_info['Company Name']
        idx = company_name.find('\\') - 1
        ticker = get_ticker(company_name if idx < 0 else company_name[:idx])
        if ticker != '':
            yield (company_name, ticker, index_info['Filename'])

def raw_data_generator(year, quarter, n=1, offset=0, check_raw=True): #n is the number of records you want 
    form_index = get_index(year, quarter) # we might want to turn this into a generation function
    record_no = 1
    num_retrieved = 0
    for company in get_viable_index(form_index):
        if offset > 0:
            offset -= 1
        else:
            if (num_retrieved  == n): 
                raise StopIteration
            if (check_raw and check_raw_exists(company[0],year)):
                continue
            try:
                raw_features = get_raw_features(year,quarter, company[1], company[2])
                yield {'name':company[0], 'year':year, 'quarter':quarter, 'ticker':company[1],
                    'stock_improvement':raw_features['stock_improvement'],
                    'risk_factors':raw_features['risk_factors']}
                num_retrieved += 1
                print("record " + str(record_no) + " " + company[0])
            except KeyboardInterrupt:					
                raise StopIteration
            except GeneratorExit:
                raise StopIteration
            except:
                print("skipping 1 due to " + str(sys.exc_info()[0]) + ", " + str(sys.exc_info()[1]))
                #raise StopIteration
        record_no += 1

def check_raw_exists(name, year):
    sql = "SELECT count(*) FROM raw_data r WHERE r.year = {year} AND r.name = '{name}'" 
    query = sql.format(year=year,name=name.replace("'","''"))
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(query)
    row = cur.fetchone()
    val = row[0]
    cur.close()
    conn.close()    
    if int(val) > 0: #should only ever be 0 or 1 
        return True
    return False
    
        
def get_and_store_raw_data_into_db(year, quarter, n=1, offset=0):
    conn = connect_to_db()
    for data in raw_data_generator(year,quarter,n,offset):
        store_raw_row_into_db(conn,data)
        conn.commit()
    conn.close()
        
def get_raw_data(year, quarter, n=1, offset=0): #n is the number of records you want 
    raw_data = []
    for data in raw_data_generator(year,quarter,n,offset):
        raw_data += [data]
    return raw_data

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

def get_raw_features(year, quarter, ticker, path): #we want to predict stock improvement
    risk_factors = get_risk_factors(path)
    qtr_stock_price = get_avg_qtr_stock_quote(ticker,year,quarter)
    improvement = get_next_qtr_stock_quote(ticker,year,quarter) - qtr_stock_price
    features = {'risk_factors':risk_factors,
                'stock_improvement': improvement}
    return features

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

def store_raw_row_into_db(conn, data):
    try:
        sql = "INSERT INTO raw_data (name, year, quarter, improvement, risk_factors, ticker) VALUES ('{name}', {year}, {quarter}, {stock_improvement}, '{risk_factors}', '{ticker}')".format(name=conn.escape_string(data['name']),year=data['year'],quarter=data['quarter'],stock_improvement=data['stock_improvement'],risk_factors=conn.escape_string(data['risk_factors']),ticker=data['ticker']) 
        conn.query(sql)
    except pymysql.IntegrityError:
        pass

def store_raw_array_into_db(data_array):
    conn = connect_to_db()
    for data in data_array:
        store_raw_row_into_db(conn, data)
    conn.commit()
    conn.close()

def retrieve_raw_data(year=None, quarter=None, count=99999, offset=0, check_analyzed=True):
    if not year is None:
        if not quarter is None:
            if check_analyzed:
                sql = "SELECT r.name, r.year, r.quarter, r.improvement, r.risk_factors, r.ticker FROM raw_data r LEFT JOIN features f ON f.name = r.name AND r.year = f.year AND r.quarter = f.quarter WHERE r.year = " + str(year) + " AND r.quarter = " + str(quarter) + " AND f.name IS NULL LIMIT " + str(count) + " OFFSET " + str(offset)
            else:
                sql = "SELECT name, year, quarter, improvement, risk_factors, ticker FROM raw_data WHERE year = " + str(year) + " AND quarter = " + str(quarter) + " LIMIT " + str(count) + " OFFSET " + str(offset)
        else:
            if check_analyzed:
                sql = "SELECT r.name, r.year, r.quarter, r.improvement, r.risk_factors, r.ticker FROM raw_data r LEFT JOIN features f ON f.name = r.name AND r.year = f.year AND r.quarter = f.quarter WHERE r.year = " + str(year) + " AND f.name IS NULL LIMIT " + str(count) + " OFFSET " + str(offset)
            else:
                sql = "SELECT name, year, quarter, improvement, risk_factors, ticker FROM raw_data WHERE year = " + str(year) +  " LIMIT " + str(count) + " OFFSET " + str(offset)
    else:
        if not quarter is None:
            if check_analyzed:
                sql = "SELECT r.name, r.year, r.quarter, r.improvement, r.risk_factors, r.ticker FROM raw_data r LEFT JOIN features f ON f.name = r.name AND r.year = f.year AND r.quarter = f.quarter WHERE r.quarter = " + str(quarter) + " AND f.name IS NULL LIMIT " + str(count) + " OFFSET " + str(offset)
            else:
                sql = "SELECT name, year, quarter, improvement, risk_factors, ticker FROM raw_data WHERE quarter = " + str(quarter) + " LIMIT " + str(count) + " OFFSET " + str(offset)
        else:
            if check_analyzed:
                sql = "SELECT r.name, r.year, r.quarter, r.improvement, r.risk_factors, r.ticker FROM raw_data r LEFT JOIN features f ON f.name = r.name AND r.year = f.year AND r.quarter = f.quarter WHERE f.name IS NULL LIMIT " + str(count) + " OFFSET " + str(offset)
            else:
                sql = "SELECT name, year, quarter, improvement, risk_factors, ticker FROM raw_data LIMIT " + str(count) + " OFFSET " + str(offset)
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{'name':row[0], 'year':row[1], 'quarter':row[2], 'stock_improvement':row[3], 'risk_factors':row[4], 'ticker':row[5]} for row in rows]

    	
