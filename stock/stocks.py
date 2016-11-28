# coding: utf-8
import requests
import html
import json
import pymysql
import time

def getAuthDict(file):
    with open(file,'r') as fp:
        authdict = json.load(fp)
    return authdict

def get_company_name(ticker):
    page = requests.get(url="http://finance.yahoo.com/quote/" + ticker)
    title = html.unescape(page.text[page.text.find("<title>") + 7:page.text.find("</title>")])
    if title == "Requested symbol wasn't found":
        company = ''
    else:
        company = scrape_company(page)
    return company

def update_company_names():
    changed = 0
    missed = 0
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute('SELECT ticker FROM Company where name is NULL')
    for row in cur:
        print('trying symbol {t} C:{c} M:{m}'.format(t=row[0], c=changed, m=missed))
        cname = get_company_name(row[0])
        if cname != '':
            conn.query("UPDATE Company SET name = '" + cname.replace("'","''") + "' WHERE ticker = '" + row[0] + "';")
            conn.commit()
            changed += 1
        else:
            missed += 1
        time.sleep(1)
    cur.close()
    conn.close()
    print("changed {} and missed {}".format(changed, missed))
    
def scrape_company(page):
    text = page.text
    text = text[text.find('<h1 class="D(ib) Fz(18px)"'):]
    company = html.unescape(text[text.find(">")+1:text.find("</h1>")])
    return company

def connect_to_db():
    auth = getAuthDict('auth.json')
    conn = pymysql.connect(auth['DBhost'],auth['DBuser'],auth['DBpass'],port=int(auth['DBport']),use_unicode=True)
    conn.select_db('Edgar')
    return conn

def get_ticker(company_name):
    conn = connect_to_db()
    cur = conn.cursor()
    rows = cur.execute("SELECT ticker from Company WHERE name like '%" + company_name.replace(' ','%') + "%'")
    if rows > 0:
        ticker = cur.fetchone()[0]
    else:
        ticker = ''
    cur.close()
    conn.close()
    return ticker

def get_stock_quote(ticker, date):
    conn = connect_to_db()
    cur = conn.cursor()
    rows = cur.execute("SELECT adj_close FROM HistoricalData WHERE ticker = '" + ticker + "' AND date = '" + date + "'")
    if rows == 1:
        quote = cur.fetchone()[0]
    else:
        quote = 0
    cur.close()
    conn.close()
    return quote

def get_avg_stock_quote(ticker, start_date, end_date):
    conn = connect_to_db()
    cur = conn.cursor()
    rows = cur.execute("SELECT AVG(adj_close) FROM HistoricalData WHERE ticker = '" + ticker + "' AND date BETWEEN '" + start_date + "' AND '" + end_date + "'")
    if rows == 1:
        quote = cur.fetchone()[0]
    else:
        quote = 0
    cur.close()
    conn.close()
    return quote

def get_avg_qtr_stock_quote(ticker, year, quarter):
    conn = connect_to_db()
    cur = conn.cursor()
    rows = cur.execute("SELECT AVG(adj_close) FROM HistoricalData WHERE ticker = '" + ticker + "' AND YEAR(date) = '" + str(year) + "' AND QUARTER(date) = '" + str(quarter) + "'")
    if rows == 1:
        quote = cur.fetchone()[0]
    else:
        quote = 0
    cur.close()
    conn.close()
    return quote

