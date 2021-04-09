
import json
import seaborn as sns
import os
import codecs
import warnings
import _pickle as pickle
import pandas as pd
import datetime
import numpy as np
import itertools
import json
import pyodbc
from pyodbc import DataError
from pyodbc import IntegrityError
from bs4 import BeautifulSoup
import requests
import threading
import re
from datetime import  timedelta
import pytz
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import zlib

try:
    import Queue
except:
    import queue as Queue



process_datetime = str(datetime.datetime.now(pytz.timezone('US/Pacific')))

headers = {
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
"Accept-Encoding": "gzip, deflate, br",
"Accept-Language": "en-US,en;q=0.9",
"Cache-Control": "max-age=0",
"Connection": "keep-alive",
"Host": "eresearch.fidelity.com",
"Sec-Fetch-Dest": "document",
"Sec-Fetch-Mode": "navigate",
"Sec-Fetch-Site": "none",
"Sec-Fetch-User": "?1",
"Upgrade-Insecure-Requests": "1",
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
        }

username = 'lum-customer-migcap-zone-residential'
def lum_pass(zone):
    conn = pyodbc.connect('DRIVER={ODBC DRIVER 13 FOR SQL SERVER};SERVER=bc5756vevd.database.windows.net;DATABASE=MIG;UID=garquette;PWD=Working1!')
    with conn:
        with conn.cursor() as cursor:
           password = cursor.execute("SELECT PASSWORD FROM LUMINATI.PASSWORD WHERE ZONE = '{0}' AND UPDATED = (SELECT MAX(UPDATED) FROM LUMINATI.PASSWORD WHERE ZONE='{0}')".format(zone)).fetchone()[0]

    return password

password = lum_pass('lum-customer-migcap-zone-residential')

def getProxies():
    global username, password
    port = 22225
    session_id = random.random()
    super_proxy_url = ('http://%s-country-us-session-%s:%s@zproxy.lum-superproxy.io:%d' %
        (username, session_id, password, port))
    proxies = {
        'http': super_proxy_url,
        'https': super_proxy_url
    }
    return proxies



def get_conn():
    #conn = pyodbc.connect(server='tcp:bc5756vevd.database.windows.net,1433;Database=GUEST', user='guest_test', passwd='mig_auth5',charset='utf8')
    #conn = pyodbc.connect('DRIVER=lib\libmsodbcsql-13.so;SERVER=bc5756vevd.database.windows.net,1433;DATABASE=MIG;UID=garquette;PWD=Working1!')
    conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=bc5756vevd.database.windows.net,1433;DATABASE=MIG;UID=garquette;PWD=Working1!')
    return conn

def insert(cur, sql, args):
    cur.execute(sql, args)   


def scrape():
    global  process_datetime, headers
    
    
    with requests.Session() as session:
        session.headers.update(headers)
        
        url = 'https://eresearch.fidelity.com/eresearch/gotoBL/fidelityTopOrders.jhtml'
               
            
        resp = session.get(url, proxies = getProxies(), timeout = 20)
        
        
        bs = BeautifulSoup(resp.content, features = "lxml")
        all_data = bs.find('table', id = 'topOrdersTable').find('tbody').findAll('tr')    
        if all_data:
            for data in all_data:
                all_records = data.findAll('td')
                
                try:
                    rank = all_records[0].find('span').text.strip()
                except:
                    print('cannot get rank data')
                    rank = None
                
                try:
                    ticker = all_records[1].find('span').text.strip()
                
                except:
                    print('cannot get ticker data')
                    ticker = None
                try:
                    cpny_name = all_records[2].text.strip()
                except:
                    print('cannot get company name')
                    cpny_name = None 
                try:
                    abs_price_change = all_records[3].find('span').text.strip()
                    abs_price_change = float(abs_price_change)
                except:
                    print('cannot get absolute price change data')
                    abs_price_change = None
                
                try:
                    rel_price_change = re.search(r'\((.*)%\)', all_records[3].text).group(1)
                    rel_price_change = float(rel_price_change)/100
                except:
                    print('cannot get relative price change data')
                    rel_price_change = None
                try:
                    buy_order_qty = all_records[4].find('span').text.replace(',', '')
                    buy_order_qty = int(buy_order_qty)
                except:
                    print('cannot get buy order quantity data')
                    buy_order_qty = None 
                try:
                    buy_ratio = re.search(r'(.*)%\sBuys', all_records[5].find('img')['title']).group(1)
                    buy_ratio = float(buy_ratio)/100
                except:
                    print('cannot get buy ratio data')
                    buy_ratio = None 
                try:
                    sell_ratio = re.search(r',\s(.*)%\sSells', all_records[5].find('img')['title']).group(1)
                    sell_ratio = float(sell_ratio)/100
                except:
                    print('cannot get sell ratio data')
                    sell_ratio = None 
                try:
                    sell_order_qty = all_records[6].find('span').text.replace(',', '')
                    sell_order_qty = int(sell_order_qty)
                except:
                    print('cannot get sell order quantity data')
                    sell_order_qty = None 
                try:
                    news_title = all_records[7].find('a')['title'].strip()
                except:
                    print('cannot find news title')
                    news_title = None
                try:
                    news_URL = all_records[7].find('a')['href'].strip()
                    news_URL = 'https://eresearch.fidelity.com' + news_URL
                except:
                    print('cannot find news URL')
                    news_URL = None
                record = tuple([process_datetime, rank, ticker, cpny_name, abs_price_change, rel_price_change, buy_order_qty, buy_ratio, \
                                 sell_ratio, sell_order_qty, news_title, news_URL])
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        print(record)
                        sql = 'insert into WSTBET.Fidelity_buy_sell values(?' + ', ?' * (len(record)-1)  +')'
                        try:
                            insert(cur, sql=sql, args= record)
                    
                        except pyodbc.IntegrityError as err: # use integrityerror to avoid inserting duplicate records
                            print(err)
                     
                    
                
                        except pyodbc.DataError as data_err: 
                            print(data_err)
                    
                
                        except pyodbc.ProgrammingError as prog_err:
                            print(prog_err)

def main():
    scrape()

if __name__ == '__main__':
    main()