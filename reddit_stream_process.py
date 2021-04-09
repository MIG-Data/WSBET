# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 14:59:08 2021

@author: tcai
"""


from gensim.models import Phrases
from gensim.corpora import Dictionary, MmCorpus
from gensim.models.word2vec import LineSentence
from gensim.models.ldamulticore import LdaMulticore
import json

import os
import codecs

import warnings
import _pickle as pickle
import pandas as pd
import datetime
import numpy as np
import itertools
import spacy
import nltk
import nltk.classify.util
from nltk.tokenize import sent_tokenize, word_tokenize, RegexpTokenizer
from nltk.stem import PorterStemmer, WordNetLemmatizer
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import stopwords, names
from nltk.util import ngrams
import s3fs
import boto3
from botocore.exceptions import ClientError


import nltk
import nltk.classify.util
from nltk.tokenize import sent_tokenize, word_tokenize, RegexpTokenizer

from nltk.corpus import stopwords, names
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from gensim import corpora, models, similarities, matutils
from sklearn.decomposition import TruncatedSVD
from sklearn.decomposition import NMF
from sklearn.metrics.pairwise import cosine_similarity
from sklearn import preprocessing
import json
import pyodbc
from pyodbc import DataError
from pyodbc import IntegrityError
from bs4 import BeautifulSoup
import requests
import threading
import re
import datetime
from datetime import timedelta
import pytz
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import sqlalchemy
from sqlalchemy import create_engine
import urllib
from ast import literal_eval
import yfinance as yf
import urllib
try:
    import Queue
except:
    import queue as Queue
import json
import logging
from get_all_tickers import get_tickers as gt

from collections import deque
import praw


ACCESS_KEY = 'AKIAWYG6AKIP2FMXUUXK'
SECRET_KEY = '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'
REGION = 'us-east-2'

base_url = 'http://webhose.io'
run_date = str(datetime.date.today())
CLIENT_ID = 'MakOOBfK0bVAtw'
SECRET_TOKEN = '76MTpF6ZVdJQ2uuApZC17kRbIdXIgQ'
username = 'tcai95'
password = 'Working1'
reddit = praw.Reddit(user_agent="Comment Extraction (by /u/USERNAME)",
                     client_id=CLIENT_ID, client_secret=SECRET_TOKEN,
                     username=username, password=password)

ticker_action_dict = dict() 
list_of_tickers = []

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# Output the all bucket names
def put_file_s3(bucket_name, local_dir, result_dict = None, bucket_folder = None):
    
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    # Retrieve the list of existing buckets
    response = s3_client.list_buckets()
    if bucket_name not in [bucket['Name'] for bucket in response['Buckets']]:
        create_bucket(bucket_name, 'us-east-2')
    content = open(local_dir, 'rb')
    
    fileName = local_dir.split('/')[-1]
    
    if bucket_folder != None:
        key = bucket_folder + '/' + fileName
    else:
        key = fileName
    
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    if result_dict == None:
    
        s3_client.put_object(
        Bucket=bucket_name, 
        Key = key,
        Body=content
        )
    
    # directly upload file from memory
    else:
   
        s3_client.put_object(
            Bucket = bucket_name,
            Key = key,
            Body = (bytes(json.dumps(result_dict).encode('UTF-8')))
    )

kinesis_client = boto3.client('kinesis', 
                                  region_name=REGION,  # enter the region
                                  aws_access_key_id=ACCESS_KEY,  # fill your AWS access key id
                                  aws_secret_access_key=SECRET_KEY)  # fill you aws secret access key
base_url = 'https://www.reddit.com'


def get_all_tickers():
    #file_path = 'C:/Users/tcai/Desktop/MIG_Capital/wstBet'
    file_path = '/home/ec2-user/WSB/ticker_list'
    #file_path = 'C:/Users/tcai/Desktop/MIG_Capital/wstBet/ticker_list'
    files = None
    for root, dirs, files in os.walk(file_path):
      root = root
      dirs = dirs
      files = deque(files)
    df_list = []
    
    while len(files) >0:
      file = files.popleft()
      full_file_path = os.path.join(root, file).replace("\\", "/")
      df = pd.read_csv(full_file_path)
      df_list.append(df)
    # concatenate each df with file date into one single large df for insertion
    df_final = pd.concat(df_list)
    return df_final['Symbol'].values.tolist()

list_of_tickers = get_all_tickers()


def ticker_lookup(row):
  
  if pd.isnull(row['ticker']) :
    full_text = row["body"]
    tokenized = word_tokenize(full_text)
    
    # Tokenize and compare to the list of U.S. tickers
    for token in tokenized:
      #print(token)
      if ':' in token:
        token = token.split(':')[1]
      token = token.strip('()')  
      if token in list_of_tickers:
        ticker = token
        return ticker

def insert_data(df, schema, table_name):
    creds = dict(driver = '{ODBC Driver 13 for SQL SERVER}', server = 'bc5756vevd.database.windows.net', port = 1433, database = 'MIG', uid = 'garquette', pwd = 'Working1!')
    params = 'DRIVER=' + creds['driver']+';'\
            'SERVER=' + creds['server']+';'\
            'DATABASE=' + creds['database']+ ';'\
            'UID=' + creds['uid']+ ';'\
            'PWD=' + creds['pwd']+ ';'\
            'PORT=' + str(creds['port']) + ';'
    params = urllib.parse.quote_plus(params)
    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    
    # using df.to_sql we could avoid of having nan insertion issue
    df.to_sql(name = table_name, con = db, schema = schema, if_exists = 'append', index = False)

def get_post_comment():
    global reddit, kinesis_client, ticker_action_dict
    
    subreddit = reddit.subreddit("wallstreetbets")
    #df = pd.DataFrame()
    df = pd.DataFrame()
    while True:
        try:
            
            for comment in subreddit.stream.comments():
                #print(submission.title)  # Output: the submission's title
                #print(submission.score)  # Output: the submission's score
                #print(submission.id)     # Output: the submission's ID
                #print(submission.url)
                
                print(datetime.datetime.fromtimestamp(comment.created_utc) + datetime.timedelta(hours = -8))
                #pprint.pprint(vars(submission))
                #pprint.pprint(vars(comment))
                
                #reddit.submission(comment.link_id.replace('t3_', '')).num_comments
                try:
                    print('title: ')
                    print(comment.link_title)
                    parent_id = str(comment.parent())
                    original = reddit.comment(parent_id)
                    #link_id =  comment.link_id.replace('t3_', '')
                    #if link_id not in link_id_dict.keys():
                        #link_id_dict[link_id] = reddit.submission(comment.link_id.replace('t3_', '')).permalink
                    
                    #print('parent: ')
                    #print(original.body)
                    #print('reply:')
                    #print(comment.body)
                except praw.exceptions.PRAWException as e:
                    pass
                try:
                    comment_name = comment.name
                except:
                    comment_name = None
                    print('cannot find comment name')
                    continue
                try:
                    comment_body = comment.body
                except:
                    comment_body = None
                    print('cannot find comment body')
                    continue
                try:
                    comment_likes = comment.likes
                except:
                    comment_likes = None
                    print('cannot find comment likes')
                    
                try:
                    comment_author = comment.author.name
                except:
                    comment_author = None
                    print('cannot find comment author')
                    
                try:
                    comment_id = comment.id
                except:
                    comment_id = None
                    print('cannot find comment id')
                    
                try:
                    comment_ups = comment.ups
                except:
                    comment_ups = None
                    print('cannot find comment ups')
                    
                try:
                    comment_downs = comment.downs
                except:
                    comment_downs = None
                    print('cannot find comment downs')
                    
                try:
                    comment_score = comment.score
                except:
                    comment_score = None
                    print('cannot find comment score')
                    
                try:
                    comment_creation_utc = str(datetime.datetime.fromtimestamp(comment.created_utc) + datetime.timedelta(hours = -8))
                except:
                    comment_creation_utc = None
                    print('cannot find comment creation time')
                    
                try:
                    comment_permalink = base_url + comment.permalink
                except:
                    comment_permalink = None
                    print('cannot find comment permalink')
                    
                try:
                    comment_link_title = comment.link_title
                except:
                    comment_link_title = None
                    print('cannot find comment link title')
                    
                
                try:
                    original_body = original.body
                except:
                    original_body = None
                    print('cannot find post body')
                    
                try:
                    original_id = original.id
                except:
                    original_id = None
                    print('cannot find original id')
                    
                try:
                    original_link = base_url + original.permalink
                except:
                    original_link = None
                    print('cannot find original link')
                    
                try:
                    original_ups = original.ups
                except:
                    original_ups = None
                    print('cannot find original ups')
                    
                try:
                    original_downs = original.downs
                except:
                    original_downs = None
                    print('cannot find original downs')
                    
                try:
                    original_likes = original.likes
                except:
                    original_likes = None
                    print('cannot find original likes')
                    
                
                
                payload = {
                     'name': comment_name,
                        'comment_body':comment_body,
                        'comment_likes':comment_likes,
                        'comment_author':comment_author,
                        'comment_id': comment_id,
                        'comment_ups': comment_ups,
                        'comment_downs': comment_downs,
                        'comment_score': comment_score,
                        'created_utc': comment_creation_utc,
                        'comment_permalink': comment_permalink,
                        'link_title': comment_link_title,
                        #'link_permalink':base_url + link_id_dict[link_id],
                        'submission_body': original_body,
                        'submission_id': original_id,
                        'submission_permalink': original_link,
                        'submission_ups': original_ups,
                        'submission_downs':original_downs,
                        'submission_likes':original_likes,
                        
                        
                        'post_type': 'comment'}
               
                payload_temp = payload
                if payload_temp['submission_body']:
                    payload_temp['submission_body'] = payload_temp['submission_body'][:1000]
                if payload_temp['comment_body']:
                    payload_temp['comment_body'] = payload_temp['comment_body'][:1000]
                df= df.append(payload_temp, ignore_index = True)
                    
                
                if len(df)>=100:
                    df['created_utc'] = pd.to_datetime(df['created_utc'])
                    df = df[pd.notnull(df['comment_body'])]
                    df.reset_index(inplace = True, drop = True)
                    df = df.apply(process_entities, axis = 1)
                    df = df[(df['ticker'] != '[]') & (pd.notnull(df['ticker']))]
                    df.drop_duplicates(inplace = True)
                    posts_df2_filter = df
                    posts_df2_filter.loc[:, 'ticker'] = posts_df2_filter.loc[:,'ticker'].apply(lambda x: literal_eval(x))
                    posts_df2_final_expand = pd.DataFrame({col:np.repeat(posts_df2_filter[col].values, posts_df2_filter['ticker'].str.len()) for col in posts_df2_filter.columns.drop('ticker')}).assign(**{'ticker':np.concatenate(posts_df2_filter['ticker'].values)})
                    ticker_action_ls = []
            
                    for publish_date in list(ticker_action_dict.keys()):
                        ticker_action_df = pd.DataFrame(ticker_action_dict[publish_date]).transpose()
                        ticker_action_df.reset_index( inplace =True)
                        ticker_action_df.rename(columns = {'index':'ticker'}, inplace = True)
                        ticker_action_df['publish_date'] = publish_date
                        ticker_action_ls.append(ticker_action_df)
                    ticker_action_df = pd.concat(ticker_action_ls)   
                    insert_data(posts_df2_final_expand, schema = 'WSTBET', table_name = 'reddit_streaming')
                    insert_data(ticker_action_df, schema = 'WSTBET', table_name = 'ticker_action_streaming')
                    df = pd.DataFrame()
                    posts_df2_filter = None
                    posts_df2_final_expand = None
                    ticker_action_df = None
                    ticker_action_dict = dict()
                    
                
                try:
                    kinesis_client.put_record(StreamName = 'reddit_streaming2', Data = json.dumps(payload), \
                                                             PartitionKey = str(comment.parent()))
                    #print('enter here')
                except (AttributeError, Exception) as e:
                        print (e)
                        pass
        except (AttributeError, Exception) as e:
            print(e)
            pass
        '''
        df = df.append({
             'name': submission.name,
                'body':submission.title,
                'likes':submission.likes,
                'author':submission.author,
                'id': submission.id,
                'permalink': submission.permalink,
                
                'ups': submission.ups,
                'downs': submission.downs,
                'score': submission.score,
                'created_utc': datetime.datetime.fromtimestamp(submission.created_utc),
                
                
                'post_type': 'post'
        }, ignore_index=True)
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            comment_create_time = datetime.datetime.fromtimestamp(comment.created_utc)
            prev_date = datetime.datetime.today()+timedelta(days = -1)
            if comment_create_time >= prev_date:
                df = df.append({
                 'name': comment.name,
                    'body':comment.body,
                    'likes':comment.likes,
                    'author':comment.author,
                    'id': comment.id,
                    'permalink': comment.permalink,
                    
                    'ups': comment.ups,
                    'downs': comment.downs,
                    'score': comment.score,
                    'created_utc': datetime.datetime.fromtimestamp(comment.created_utc),
                    
                    
                    'post_type': 'comment'}, ignore_index = True)
    return df
    '''
def process_entities(row):
    global ticker_action_dict, list_of_tickers
    wordnet_lemmatizer = WordNetLemmatizer()
  
  
    publish_date = row['created_utc']
    publish_date = publish_date.date()
    ticker_ls = []
    full_text = row['comment_body'].strip()
    if row['submission_body']:
        full_text = row['submission_body'].strip() + ' ' + full_text
        
    
    tokenized = word_tokenize(full_text)
    
    tokenized = [token2.strip('()') for token2 in [token1.split(':')[1]  if ':' in token1 else token1 for token1 in tokenized]]
    # lemmatize token and perform part of speech tagging
    original_token = [token for token in tokenized if token]
    pos_tag_tokens =  nltk.pos_tag([wordnet_lemmatizer.lemmatize(token.lower()) for token in tokenized if token])
    
    for token_pos in pos_tag_tokens:  
      # only consider certain part of speech of tokens
      if token_pos[1] in ['NN', 'NNS', 'NNP', 'NNPS']:
        token = original_token[pos_tag_tokens.index(token_pos)]
        if (token == 'RH' and 'restoration hardware' in full_text.lower()) or token =='$RH' or (token == 'RH' and '¢' in full_text) or (token == 'RH' and '$' in full_text) or (token != 'RH' and token in list_of_tickers):
          
          ticker_ls.append(token)
    
          keyword_freq = {'put': 0, 'call':0, 'pump':0, 'dump':0}
          ticker_index = pos_tag_tokens.index(token_pos)
          action = None
          # find posters' action 
          for keyword in keyword_freq.keys():
            if ticker_index +1 < len(pos_tag_tokens):
              # AAPL puts
              if keyword in original_token[ticker_index +1].lower():
                action = keyword
                break
            # puts on AAPL or put AAPL
            if ticker_index -1 >=0:
              if keyword in original_token[ticker_index -1].lower():
                action = keyword
                break
            
            if ticker_index -2 >=0:
              if keyword in original_token[ticker_index -2].lower():
                action = keyword
                break
            
          if publish_date and publish_date in ticker_action_dict.keys():
              
            if token in ticker_action_dict[publish_date].keys():
                if action != None:
                    ticker_action_dict[publish_date][token][action] = ticker_action_dict[publish_date][token][action]+1
                    
                    #print('enter here')
            else:
                ticker_action_dict[publish_date].update({token: keyword_freq})
          else:
             ticker_action_dict.update({publish_date:{token: keyword_freq}})
    
    row['ticker'] = json.dumps(list(set(ticker_ls)))
    return row

def main():
    global reddit
    run_time = datetime.datetime.today()
    get_post_comment()
    

if __name__ == '__main__':
    main() 

