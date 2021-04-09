# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 14:59:08 2021

@author: tcai
"""

import webhoseio
import requests
import json
import datetime
from datetime import  timedelta
try:
   from urllib.parse import urlencode, quote_plus
except ImportError:
   from urlparse import urlparse
   from urllib import urlencode
import boto
import boto.s3
import boto3
from boto.exception import S3CreateError
from botocore.exceptions import ClientError
import sys
from boto.s3.key import Key
import re
import matplotlib.pyplot as plt
import os
import boto3
import logging
import s3fs
import pandas as pd
import praw
import pprint

ACCESS_KEY = 'AKIAWYG6AKIP2FMXUUXK'
SECRET_KEY = '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'
fs = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY)

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


def get_post_comment():
    global reddit
    subreddit = reddit.subreddit("wallstreetbets")
    df = pd.DataFrame()
    for submission in subreddit.hot(limit = 10):
        #print(submission.title)  # Output: the submission's title
        #print(submission.score)  # Output: the submission's score
        #print(submission.id)     # Output: the submission's ID
        #print(submission.url)
        #print(datetime.fromtimestamp(submission.created_utc))
        #pprint.pprint(vars(submission))
        
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

def main():
    global reddit
    run_time = datetime.datetime.today()
    df = get_post_comment()
    df.drop_duplicates(inplace = True)

    prev_date = pd.Timestamp('today').normalize()+timedelta(days = -1)
    df = df[df['created_utc']>=prev_date]
    df.reset_index(inplace = True, drop = True)
    file_name = run_date+ '_' + '-' + 'reddit_wstbet_api' + '.csv'
    with open(file_name, 'w') as fp:
        df.to_csv(file_name)
	
    # spark dbfs cannot read ':' directory, need to replace it
    put_file_s3(bucket_name = 'reddit-wstbet', local_dir = './'+ file_name, result_dict = None, bucket_folder = 'webhose/'+str(run_time).replace(' ', '-').replace(':', '-'))
    
    # remove local json file at the end
    os.remove('./'+ file_name)

if __name__ == '__main__':
    main() 

