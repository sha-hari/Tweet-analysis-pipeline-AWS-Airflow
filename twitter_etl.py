# import tweepy
import pandas as pd
import json
import csv
from datetime import datetime
# import s3fs
from io import StringIO
import boto3


def run_twitter_etl():
        
    # csv dataset from kaggle
    # numrows = 5000
    data=pd.read_csv('tweets2.csv', delimiter=',') #param to limit rows: nrows=numrows)
    print(data.head(5))

    #filtering data by column of dataframe
    data_filtered = data.query('author=="Cristiano"') #filtering data to one user
    data_filtered = data_filtered [['author','date_time','content','number_of_likes','number_of_shares']]

    print(data_filtered.head(5))

    data_dict=data.to_dict(orient='records')

    # filtered_results=data_filtered.to_csv('cristiano_tweets.csv') #testing results locally

    s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='')
    # read_file = s3.get_object(Bucket, Key)

    bucket = 'airflow-tweets-bucket-shash' # already created on s3
    csv_buf = StringIO()
    data_filtered.to_csv(csv_buf, header=True, index=False)
    s3_resource = boto3.resource('s3')
    s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key='cristiano_tweets.csv') # Writing to s3


# run_twitter_etl()
