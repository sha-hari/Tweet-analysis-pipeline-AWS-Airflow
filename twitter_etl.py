import tweepy
import pandas as pd  # data processing
import json
import csv
from datetime import datetime
# import s3fs
from io import StringIO
import boto3


def run_twitter_etl():

    #twitter api info
    # access_key = 
    # access_secret =
    # consumer_key =
    # consumer_secret = 

    #twitter authentication
    # auth = tweepy.OAuthHandler(access_key, access_secret)
    # auth.set_access_token(consumer_key, consumer_secret)

    #creating an API object
    # api = tweepy.API(auth)

    #extracting a user's tweets, max of 200, retweets excluded
    # tweets= api.user_timeline(screen_name='@elonmusk',
    #                   #200 max count
    #                   count=200,
    #                   include_rts=False,
    #                   #necessary to keep full text, else only 140 words are shown
    #                   tweet_mode='extended')

    # print(tweets)

    # tweet_list=[]
    # for tweet in tweets:
    #     text=tweet._json["full_text"] #reading json data

    #     refined_tweet= {"user": tweet.user.screen_name,
    #                     'text': text,
    #                     'favorite_count': tweet.favorite_count,
    #                     'retweet_count': tweet.retweet_count,
    #                     'created_at': tweet.created_at}
        
    #     tweet_list.append(refined_tweet)

    # df=pd.DataFrame(tweet_list)
    # df.to_csv("elonmusk_twitter_date.csv")


    # csvdata = csv.read('twitter data example.csv')
    # print(csvdata)
    
    # csv dataset from kaggle
    # with open('tweets2.csv', newline='', encoding="utf8") as csvfile:
    #     tweets = csv.reader(csvfile, delimiter=' ', quotechar='|')
        # csvfile.readlines()
        # for row in tweets:
        #     print(', '.join(row))


    # with open('tweets2.csv', 'r', encoding="utf8") as file:
    #     csv_reader = csv.DictReader(file)
    #     data = [row for row in csv_reader]
    # print(data)


    # numrows = 5000
    data=pd.read_csv('tweets2.csv', delimiter=',') #param to limit rows: nrows=numrows)
    print(data.head(5))

    #filtering data by column of dataframe
    data_filtered = data.query('author=="Cristiano"')
    data_filtered = data_filtered [['author','date_time','content','number_of_likes','number_of_shares']]

    print(data_filtered.head(5))

    data_dict=data.to_dict(orient='records')

    # tweet_list=[]

    # for tweet in data_dict:
    #     print(tweet) 
        
        # refined_tweet= {'author': tweet.author}
        # 'created_at': tweet.date_time,
        # 'country': tweet.country,
        # 'like_count': tweet.number_of_likes,
        # 'share_count': tweet.number_of_shares,
        # 'text': tweet.content}
        # print(refined_tweet)

        # tweet_list.append(refined_tweet)
        # print(tweet_list)

    # df=pd.DataFrame(tweet_list)
    # df.to_csv('tweet_results.csv')
        
        
    # df=pd.DataFrame(data_dict)
    # print(df.head(5))
    # df.to_csv('tweet_results.csv')

    # results=data.to_csv('tweetres.csv')
    # filtered_results=data_filtered.to_csv('cristiano_tweets.csv')
    # filtered_results=data_filtered.to_csv("s3://airflow-tweets-bucket-shash/cristiano_tweets.csv")

    s3 = boto3.client('s3', aws_access_key_id='AKIA6AUWF47CAXEEC7XX', aws_secret_access_key='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')
    # read_file = s3.get_object(Bucket, Key)

    bucket = 'airflow-tweets-bucket-shash' # already created on S3
    csv_buf = StringIO()
    data_filtered.to_csv(csv_buf, header=True, index=False)
    s3_resource = boto3.resource('s3')
    # s3_resource.Object(bucket, 'cristiano_tweets.csv').put(Body=csv_buf.getvalue())
    # s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key='s3://airflow-tweets-bucket-shash/cristiano_tweets.csv')
    s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key='cristiano_tweets.csv')


# run_twitter_etl()