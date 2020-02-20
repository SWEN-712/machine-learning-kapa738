# Databricks notebook source
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Cursor
consumer_key = "KYQSilhav4tcapxi9EmzIAovy" #twitter app’s API Key
consumer_secret = "VDudCVzWBpjDPMqmxaywYToJYvzRhUgD8wOsfx2U62JrxPDz0w" #twitter app’s API secret Key
access_token = "1227976033036652550-nHZQYHKhhDbYZJxy7rqRuB4M1gcnG0" #twitter app’s Access token
access_token_secret = "OGLHUWeswhy5OWWtHxElqGmYz2pb3YIVJaBIox6IglsI1" #twitter app’s access token secret

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
auth_api = API(auth)

trump_tweets = auth_api.user_timeline(screen_name = 'Jennison', count = 1000, include_rts = False, tweet_mode = 'extended')

final_tweets = [each_tweet.full_text for each_tweet in trump_tweets]

with open('/dbfs/a11y_tweets.txt', 'w') as f:
  for item in final_tweets:
    f.write("%s\n" % item)

read_tweets = []
with open('/dbfs/a11y_tweets.txt','r') as f:
  read_tweets.append(f.read())
  

# COMMAND ----------

for x in read_tweets:
  print (x)

# COMMAND ----------


