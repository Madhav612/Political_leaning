import pandas as pd
import numpy as np
import tweepy as tw
import json,requests
import datetime
import re
import time
from urllib.request import urlopen
import emoji

consumer_key = 'XXXXXXXXXXXXXXXXXXXXXXXXXXX'
consumer_secret = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
access_token = 'XXXXXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
access_token_secret='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


from pymongo import MongoClient


client = MongoClient("connection _string",27017)
political_leaning = client.political_leaning
leader_tweets = political_leaning.leader_tweets

conn = psycopg2.connect(
   database="postgres", user='user', password='XXXXXXXXXXX', host='127.0.0.1', port= '5432'
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
cursor = conn.cursor()

checker_table = 'tblPoliticalLeaning'
table_name = 'tblUserInfo'

def give_emoji_free_text(text):
    return emoji.get_emoji_regexp().sub(r'', text.decode('utf8'))

select_query = 'SELECT * FROM '+table_name+' WHERE fetched_or_not=false LIMIT 10'
cursor.execute(select_query)
rows = cursor.fetchall()

then=time.time()
break_flag=False

for row in rows:
    try:
        main_user_info = api.get_user(user_id=row[2])
        if main_user_info.protected==False:
            for follower in tw.Cursor(api.friends_ids,row[2]).items():
                now=time.time()
                if now-then<82800:
                    select_query = 'SELECT * FROM '+table_name+' WHERE UserId='+str(follower)#to check if user is already added in the database or not
                    cursor.execute(select_query)
                    result = cursor.fetchall()
                    if result==[]:
                        tweets_list = []
                        user_info = api.get_user(user_id=follower)
                        insert_query1 = "INSERT INTO "+table_name+" VALUES('"+re.sub("'","''",user_info.name)+"','"+user_info.screen_name+"',"+str(follower)+",false)"
                        cursor.execute(insert_query1)
                        conn.commit()
                        if user_info.protected==False:
                            for status in tw.Cursor(api.user_timeline,user_id=follower,tweet_mode='extended').items(100):
                                if status.lang=='en':
                                    temp = give_emoji_free_text(status.full_text.encode('utf8','strict'))
                                    temp = ' '.join(x for x in temp.split() if not x.startswith('#'))
                                    temp = ' '.join(x for x in temp.split() if not x.startswith('@'))
                                    temp = ' '.join(x for x in temp.split() if not x.startswith('http'))
                                    tweets_list.append(temp)
                            if len(tweets_list)>40:
                                rec = {
                                    'UserId':follower,
                                    'Tweets':tweets_list
                                }
                                leader_tweets.insert_one(rec)
                else:
                    break_flag=True
                    break    
        if break_flag:
            break
        update_query = 'UPDATE '+table_name+' SET fetched_or_not=true WHERE UserId='+str(row[2])
        cursor.execute(update_query)
        conn.commit()
    except tw.RateLimitError:
        time.sleep(15*60)
    except:
        pass

cursor.close()
conn.close()
client.close()
