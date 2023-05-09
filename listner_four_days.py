#four_days_script

import requests
import os
import json
import pandas as pd
import csv
from datetime import *
import dateutil.parser
import unicodedata
import time
import socket

os.environ['TOKEN'] = "AAAAAAAAAAAAAAAAAAAAAIdCcgEAAAAADxqlmxSiZLO05fKmfbrX7G3ckqQ%3DCCPSGoWTDF6uu4qdFsncsuOat5GFTTFv5blXPdA4ueK4YLu3gg"

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results=10):
    search_url = "https://api.twitter.com/2/tweets/search/recent"
    query_params = {
        'query': keyword,
        'start_time': start_date,
        'end_time': end_date,
        'max_results': max_results,
        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
        'place.fields': 'full_name,id,country,country_code,geo,name,place_type'
    }
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", url, headers=headers, params=params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "Sudan lang:en"
max_results = 100
# Set the end time to the current time minus one minute
end_time = (datetime.utcnow() - timedelta(minutes=1)).replace(microsecond=0).isoformat() + 'Z'

# Set the start time to 4 days before the end time
start_time = (datetime.utcnow() - timedelta(days=4)).replace(microsecond=0).isoformat() + 'Z'

# Update the URL with the new start and end times
url = create_url(keyword, start_time, end_time, max_results)


s = socket.socket()
host = "127.0.0.1"
port = 7770
s.bind((host, port))
print("Listening on port: %s" % str(port))
s.listen(5)
clientsocket, address = s.accept()

next_token = None
while True:
    # Add the next_token parameter to the query params if it exists
    if next_token is not None:
        url[1]['next_token'] = next_token
    # Make the API request and get the JSON response
    json_response = connect_to_endpoint(url[0], headers, url[1])
    # Extract the tweet and user data from the response
    for tweet in json_response['data']:
        user_id = tweet['author_id']
        user_data = next((user for user in json_response['includes']['users'] if user['id'] == user_id), None)
        if user_data is None:
            continue
        filtered_data = {
            'text': tweet['text'],
            'id': tweet['id'],
            'retweet_count': tweet['public_metrics']['retweet_count'],
            'reply_count': tweet['public_metrics']['reply_count'],
            'like_count': tweet['public_metrics']['like_count'],
            'quote_count': tweet['public_metrics']['quote_count'],
            'created_at': tweet['created_at'],
            'author_id':tweet['author_id'],
            'name': user_data['name'],
            'username': user_data['username'],
            'followers_count': user_data['public_metrics']['followers_count'],
            'following_count': user_data['public_metrics']['following_count'],
            'tweet_count': user_data['public_metrics']['tweet_count'],
            'listed_count': user_data['public_metrics']['listed_count'],
            'verified': user_data['verified']
        }
        # Convert the filtered data to JSON and send it over the socket
        json_data = json.dumps(filtered_data)
        clientsocket.send((json_data + '\n').encode('utf-8'))
    # Check if there are more results available
    if 'next_token' in json_response['meta']:
        next_token = json_response['meta']['next_token']
    else:
        break
    # Introduce a delay to avoid hitting the rate limit
    time.sleep(5)

# Close the socket connection when the loop is finished
clientsocket.close()







