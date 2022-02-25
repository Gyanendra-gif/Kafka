# from tweepy import StreamListener
# from tweepy import OAuthHandler
# from tweepy import Stream
# from kafka import KafkaProducer
# import json

# access_token = "963790698402103297-FSx36WAYrEqFwkLsamp0PuFOVeO8fdW"
# access_token_secret =  "8hoa165c5Tc1taYZMXi040gwOcvjNDq9JtAIgblZZihdZ"
# api_key =  "PzXuyAoOr2Z00HXJJyb0TCd9r"
# api_secret =  "jkN5gytODpZE7EZjbvfePfZV3uyPc7YDzTOsGgSI8LiewpQdcU"

# class StdOutListener(StreamListener):
#     def on_data(self, data):
#         json_ = json.loads(data) 
#         producer.send('basic', json_["text"].encode('utf-8'))
#         return True
#     def on_error(self, status):
#         print(status)

# producer = KafkaProducer(bootstrap_servers='localhost:9092')
# l = StdOutListener()
# auth = OAuthHandler(api_key, api_secret)
# auth.set_access_token(access_token, access_token_secret)
# stream = Stream(auth, l)
# stream.filter(track=["covid19", "corona virus"])

import csv
import requests
from kafka import KafkaProducer
from json import dumps
 
# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=15min&slice=year1month1&apikey=BMKUL0QPLRBAX5L4'
 
with requests.Session() as s:
   download = s.get(CSV_URL)
   decoded_content = download.content.decode('utf-8')
   cr = csv.reader(decoded_content.splitlines(), delimiter=',')
   my_list = list(cr)
   producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda K:dumps(K).encode('utf-8'))
   for row in my_list:
       producer.send('basic',row)