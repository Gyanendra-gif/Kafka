from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

access_token = "963790698402103297-FSx36WAYrEqFwkLsamp0PuFOVeO8fdW"
access_token_secret =  "8hoa165c5Tc1taYZMXi040gwOcvjNDq9JtAIgblZZihdZ"
api_key =  "PzXuyAoOr2Z00HXJJyb0TCd9r"
api_secret =  "jkN5gytODpZE7EZjbvfePfZV3uyPc7YDzTOsGgSI8LiewpQdcU"

class StdOutListener(StreamListener):
    def on_data(self, data):
        json_ = json.loads(data) 
        producer.send('basic', json_["text"].encode('utf-8'))
        return True
    def on_error(self, status):
        print(status)

producer = KafkaProducer(bootstrap_servers='localhost:9092')
l = StdOutListener()
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["covid19", "corona virus"])