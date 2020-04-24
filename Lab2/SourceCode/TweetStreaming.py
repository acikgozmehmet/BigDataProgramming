import json
import socket

import tweepy
from tweepy import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler
import twitter_credentials


class TweetListener(StreamListener):
    def __init__(self, socket):
        print("Listener initialized")
        self.client_socket = socket

    def on_data(self, data):
        try:
            jsonmessage = json.loads(data)
            message = jsonmessage["text"].encode("utf-8")
            print(str(message))
            self.client_socket.send(message)
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return False

def connect_to_twitter(connection, tracks):
    auth = OAuthHandler(twitter_credentials.api_key, twitter_credentials.api_secret)
    auth.set_access_token(twitter_credentials.access_token, twitter_credentials.access_token_secret)
    tweeter_stream = Stream(auth, TweetListener(connection))
    tweeter_stream.filter(track=tracks, languages=["en"])


if __name__ == "__main__":

    host = "localhost"
    port = 9999
    tracks ="coronavirus covid"
    # tracks =[""]

    s = socket.socket()
    s.bind((host, port))
    print("Listening on port: %s" % str(port))

    s.listen(5)
    connection, client_address = s.accept()

    print( "Received request from: " + str(client_address))
    print("Initializing listener for these tracks: ", tracks)

    connect_to_twitter(connection, tracks)

