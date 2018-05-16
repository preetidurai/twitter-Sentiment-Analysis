# -*- coding: utf-8 -*-

import json 
import tweepy
import socket
import time
import geocoder
import sys

ACCESS_TOKEN = 'MY_TOKEN'
ACCESS_SECRET = 'MY_SECRET'
CONSUMER_KEY = 'MY_KEY'
CONSUMER_SECRET = 'MY_SECRET'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#'+sys.argv[1];
print(hashtag)

TCP_IP = 'localhost'
TCP_PORT = 9001

print("Reached here");
# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        #print(status.text)
        
        myDict={'text':status.text,'location':status.user.location}
        if(status.user.location is not None):
            g = geocoder.google(status.user.location);
            print(g.latlng) 
            print(type(g.latlng));
            myDict={'text':status.text,'location':g.latlng}
            toSend=json.dumps(myDict)
            toSend=toSend+"\n";
            print(toSend);
            toSend=toSend.encode('utf-8')
            conn.send(toSend)
            time.sleep(0.3)
            
        
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

myStream.filter(track=[hashtag])
