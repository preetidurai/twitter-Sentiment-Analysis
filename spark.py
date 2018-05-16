from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import operator
from elasticsearch import Elasticsearch
import queue
import traceback
from datetime import datetime
import time



def getSentiment(tweet):
    toBeProcessed=tweet['text'];
    sid = SentimentIntensityAnalyzer()
    ss = sid.polarity_scores(toBeProcessed)
    del ss['compound'];    
    del ss['neu'];
    sentiment=( max(ss.items(), key=operator.itemgetter(1))[0]) ;
    tweet['sentiment']=sentiment;
    return(tweet);
    
def get_json(myjson):
    jsonOb=json.loads(myjson);
    return(jsonOb)
    
    
def removeEmojis(post):
    toBeProcessed=post['text'];
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE);
    textValue= emoji_pattern.sub(r'', toBeProcessed);
    post['text']=textValue;
    return(post);


def cleanTweet(tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        toBeProcessed=tweet['text'];
        replacement= ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", toBeProcessed).split());
        tweet['text']=replacement;
        return(tweet);

def sendToES(partition):
    tweets = list(partition)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200,'index_type':'tweet'}]);
    if(es.indices.exists(index = "preeti")):
        if(len(tweets) != 0):
            for tweet in tweets:     
                if(tweet['location'] is not None):
                    doc = {
                            "text": tweet['text'],
                            "location3": {
                                    "lat": tweet['location'][0],
                                    "lon": tweet['location'][1]
                                    },
                            "sentiment":tweet['sentiment'],
                            "timestamp":int(round(time.time() * 1000))
                    }
                    es.index(index="preeti", doc_type='request-info', body=doc);
    else:
        mappings={
                "mappings":{
                        "request-info":{
                                "properties":{
                                        "text":{
                                                "type":"keyword"},
                                        "location3":{
                                                "type":"geo_point"},
                                        "sentiment":{
                                                "type":"keyword"},
                                        "timestamp":{
                                                "type":"date",
                                                "format":"epoch_millis"
                                                
                                                }
                                       }}}
                }
        es.indices.create(index='preeti', body=mappings);
        if(len(tweets) != 0):
            for tweet in tweets:                
                doc = {
                    "text": tweet['text'],
                    "location3": {
                            "lat": tweet['location'][0],
                            "lon": tweet['location'][1]
                            },
                    "sentiment":tweet['sentiment'],
                    "timestamp":int(round(time.time() * 1000))
                                       
                    }
                es.index(index="preeti", doc_type='request-info', body=doc)
    

TCP_IP = 'localhost'
TCP_PORT = 9001

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')
# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
print(type(dataStream))

dstream_tweets=dataStream.map(lambda post: get_json(post)).map(lambda y:removeEmojis(y)).map(lambda z:cleanTweet(z))
sentimentTweets=dstream_tweets.map(lambda x: getSentiment(x));
c=sentimentTweets.foreachRDD(lambda x:x.foreachPartition(lambda y:sendToES(y)))
sentimentTweets.pprint()


ssc.start()
ssc.awaitTermination()
