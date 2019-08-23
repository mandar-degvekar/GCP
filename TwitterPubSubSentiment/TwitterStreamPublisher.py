from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json

consumer_key = 'N2K1xAor3sdpgUeOkL5et1e2e'
consumer_secret = 't2w0HT3TYywiCCHfz5ND3Sao34ZhfaAOYgnYaBJH3Jz9LPVxco'
access_token = '565316406-CYqHPLL4KSrkd5bIGQ1E50D6OM25tNbw01vam26e'
access_token_secret = 'O6Z24kmT4jsFySPbMjd3uxlfL3gjEF17UX2uqGciclJOt'

PUBSUB_TOPIC = 'TweetPub'
NUM_RETRIES = 3
project_id='lofty-shine-248403'






class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """
    count = 0
    twstring = ''
    tweets = []
    batch_size = 50
    total_tweets = 10000000




    def on_data(self, data):
        from google.cloud import pubsub_v1
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, PUBSUB_TOPIC)
        json_load = json.loads(data)
        texts = json_load['text']
        if not json_load['retweeted'] and 'RT @' not in json_load['text']:
            coded = texts.encode('utf-8')
            print(texts)
            publisher.publish(topic_path, data=coded)
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    print '....'
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)
    stream.filter(track=['follow','bigdata','justin timberlake'],languages=['en'])