import tweepy as tw
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json

def fetch_tweets():
    # declaring consumer and api keys

    consumer_key = 'N2K1xAor3sdpgUeOkL5et1e2e'
    consumer_secret = 't2w0HT3TYywiCCHfz5ND3Sao34ZhfaAOYgnYaBJH3Jz9LPVxco'
    access_token = '565316406-CYqHPLL4KSrkd5bIGQ1E50D6OM25tNbw01vam26e '
    access_token_secret = 'O6Z24kmT4jsFySPbMjd3uxlfL3gjEF17UX2uqGciclJOt'

    #  Define the search term and the date_since date as variables
    search_words = "#wild  -filter:retweets"
    date_since = "2019-08-16"

    class listener(StreamListener):

        def on_data(self, data):
            print(data)
            return (True)

        def on_error(self, status):
            print status

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    twitterStream = Stream(auth, listener())
    print (twitterStream.filter(track=["wild"]))





def create_topic(project_id, topic_name):
    """Create a new Pub/Sub topic."""
    # [START pubsub_quickstart_create_topic]
    # [START pubsub_create_topic]
    from google.cloud import pubsub_v1

    # TODO project_id = "Your Google Cloud Project ID"
    # TODO topic_name = "Your Pub/Sub topic name"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    topic = publisher.create_topic(topic_path)

    print('Topic created: {}'.format(topic))
    # [END pubsub_quickstart_create_topic]
    # [END pubsub_create_topic]

# create_topic('lofty-shine-248403','TweetPub')

def publish_messages(project_id, topic_name):
    """Publishes multiple messages to a Pub/Sub topic."""
    # [START pubsub_quickstart_publisher]
    # [START pubsub_publish]
    from google.cloud import pubsub_v1

    # TODO project_id = "Your Google Cloud Project ID"
    # TODO topic_name = "Your Pub/Sub topic name"

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_name}`
    topic_path = publisher.topic_path(project_id, topic_name)
    fetch_tweets()
    # for tweet in tweets:
    #     rdata = json.loads(tweet.text)
    #     ndata = str(rdata)
    #     print (rdata)
    #

    # response=fetch_tweets()
    # rdata = json.loads(fetch_tweets())
    # ndata = str(rdata)
    # # print (rdata)

    # data = format(ndata)
    # # Data must be a bytestring
    # data = data.encode('utf-8')
    # print (data)

    # future = publisher.publish(topic_path, data=data)
    # print(future.result())
    # print('Published messages.')

publish_messages('lofty-shine-248403','TweetPub')
