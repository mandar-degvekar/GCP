import argparse

import pandas as pd
import requests
import json
import time

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

# create_topic('lofty-shine-248403','PredTest')

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
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=1min&outputsize=compact&apikey=0D8RR9NDGU7URNQT"
    response = requests.get(url)
    rdata = json.loads(response.text)
    ndata=str(rdata)
    # print (rdata)

    data = format(ndata)
    # Data must be a bytestring
    data = data.encode('utf-8')

    future = publisher.publish(topic_path, data=data)
    print(future.result())
    print('Published messages.')




publish_messages('lofty-shine-248403','Pred')




