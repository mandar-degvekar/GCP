from google.cloud import pubsub_v1

def receive_messages(project_id, subscription_name):
    output = ''
    data_tosendto_bg=[]
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    print('Listening for messages on {}'.format(subscription_path))
    response = subscriber.pull(subscription_path, max_messages=5)

    for msg in response.received_messages:
        output = str(msg.message.data)
        data = eval(output)

    # print("Received message:", data['Time Series (1min)'].items())
    for item in data['Time Series (1min)'].items():
        d = item[1]
        d['date'] = item[0]
        data_tosendto_bg.append(d)
    print(data_tosendto_bg)

receive_messages('lofty-shine-248403','SubPred')

