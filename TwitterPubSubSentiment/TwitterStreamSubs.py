import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

dataflow_options = ['--project=lofty-shine-248403', '--job_name=newjob', '--temp_location=gs://testing-gcp-mandar/temp']
dataflow_options.append('--staging_location=gs://testing-gcp-mandar/staging')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

# Dataflow runner
options.view_as(StandardOptions).runner = 'Directrunner'
# options.view_as(SetupOptions).save_main_session = True
options.view_as(StandardOptions).streaming = True

table_schema = {'fields': [
    {'name': 'Tweet', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Sentiment', 'type': 'STRING', 'mode': 'NULLABLE'}
]}

def MLmodel(data):
    import pickle
    import numpy as np
    from google.cloud import storage

    storage_client = storage.Client()
    bucket = storage_client.get_bucket("testing-gcp-mandar")

    blob = bucket.blob("model.pkl")
    model_local = "TwitterSA_model.pkl"
    blob.download_to_filename(model_local)
    pickle_in = open("TwitterSA_model.pkl", "rb")
    model = pickle.load(pickle_in)

    blob = bucket.blob("prep.pkl")
    model_local = "TwitterSA_prep.pkl"
    blob.download_to_filename(model_local)
    pickle_prep = open("TwitterSA_prep.pkl", "rb")
    prep = pickle.load(pickle_prep)

    # print tweet
    ntweet = [data]
    x = prep.transform(ntweet)
    pred = model.predict(x)
    l1 = []
    temp = {}
    temp['Tweet'] = str(data)
    temp['Sentiment'] = str(pred).replace('[', '').replace(']', '')
    l1.append(temp)
    print(l1)

p = beam.Pipeline(options=options)

lines = \
    (p | 'Read Data From PubSub' >> beam.io.ReadFromPubSub(subscription='projects/lofty-shine-248403/subscriptions/TweetSub').with_output_types(bytes)
    # | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
    | 'predict' >> beam.ParDo(MLmodel)
    | 'storing in bigQ' >> beam.io.WriteToBigQuery(
                schema=table_schema,
                table="lofty-shine-248403:my_new_datasset.TweetLiveSentiment")

    )

p.run().wait_until_finish()