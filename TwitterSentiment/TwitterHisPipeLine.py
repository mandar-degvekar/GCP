from nltk.corpus import twitter_samples
from nltk.tokenize import TweetTokenizer
import string
import re
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import TweetTokenizer
from random import shuffle
from nltk import classify
from nltk import NaiveBayesClassifier
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import nltk
from wordcloud import WordCloud,STOPWORDS
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, ENGLISH_STOP_WORDS
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.pipeline import make_pipeline
import apache_beam as beam
import numpy as np
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions,SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import warnings
warnings.filterwarnings("ignore")
import pickle

dataflow_options = ['--project=lofty-shine-248403', '--job_name=newjob', '--temp_location=gs://testing-gcp-mandar/temp']
dataflow_options.append('--staging_location=gs://testing-gcp-mandar/staging')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

# Dataflow runner
options.view_as(StandardOptions).runner = 'DirectRunner'
options.view_as(SetupOptions).save_main_session = True
options.view_as(StandardOptions).streaming = True

table_schema = {'fields': [
    {'name': 'Tweet', 'type': 'STRING'},
    {'name': 'Sentiment', 'type': 'STRING'}
]}

class Preprocess(beam.DoFn):


    def process(self, element):
        # vs = [v for v in element[1]]
        print (element)
        # df = pd.DataFrame(vs)
        # df = df.iloc[:, 0:2]
        # df.columns = ['id', 'tweet']
        # df['tweet'] = df.tweet.apply(lambda x: ' '.join([word for word in x.split() if not word.startswith('@')]))
        # df['tweet'] = df.tweet.apply(lambda x: ' '.join([word for word in x.split() if not word.startswith('#')]))
        # df['tweet'] = df.tweet.apply(lambda x: ''.join(e for e in string if e.isalnum()))
        # return df['tweet']

class Model(beam.DoFn):
    def process(self,element):
        print(element)
        # x = element['tweet'].values
        pickle_in = open("dict.pickle", "rb")
        classifier = pickle.load(pickle_in)
        preproc = pickle.load(pickle_in)
        print ('nnananakankankanakankakankankaknkan')
        X_train_vectorized = preproc.transform(np.array(element))
        preds=classifier.predict(np.array(element))
        l1 = []
        i = 0
        print('hswhahahahahahhahahahahahahahhahh')
        for val in preds:
            temp = {}
            temp['Tweet'] = str(element)
            temp['Sentiment'] = str(preds[i]).replace('[', '').replace(']', '')
            l1.append(temp)
            i = i + 1
        print(l1)
        return l1

p = beam.Pipeline(options=options)

lines = \
    (p | 'Read Data From PubSub' >> beam.io.ReadFromText('test_tweets_anuFYb8.csv', skip_header_lines=1)
       | 'preprocess' >> beam.ParDo(Preprocess())
    # | "splitter using beam.map" >> beam.Map(lambda rec:(rec.split(',')))
    # | 'print' >> beam.ParDo(Preprocess())
    # | 'Map record to 1' >> beam.Map(lambda record: ('aa',record))
    # | 'Group by data' >> beam.GroupByKey()
    # | 'preprocess' >> beam.ParDo(Preprocess())
    # | 'Build Model' >> beam.ParDo(Model())
    # |beam.io.WriteToBigQuery(
    #         schema=table_schema,
    #         table="lofty-shine-248403:my_new_datasset.TweetHisSentiment")
    )

p.run()



