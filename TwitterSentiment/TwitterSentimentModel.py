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

import warnings
warnings.filterwarnings("ignore")
import pickle


train  = pd.read_csv("train_E6oV3lV.csv")
# test = pd.read_csv("twitter-sentiment-analysis/test_tweets_anuFYb8.csv")
train.sample(2)







#Data cleaning and Preprocessing

# Combine train and test dataset for pre processing. Here we'll remove all the unnecessary contents from the data. This will help to increase the accuracy.



# df = train.append(test, ignore_index = True)


train['cleaned_tweet'] = train.tweet.apply(lambda x: ' '.join([word for word in x.split() if not word.startswith('@')]))
# test['cleaned_tweet'] = test.tweet.apply(lambda x: ' '.join([word for word in x.split() if not word.startswith('@')]))


# ### Hashtags
# Graph to show normal tweets

# In[19]:

#Select all words from normal tweet
normal_words = ' '.join([word for word in train['cleaned_tweet'][train['label'] == 0]])



#Repeat same steps for negative tweets
negative_words = ' '.join([word for word in train['cleaned_tweet'][train['label'] == 1]])




# Words used like love, friend, happy are used in normal tweets whereas negative can be found in words like trump, black, politics etc.
train.head(2)


# Rescale data using CountVectorizer
# CountVectorizer
# Itll see the unique words in the complete para or content given to it and then does one hot encoding accordingly


# Split data into train and test dataset

X_train, X_val, y_train, y_val = train_test_split(train['cleaned_tweet'], train['label'], random_state = 0)
X_train.shape, X_val.shape


# Train the model

cv = CountVectorizer()
vect = cv.fit(X_train)
X_train_vectorized = cv.transform(X_train)


#Logistic Regression

logistic_model_cv = LogisticRegression()
logistic_model_cv.fit(X_train_vectorized, y_train)
pred = logistic_model_cv.predict(vect.transform(X_val))
print('F1 :', f1_score(y_val, pred))


# Logistic Regression performed well then Naive Bayes for the default parameters. Thus, we will be using only Logistic Regression ahead.


# As logistic_model_cv gave us highest accuracy we'll go with it and save it to pickle file


with open('model.pkl','wb') as file:
    pickle.dump(logistic_model_cv, file)
with open('prep.pkl','wb') as file:
    pickle.dump(cv, file)





