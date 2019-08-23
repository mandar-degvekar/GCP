import tweepy as tw
import pandas as pd

#declaring consumer and api keys

consumer_key= 'R0K1bKj3D8NydFC0mwTngVyCI'
consumer_secret= 'ZIbXBYEauHxpitMAV4oKd95efOwszISslhSMP65uFDCrMs4Bd5'
access_token= '708708313664397312-1gW6cuUrogkSKN9FbKLM78BzUwOjx6D'
access_token_secret= 'IemJ9fiJZn8tc9IQVlkeYaWgKlq41WLqrwCd2aSK81REg'

#autentication
auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

#testing by tweeting
# api.update_status("Look, I'm tweeting from #Python")

#  Define the search term and the date_since date as variables
search_words = "#RohitPatadeTestingTwitterPy"
date_since = "2019-08-22"

tweets = tw.Cursor(api.search,
              q=search_words,
              lang="en",
              since=date_since).items(10)

# Iterate on tweets
for tweet in tweets:
    print(tweet.text)