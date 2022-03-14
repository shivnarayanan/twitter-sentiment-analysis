def fetchTweets():
    import tweepy
    from tweepy import OAuthHandler
    import pandas as pd
    import json

    consumer_key = 'VvfQ6PLzJON2w7RiOcyXmKm3O' # Add your API key here
    consumer_secret = 'iWWbwUGBuaoxBHkcnn5u8Vp8IQ8qg7YC6BEmxSa7KpClPYN0dF' # Add your API secret key here
    access_token = '1433222201780563969-PvXqlg9boERAZUgpVnA9I9zDjQvo5X' # Add your Access Token key here
    access_token_secret = 'c4y2surpvnhmFfqYLSPGhBbjBPOXlMYwh3GIh59W66Inn' # Add your Access Token secret key here

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    try:
        api.verify_credentials()
        print("Authentication Successful!")
    except Exception as e:
        print("Error during authentication:", e)

    print("Fetching tweets... ")
    search_words = 'covid'
    max_tweets = 10
    tweets = tweepy.Cursor(api.search, q=search_words, lang="en", tweet_mode='extended').items(max_tweets)

    # for tweet in tweets:
    #     print("----------------------------------------------------")
    #     print('Tweet ID ' + str(tweet.id))
    #     print(f'Tweeted by: @{tweet.user.screen_name}, Created at: {str(tweet.created_at)}, Location: {tweet.user.location}' )
    #     print("Tweet: " + tweet.full_text)

    filename = "/Users/shivnarayanan/Desktop/twitter-airflow/data/tweets.json"

    with open(filename, "w") as output:
        for tweet in tweets:
            myjson = tweet._json
            output.write(json.dumps(myjson)+"\n")

def readTweets():
    import json 
    import pandas as pd

    filename = "/Users/shivnarayanan/Desktop/twitter-airflow/data/tweets.json" 

    dfs = []
    with open(filename) as fi:
        for line_cnt, line in enumerate(fi):
            tweet_json = json.loads(line.strip())
            data = {'id':[tweet_json['id']], 'date':[tweet_json['created_at']], 'content':[tweet_json['full_text']]}
            df = pd.DataFrame.from_dict(data)
            dfs.append(df)   
    
    df = pd.concat(dfs, ignore_index=True)

    filepath = '/Users/shivnarayanan/Desktop/twitter-airflow/data/dataframe.csv'
    df.to_csv(filepath, index=False)
    print("Dataframe created!")

fetchTweets()
readTweets()


