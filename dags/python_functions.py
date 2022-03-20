def fetchTweets(**kwargs):
    import os
    import tweepy
    from tweepy import OAuthHandler
    import pandas as pd
    import json
    import config

    consumer_key = config.consumer_key
    consumer_secret = config.consumer_secret
    access_token = config.access_token
    access_token_secret = config.access_token_secret 

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
    max_tweets = 5
    tweets = tweepy.Cursor(api.search_tweets, q=search_words, lang="en", tweet_mode='extended').items(max_tweets)

    myPath = "/Users/shivnarayanan/Desktop/airflow-twitter-extraction/"
    dockerPath = "/usr/local/airflow/"
    filename = dockerPath +"data/tweets_{}.json".format(kwargs['todayDate'])

    with open(filename, "w") as output:
        for tweet in tweets:
            myjson = tweet._json
            output.write(json.dumps(myjson)+"\n")

def readTweets(**kwargs):
    import os 
    import json 
    import pandas as pd
    
    myPath = "/Users/shivnarayanan/Desktop/airflow-twitter-extraction/"
    dockerPath = "/usr/local/airflow/"
    filename = dockerPath +"data/tweets_{}.json".format(kwargs['todayDate'])

    dfs = []
    with open(filename) as fi:
        for line_cnt, line in enumerate(fi):
            tweet_json = json.loads(line.strip())
            data = {'id':[tweet_json['id']], 'date':[tweet_json['created_at']], 'content':[tweet_json['full_text']]}
            df = pd.DataFrame.from_dict(data)
            dfs.append(df)   
    
    df = pd.concat(dfs, ignore_index=True)

    filepath = dockerPath +"data/tweets_dataframe_{}.json".format(kwargs['todayDate'])
    df.to_csv(filepath, index=False)
    print("Dataframe created!")