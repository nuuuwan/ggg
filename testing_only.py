import json


def test_tweet(api):
    id = 1523622701159825416
    tweet = api.get_status(id, tweet_mode='extended')
    print(json.dumps(tweet._json, indent=2))
    print(get_video_url_list(tweet))
