import os
import json
import tweepy
from utils import jsonx

TEST_TWEET_ID = '1523569146134360064'

MAX_VIDEOS_TO_SCRAPE = 3


def download_video(remote_url):
    local_video_file = 'test.mp4'
    cmd = f'wget -O {local_video_file} {remote_url}'
    os.system(cmd)
    print(f'Downloaded "{remote_url}" to {local_video_file}')


def get_video_url_list(tweet):
    if 'extended_entities' not in tweet.__dict__:
        return None
    if 'media' not in tweet.extended_entities:
        return None
    media = tweet.extended_entities['media']
    if not media:
        return None
    video_url_list = []
    for media_i in media:
        if 'video_info' in media_i:
            video_url = media_i['video_info']['variants'][-1]['url']
            video_url_list.append(video_url)
    return video_url_list

def write_video_info(video_info):
    id = video_info['id']
    json_file = f'video_metadata/{id}.json'
    jsonx.write(json_file, video_info)
    print(f'Wrote {json_file}')

def main():
    consumer_key = os.environ['TWTR2_API_KEY']
    consumer_secret = os.environ['TWTR2_API_SECRET_KEY']

    access_token = os.environ['TWTR2_ACCESS_TOKEN']
    access_token_secret = os.environ['TWTR2_ACCESS_TOKEN_SECRET']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    video_info_list = []

    # test_tweet = api.get_status(1523611161232494593, tweet_mode='extended')
    n_videos = 0
    for tweet in tweepy.Cursor(
        api.search,
        q="video #GotaGoGama -filter:retweets",
        count=5,
        since="2022-05-09",
        lang="en",
        tweet_mode='extended'
    ).items():
        print(tweet.id)
        video_url_list = get_video_url_list(tweet)
        if not video_url_list:
            continue
        video_info = dict(
            id=tweet.id,
            full_text=tweet.full_text,
            video_url_list=video_url_list,
        )
        write_video_info(video_info)
        n_videos += 1

        if n_videos > MAX_VIDEOS_TO_SCRAPE:
            break





if __name__ == '__main__':
    main()
