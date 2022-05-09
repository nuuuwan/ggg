import os
import json
import tweepy
from utils import jsonx, logx
log = logx.get_logger('ggg')

MAX_VIDEOS_TO_SCRAPE = 10
DIR_VIDEO_METADATA = 'video_metadata'
DIR_VIDEOS = 'videos'
MIN_FILE_SIZE = 1000

def download_video(remote_url, local_video_file):
    cmd = f'wget -nv -O {local_video_file} {remote_url}'
    os.system(cmd)


def get_video_url_list(tweet):
    if 'extended_entities' not in tweet.__dict__:
        return []

    if 'media' not in tweet.extended_entities:
        return []
    media = tweet.extended_entities['media']

    if not media:
        return []
    video_url_list = []
    for media_i in media:
        if 'video_metadata' in media_i:
            video_url = media_i['video_metadata']['variants'][-1]['url']
            video_url_list.append(video_url)

        if 'video_info' in media_i:
            video_url = media_i['video_info']['variants'][-1]['url']
            video_url_list.append(video_url)

    return video_url_list


def wrote_video_metadata(video_metadata):
    id = video_metadata['id']
    video_metadata_file = os.path.join(DIR_VIDEO_METADATA, f'{id}.json')
    jsonx.write(video_metadata_file, video_metadata)
    print(f'Wrote {video_metadata_file}')


def scrape_metadata():
    consumer_key = os.environ['TWTR2_API_KEY']
    consumer_secret = os.environ['TWTR2_API_SECRET_KEY']

    access_token = os.environ['TWTR2_ACCESS_TOKEN']
    access_token_secret = os.environ['TWTR2_ACCESS_TOKEN_SECRET']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # test_tweet(api)
    # return

    n_videos = 0
    for tweet in tweepy.Cursor(
        api.search,
        q="video #GotaGoGama -filter:retweets",
        count=5,
        since="2022-05-09",
        lang="en",
        tweet_mode='extended'
    ).items():
        user = tweet.user

        video_url_list = get_video_url_list(tweet)
        n_video_url_list = len(video_url_list)
        if n_video_url_list == 0:
            log.debug(f'{tweet.id} - No Videos')
            continue

        log.info(f'{tweet.id} - {n_video_url_list} Videos')

        video_metadata = dict(
            id=tweet.id,
            full_text=tweet.full_text,
            video_url_list=video_url_list,
            created_at=str(tweet.created_at),
            user_id=user.id,
            user_name=user.name,
            user_friends_count=user.friends_count,
            user_followers_count=user.followers_count,
            user_statuses_count=user.statuses_count,
        )
        wrote_video_metadata(video_metadata)
        n_videos += 1

        if n_videos > MAX_VIDEOS_TO_SCRAPE:
            break

def download_videos():
    file_list = list(os.listdir(DIR_VIDEO_METADATA))
    n = len(file_list)
    for i_file, file_only in enumerate(file_list):
        if file_only[-5:] != '.json':
            continue

        i_file1 = i_file + 1
        log.info(f'{i_file1}/{n} Videos')
        video_metadata_file = os.path.join(DIR_VIDEO_METADATA, file_only)
        video_metadata = jsonx.read(video_metadata_file)
        if not video_metadata:
            log.error(f'Could not read {video_metadata_file}')
            continue

        id = video_metadata['id']
        video_file = os.path.join(DIR_VIDEOS, f'{id}-0.mp4')
        if os.path.exists(video_file):
            log.debug(f'{video_file} already exists')
            continue

        video_url_list = video_metadata['video_url_list']
        for i_video, video_url in enumerate(video_url_list):
            video_file = os.path.join(DIR_VIDEOS, f'{id}-{i_video}.mp4')
            download_video(video_url, video_file)
            log.debug(f'Downloaded {video_file}'...)



def test_tweet(api):
    id = 1523622701159825416
    tweet = api.get_status(id, tweet_mode='extended')
    print(json.dumps(tweet._json, indent=2))
    print(get_video_url_list(tweet))


if __name__ == '__main__':
    scrape_metadata()
    download_videos()
