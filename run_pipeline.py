import os

import tweepy
from utils import jsonx, logx, timex, tsv

log = logx.get_logger('ggg')

MAX_VIDEOS_TO_SCRAPE = 100
MAX_DOWNLOADS_PER_ATTEMPT = 10
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

    n_scrapes = 0
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
        n_scrapes += 1
        if n_scrapes >= MAX_VIDEOS_TO_SCRAPE:
            break


def download_videos():
    file_list = list(os.listdir(DIR_VIDEO_METADATA))
    n = len(file_list)
    n_downloads = 0
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
        video0_file = os.path.join(DIR_VIDEOS, f'{id}-0.mp4')
        if os.path.exists(video0_file):
            log.debug(f'{video0_file} already exists')
            continue

        video_url_list = video_metadata['video_url_list']
        for i_video, video_url in enumerate(video_url_list):
            video_file = os.path.join(DIR_VIDEOS, f'{id}-{i_video}.mp4')
            n_downloads += 1
            download_video(video_url, video_file)
            log.debug(f'Downloaded {video_file}')

            if n_downloads >= MAX_DOWNLOADS_PER_ATTEMPT:
                return


def write_summmary():
    file_list = list(os.listdir(DIR_VIDEO_METADATA))
    video_metadata_list = []
    for file_only in file_list:
        if file_only[-5:] != '.json':
            continue
        video_metadata_file = os.path.join(DIR_VIDEO_METADATA, file_only)
        video_metadata = jsonx.read(video_metadata_file)
        video0_file = os.path.join(DIR_VIDEOS, f'{id}-0.mp4')
        video_metadata['video_downloaded'] = os.path.exists(video0_file)
        video_metadata_list.append(video_metadata)

    video_metadata_list_file = 'video_metadata_list.tsv'
    n_video_metadata_list = len(video_metadata_list)
    tsv.write(video_metadata_list_file, video_metadata_list)
    print(f'Wrote {n_video_metadata_list} to {video_metadata_list_file}')


if __name__ == '__main__':

    scrape_metadata()

    time_id = timex.get_time_id()
    os.system('git add .')
    os.system(
        f'git commit -m "[run_pipeline][scrape_metadata] {time_id}"',
    )

    download_videos()

    time_id = timex.get_time_id()
    os.system('git add .')
    os.system(
        f'git commit -m "[run_pipeline][download_videos] {time_id}"',
    )

    write_summmary()
    os.system('git add .')
    os.system(
        f'git commit -m "[run_pipeline][write_summmary] {time_id}"',
    )

    os.system('git push origin master')
