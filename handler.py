import json
import logging
import os
import sys
import time
from datetime import datetime

import requests

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'DEBUG'))


def fatal(msg=None):
    if msg:
        logger.fatal(msg)
    sys.exit(1)


class VimeoVideos:
    api_key = os.environ.get("VIMEO_API_KEY")

    # max_retries is the maximum retry times when a temporary HTTP error has occurred
    max_retries = 5

    # page_size is the number of videos per request.
    # 1 to 100 is allowed according to https://developer.vimeo.com/api/reference/videos#get_videos
    # we set it to 100 to reduce the number of requests.
    page_size = 100

    api_base = 'https://api.vimeo.com'

    def __init__(self):
        # videos is used to save the video data pulled from Vimeo
        self.videos = []

        # retried is the number of retry times when a temporary HTTP error has occurred
        self.retried = 0

        # initialize a session so that we can just use one TCP connection for multiple API requests
        self.session = requests.Session()
        self.session.headers.update({'Authorization': f'bearer {self.api_key}'})

    # pull_videos() pulls all my vimeo videos and save them to self.videos
    def pull_videos(self, url=None):

        # pulling video from first page with no need to feed an url
        # feeding url mainly for pulling next pages
        if not url:
            url = f'{self.api_base}/me/videos?per_page={self.page_size}&page=1'

        logger.debug(f'requesting {url}')
        response = self.session.get(url)
        logger.debug(f'response with code {response.status_code}')

        if response.status_code == 200:
            json_resp = response.json()
            self.videos.extend(json_resp['data'])
            logger.debug(f'retrieved {len(json_resp["data"])} videos')

            next_page = json_resp['paging']['next']
            if next_page:
                self.pull_videos(f'{self.api_base}{next_page}')

        elif response.status_code == 429:
            # Temporary Rate limit error (ref: https://developer.vimeo.com/guidelines/rate-limiting)
            self.retried += 1
            logger.debug(f'Rate Limit occurred {self.retried} times')

            if self.retried >= self.max_retries:
                fatal(f'Task failed after retrying {self.retried} times!')

            try:
                reset_time = datetime.strptime(response.headers.get('X-RateLimit-Reset'),
                                               '%Y-%m-%dT%H:%M:%S%z').utcnow()
                offset = reset_time - datetime.utcnow()
                time.sleep(offset.total_seconds())
            except Exception:
                # With any exception, we sleep 5 seconds as default
                time.sleep(5)

            # Trying to request the same page again after sleeping to rate limit reset time
            self.pull_videos(url)

        else:
            # Unexpected error
            fatal(f'Fatal error requesting {url}. Response code {response.status_code}')


def vimeo2vidapp(videos):
    result = []
    for v in videos:
        video = {"Type": "video",
                 "AppId": "",
                 "OriginalFilename": "",
                 "Title": "",
                 "ThumbnailSource": "",
                 "DurationSeconds": "",
                 "DataSource": "Vimeo",
                 "SourceId": "",
                 "SourceDescription": "",
                 "SourceThumbnailSource": "",
                 "SourceDateUploaded": "2019-11-12 20:47:59",
                 "Published": "Published",
                 "Tag": "|Pipe|Delimited|Video|Tags",
                 "Files": [
                     {
                         "Type": "540",
                         "URL": "https://player.vimeo.com/external/372726546.sd",
                         "Size": "743098000"
                     }
                 ]
                 }
        result.append(video)

    logger.debug(json.dumps(result, indent=4))
    return result


def extract_vimeo(event, context):
    vv = VimeoVideos()
    vv.pull_videos()
    return vimeo2vidapp(vv.videos)


if __name__ == "__main__":
    logger.addHandler(logging.StreamHandler(sys.stdout))
    extract_vimeo(None, None)
