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

    def __init__(self, modified_after=None):
        self.modified_after = modified_after

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

            # if modified_after is set, we only retrieve Vimeo videos newer than the given time
            if self.modified_after:
                # This is something COOL in python!!
                self.videos.extend(v for v in json_resp['data'] if v['modified_time'] > self.modified_after)
            else:
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


def extract_vimeo(event, context):
    vv = VimeoVideos(modified_after=event.get('modifiedAfter'))
    vv.pull_videos()

    result = []
    for v in vv.videos:
        video = {"Type": v["type"],
                 "AppId": event.get("AppId", "UnknownAppId"),
                 "Title": v["name"],
                 "DurationSeconds": str(v["duration"]),
                 "DataSource": "Vimeo",
                 "SourceDescription": v["description"],
                 "SourceDateUploaded": v['release_time'],
                 "Published": "Published",
                 "Tag": '|'.join(tag for tag in v["tags"]),
                 "Files": [],
                 "Thumbnails": {},
                 }
        try:
            source_id = v.get('uri', '').rsplit('/', 1)[1]
        except IndexError:
            # Without source ID, we log an error and skip this entry
            logger.error(f'Failed to retrieve source ID from uri:{v["uri"]}')
            continue

        video['SourceId'] = source_id
        video['OriginalFilename'] = f'Vimeo{source_id}'

        # It seems that a Vimeo video often has 7 thumbnail pictures.
        # Vidapp: 1280x720 as large, 640x360 as medium and 295x166 as small.
        for pic in v['pictures']['sizes']:
            if pic['width'] == 1280 or pic['height'] == 720:
                video['ThumbnailSource'] = video['SourceThumbnailSource'] = pic['link']
                video['Thumbnails']['source'] = video['Thumbnails']['large'] = pic['link']
            elif pic['width'] == 640 or pic['height'] == 360:
                video['Thumbnails']['medium'] = pic['link']
            elif pic['width'] == 295 or pic['height'] == 166:
                video['Thumbnails']['small'] = pic['link']

        video['Files'].extend({'Type': f['type'],
                               'URL': f['link'],
                               'Size': str(f['size']),
                               } for f in v['files'])

        result.append(video)

    # logger.debug(json.dumps(result, indent=4))
    return result


if __name__ == "__main__":
    logger.addHandler(logging.StreamHandler(sys.stdout))
    extract_vimeo(None, None)
