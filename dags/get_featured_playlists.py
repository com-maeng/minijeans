import os
import json
import datetime
from urllib.parse import urlencode

import boto3
import pendulum
from dotenv import load_dotenv
from pytz import timezone
from urllib3 import request
from airflow.decorators import dag, task


BUCKET_NAME = 'minijeans-spotify-api'
KEY_PREFIX = 'featured_playlists/'

load_dotenv()


@dag(schedule='@daily', start_date=pendulum.datetime(2024, 8, 4, tz='Asia/Seoul'))
def get_featured_playlists() -> None:
    @task(task_id='get_access_token')
    def get_access_token() -> str:
        token_endpoint = 'https://accounts.spotify.com/api/token/?'
        encoded_args = urlencode({
            'grant_type': 'client_credentials',
            'client_id': os.getenv('MNJS_SPOTIFY_API_CLIENT_ID'),
            'client_secret': os.getenv('MNJS_SPOTIFY_API_CLIENT_SECRET')
        })

        resp = request(
            'POST',
            token_endpoint + encoded_args,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )
        return json.loads(resp.data)['access_token']

    @task(task_id='fetch_data')
    def fetch_data(access_token: str) -> list[dict]:
        limit_param_max = 50
        locale_list = ['en_US', 'ko_KR']
        ret = []

        for locale in locale_list:
            url = 'https://api.spotify.com/v1/browse/featured-playlists?' + \
                f'locale={locale}&limit={limit_param_max}'
            while True:
                resp = request(
                    'GET',
                    url,
                    headers={'Authorization': 'Bearer ' + access_token}
                )
                data = resp.data  # `bytes` data

                dict_data = json.loads(data)
                ret.append(dict_data)

                if not dict_data['playlists']['next']:
                    break

                url = dict_data['playlists']['next']

        return ret

    @task(task_id='put_object')
    def put_object(fetched_data: list[dict]) -> None:
        current_time = datetime.datetime.now(
            timezone('Asia/Seoul')).strftime('%Y-%m-%dT%H:%M:%S%z')
        obj_ext = '.json'

        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('MNJS_AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('MNJS_AWS_SECRET_ACCESS_KEY')
        )

        for idx, data in enumerate(fetched_data):
            s3.put_object(
                Body=json.dumps(data).encode('utf-8'),
                Bucket=BUCKET_NAME,
                Key=KEY_PREFIX + current_time + f'-{idx}' + obj_ext
            )

    put_object(fetch_data(get_access_token()))


get_featured_playlists()
