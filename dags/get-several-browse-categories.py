from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import requests
import base64
import json
from datetime import datetime

def upload_to_s3() -> None:

    client_id = "1389fb6773024ecbb3b78139a39cc06d"
    client_secret = "b82e46819cf147e1a119b73c58ba2a73"
    endpoint = "https://accounts.spotify.com/api/token"

# python 3.x 버전
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {"Authorization": "Basic {}".format(encoded)}

    payload = {"grant_type": "client_credentials"}

    response = requests.post(endpoint, data=payload, headers=headers)

    access_token = json.loads(response.text)['access_token']
#print(access_token)

    response = requests.get("https://api.spotify.com/v1/browse/categories", headers={"Authorization": "Bearer " + access_token})
    data = json.loads(response.content)
#print(data)

    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = current_time + "_data.json"
    data_dir = "get-several-browse-categories/"
    dags_path = "/opt/airflow/dags/" + data_dir
    file_path = dags_path + file_name
    s3_path = data_dir + file_name

    with open(file_path, 'w') as f:
        json.dump(data, f)

#with open(file_path, 'r') as f:
#    print(json.load(f))

    bucket = 'minijeans-spotify-api-data-poc' # 버킷 이름

    hook = S3Hook('aws_conn')
    hook.load_file(filename=file_path,
                   key=s3_path,
                   bucket_name=bucket,
                   replace=True)


with DAG('get-several-browse-categories',
         schedule="*/30 * * * *",
         start_date=datetime(2022, 1, 1),
         catchup=False
         ) as dag:
    upload = PythonOperator(task_id='upload',
                            python_callable=upload_to_s3)

    upload
