from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from google.cloud import pubsub_v1
import logging
import geopandas as gpd
import re

PROJECT_ID = "is-data-engineering-project"
TOPIC_NAME = "MRTExits"
DATASET_ID = "d_b39d3a0871985372d7e1637193335da5"


def publish_message(message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    message_bytes = json.dumps(message).encode("utf-8")
    publisher.publish(topic_path, message_bytes)

def fetch_data(**kwargs):
    MAX_POLLS = 5
    for i in range(MAX_POLLS):
        poll_download_response = requests.get(
            f"https://api-open.data.gov.sg/v1/public/api/datasets/{DATASET_ID}/poll-download",
            headers={"Content-Type":"application/json"},
            json={}
        )
        if "url" in poll_download_response.json()['data']:
            API_URL = poll_download_response.json()['data']['url']
    try:
        response = requests.get(f"{API_URL}")
        response.raise_for_status()
        geojson_dict = json.loads(response.text)

        kwargs['ti'].xcom_push(key='records', value=geojson_dict)
 
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        kwargs['ti'].xcom_push(key='records', value=[])


def transform_and_publish(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='fetch_data', key='records')
    if not records:
        return
    
    lta_mrt_exits_df = gpd.GeoDataFrame.from_features(records["features"])
    lta_mrt_exits_df['longitude'] = lta_mrt_exits_df['geometry'].x
    lta_mrt_exits_df['latitude'] = lta_mrt_exits_df['geometry'].y

    station_pattern = re.compile(r'<th>STATION_NA</th> <td>(.*?)</td>')
    exit_pattern = re.compile(r'<th>EXIT_CODE</th> <td>(.*?)</td>')

    def extract_combined_info(description):
        station_name = station_pattern.search(description)
        exit_code = exit_pattern.search(description)

        station_name = station_name.group(1) if station_name else "Unknown Station"
        exit_code = exit_code.group(1) if exit_code else "Unknown Exit"

        return f"{station_name} - {exit_code}"

    lta_mrt_exits_df['Station_Exit'] = lta_mrt_exits_df['Description'].apply(extract_combined_info)
    lta_mrt_exits_df.drop(columns=['Description'], inplace=True)
    lta_mrt_exits_df["geometry"] = lta_mrt_exits_df["geometry"].astype(str)

    for _, record in lta_mrt_exits_df.iterrows():
        publish_message(record.to_dict())
        print(record.to_dict())

    logging.info(f"Published {len(lta_mrt_exits_df)} records")

# Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 10),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "mrt_exit_to_pubsub",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

fetch_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

transform_publish_task = PythonOperator(
    task_id="transform_and_publish",
    python_callable=transform_and_publish,
    provide_context=True,
    dag=dag,
)

fetch_task >> transform_publish_task
