from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from google.cloud import pubsub_v1
import logging
import os
import pytz


PROJECT_ID = "is-data-engineering-project"
TOPIC_NAME = "HDBResalePriceIndex"
DATASET_ID = "d_14f63e595975691e7c24a27ae4c07c79"
API_URL = "https://data.gov.sg/api/action/datastore_search?resource_id="  + DATASET_ID


def publish_message(message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    message_bytes = json.dumps(message).encode("utf-8")
    publisher.publish(topic_path, message_bytes)

def fetch_data(**kwargs):
    try:
        all_records = []
        offset, limit = 0, 100

        while True:
            url = f"{API_URL}&offset={offset}&limit={limit}"
            response = requests.get(url)
            data = response.json()

            if "result" in data and "records" in data["result"]:
                records = data["result"]["records"]
                all_records.extend(records)

            if len(all_records) % limit == 0:
                offset += limit
            
            if (len(all_records) == data["result"]["total"]):
                break
            
        kwargs['ti'].xcom_push(key='records', value=all_records)
    
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        kwargs['ti'].xcom_push(key='records', value=[])

def transform_and_publish(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='fetch_data', key='records')
    if not records:
        return
    
    hdb_resale_price_index_df = pd.DataFrame(records)
    
    hdb_resale_price_index_df['quarter'] = hdb_resale_price_index_df['quarter'].astype(pd.StringDtype()).str.replace('-', '', regex=True)

    # Convert 'index' column to float64
    hdb_resale_price_index_df['index'] = pd.to_numeric(hdb_resale_price_index_df['index'], errors='coerce', downcast="float")

    hdb_resale_price_index_df = hdb_resale_price_index_df.sort_values(by='quarter')
    hdb_resale_price_index_df['Price_Index_Lagged'] = hdb_resale_price_index_df['index'].shift(1)
    hdb_resale_price_index_df.drop(columns=['index'], inplace=True)
    hdb_resale_price_index_df["ingested_date"] = datetime.now(pytz.timezone('Asia/Singapore')).isoformat()

    for record in hdb_resale_price_index_df.to_dict(orient='records'):
        publish_message(record)
        print(record)

    logging.info(f"Published {len(records)} records")

# Airflow DAG 
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 10),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "price_index_to_pubsub",
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
