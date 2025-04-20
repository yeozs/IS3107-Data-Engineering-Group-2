from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from google.cloud import pubsub_v1
import logging
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

PROJECT_ID = "is-data-engineering-project"
TOPIC_NAME = "MOESchoolName"
DATASET_ID = "d_688b934f82c1059ed0a6993d2a829089"
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

geolocator = Nominatim(user_agent="simonseeyongsheng@gmail.com", timeout=10)
def get_coordinates_osm(address, retries=3):
    for _ in range(retries):
        try:
            location = geolocator.geocode(address)
            return (location.latitude, location.longitude) if location else None
        except GeocoderTimedOut:
            print(f"Timeout! Retrying for address: {address}...")
            time.sleep(2)  # Wait before retrying
    return "Failed after retries"

def transform_and_publish(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='fetch_data', key='records')
    if not records:
        return

    moe_schools_df = pd.DataFrame(records)
    
    moe_schools_df_trunc = moe_schools_df[['school_name', 'address', 'postal_code', 'mainlevel_code', 'gifted_ind']]

    categorical_columns = ['mainlevel_code', 'gifted_ind']
    moe_schools_df_trunc[categorical_columns] = moe_schools_df_trunc[categorical_columns].astype('category')

    string_columns = ['school_name', 'address', 'postal_code']
    moe_schools_df_trunc[string_columns] = moe_schools_df_trunc[string_columns].astype(pd.StringDtype())

    moe_schools_df_trunc.loc[moe_schools_df_trunc['address'].str.contains('COMPASSVAE', na=False), 'address'] = moe_schools_df_trunc['address'].str.replace('COMPASSVAE', 'COMPASSVALE', regex=False)

    # Filter for Primary schools
    primary_schools_df = moe_schools_df_trunc.loc[moe_schools_df_trunc['mainlevel_code'] == "PRIMARY"]

    # Get full address
    primary_schools_df['full_address'] = primary_schools_df[['address', 'postal_code']].agg(', Singapore '.join, axis=1)
    
    unique_addresses = primary_schools_df['full_address'].drop_duplicates()
    unique_addresses_df = unique_addresses.to_frame()

    # Get coordinates for unique addresses
    unique_addresses_df['coordinates'] = unique_addresses_df['full_address'].apply(get_coordinates_osm)
    unique_addresses_df[['latitude', 'longitude']] = unique_addresses_df['coordinates'].apply(pd.Series)
    unique_addresses_df = unique_addresses_df.drop(columns=['coordinates'])
    
    primary_schools_df_coord = primary_schools_df.merge(unique_addresses_df, on='full_address', how='left')
    primary_schools_df_coord = primary_schools_df_coord.dropna(subset=['latitude', 'longitude'])

    for _, record in primary_schools_df_coord.iterrows():
        publish_message(record.to_dict())
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
    "moeschool_to_pubsub",
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
