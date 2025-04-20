from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from google.cloud import pubsub_v1
import logging
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

PROJECT_ID = "is-data-engineering-project"
TOPIC_NAME = "HDBResaleTransactions"
DATASET_ID = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
API_URL = "https://data.gov.sg/api/action/datastore_search?resource_id=" + DATASET_ID

def publish_message(message, ordering_key):
    publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    message_bytes = json.dumps(message).encode("utf-8")
    publisher.publish(topic_path, message_bytes, ordering_key=ordering_key)

def fetch_data(**kwargs):
    try:
        all_records = []
        # starting_index = 196984 # Starting from 2025
        offset, limit = starting_index, 1000 

        while True:
            url = f"{API_URL}&offset={offset}&limit={limit}"
            response = requests.get(url)
            data = response.json()

            if "result" in data and "records" in data["result"]:
                records = data["result"]["records"]
                all_records.extend(records)

            if len(all_records) % limit == 0:
                offset += limit
            
            if (len(all_records) + starting_index + 1 >= data["result"]["total"]):
                break
                    
        kwargs['ti'].xcom_push(key='records', value=all_records)

    except requests.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        kwargs['ti'].xcom_push(key='records', value=[])

geolocator = Nominatim(user_agent="simonseeyongsheng@gmail.com", timeout=10)

# Function to get coordinates with retry mechanism
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

    resale_flat_df = pd.DataFrame(records)
    
    resale_flat_df['month'] = pd.to_datetime(resale_flat_df['month'], format='%Y-%m')
    resale_flat_df['lease_commence_date'] = resale_flat_df['lease_commence_date'].astype('int64')

    categorical_columns = ['town', 'flat_type', 'storey_range', 'flat_model']
    resale_flat_df[categorical_columns] = resale_flat_df[categorical_columns].astype('category')

    string_columns = ['block', 'street_name']
    resale_flat_df[string_columns] = resale_flat_df[string_columns].astype(pd.StringDtype())
    resale_flat_df['full_address'] = resale_flat_df[['block', 'street_name']].agg(' '.join, axis=1) + ", Singapore"

    resale_flat_df['floor_area_sqm'] = resale_flat_df['floor_area_sqm'].astype('float64')
    resale_flat_df['resale_price'] = resale_flat_df['resale_price'].astype('float64')
    resale_flat_df['price_per_sqm'] = resale_flat_df['resale_price'] / resale_flat_df['floor_area_sqm']

    resale_flat_df[['years', 'months']] = resale_flat_df['remaining_lease'].str.extract(r'(?:(\d+) years?)?\s?(?:(\d+) months?)?')
    resale_flat_df['years'] = resale_flat_df['years'].astype(float).fillna(0)
    resale_flat_df['months'] = resale_flat_df['months'].astype(float).fillna(0)
    resale_flat_df['remaining_lease_months'] = (resale_flat_df['years'] * 12) + resale_flat_df['months']
    resale_flat_df['remaining_lease_months'] = resale_flat_df['remaining_lease_months'].astype('int64')
    resale_flat_df['quarter'] = resale_flat_df['month'].dt.to_period('Q').astype(str)
    resale_flat_df['month'] = resale_flat_df['month'].astype(str)
    resale_flat_df_converted = resale_flat_df.drop(columns=['years', 'months', 'remaining_lease'])

    unique_addresses = resale_flat_df_converted['full_address'].drop_duplicates()
    unique_addresses_df = unique_addresses.to_frame()

    # Get coordinates for unique addresses
    unique_addresses_df['coordinates'] = unique_addresses_df['full_address'].apply(get_coordinates_osm)
    unique_addresses_df[['latitude', 'longitude']] = unique_addresses_df['coordinates'].apply(pd.Series)
    unique_addresses_df = unique_addresses_df.drop(columns=['coordinates'])
        
    resale_flat_df_converted_coord = resale_flat_df_converted.merge(unique_addresses_df, on='full_address', how='left')
    resale_flat_df_converted_coord = resale_flat_df_converted_coord.dropna(subset=['latitude', 'longitude'])
    
    for _, record in resale_flat_df_converted_coord.iterrows():
        publish_message(record.to_dict(), ordering_key=str(record['_id']))
        print(record.to_dict())

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
    "hdbresale_to_pubsub",
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
