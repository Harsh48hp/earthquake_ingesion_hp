from datetime import datetime
import logging, requests, json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.models import Variable
from google.cloud import storage, bigquery
from bronze.util import create_bucket

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {
    'owner': 'harshal',
    'start_date': datetime(2024, 11, 6),
    'retries': 1
}

dag = DAG(
    dag_id='loading_historical_data_earthquake_analysis',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    description='Load earthquake historical data, transform, and store in BigQuery through GCS'
)


def request_url(url, ti):
    """Makes a GET request and retrieves earthquake data in JSON format.
    
    Args:
        url (str): The URL to make the request to.
    
    Returns:
        dict: JSON response data or None if the request fails.
    """
    response = requests.get(url=url)
    
    if response.status_code == 200:
        data = response.json()
        
        ti.xcom_push(key='earthquake_data', value=data)
        logger.info('Successfully fetched the data and pushed to xcom.')
        
    else:
        logger.error(f"Request failed with status code: {response.status_code}")   
        return None
    
    
url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

task_1 = PythonOperator(
    task_id='retrieve_earthquake_data_JSON',
    python_callable=request_url,
    op_args=[url],
    dag=dag
)


# task_2 = GCSCreateBucketOperator(
#     task_id='create_bucket',
#     bucket_name='earthquake_analysis_by_hp_24',
#     location='us-central1',
#     storage_class='STANDARD',
#     project_id=Variable.get('project_id'),
#     gcp_conn_id='airflow_gcs_admin', 
#     dag=dag
# )


bucket_name = 'earthquake_analysis_by_hp_24'

task_2 = PythonOperator(
    task_id='create_or_retreive_new_gcs_bucket',
    python_callable=create_bucket,
    op_args=[bucket_name],
    dag=dag
)


def upload_to_gcs(bucket_name, destination_blob_name, folder_path, ti):
    """Uploads JSON data to Google Cloud Storage.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        destination_blob_name (str): Name of the file in GCS.
        folder_path (str): Folder path within the bucket.
    """
    data = ti.xcom_pull(task_ids='retrieve_earthquake_data_JSON', key='earthquake_data')
    if not data:
        logger.error("No data found in XCom. Skipping Upload")
        return
    
    json_data = json.dumps(data)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_path}{destination_blob_name}")

    blob.upload_from_string(json_data)
    logger.info(f"Uploaded response data to GCS at '{destination_blob_name}'.")
    
    
bucket_name = 'earthquake_analysis_by_hp_24'
str_date = datetime.now().strftime('%Y%m%d')
folder_path = "apache_airflow/landing/"
destination_blob_name = f'historical_data_{str_date}.json'

task_3 = PythonOperator(
    task_id='upload_json_data_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={
        'bucket_name': bucket_name,
        'destination_blob_name': destination_blob_name,
        'folder_path': folder_path
    },
    dag=dag
)


def read_json_from_gcs(bucket_name, blob_name, ti):
    """Reads a JSON file from GCS and returns it as a dictionary or list.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Path to the JSON file in the bucket.
    
    Returns:
        dict: Parsed JSON data.
    """    
    logger.info(f"Attempting to read JSON data from GCS: bucket '{bucket_name}', blob '{blob_name}'")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data_string = blob.download_as_string()
    json_data = json.loads(data_string)
    
    earthquake_records = json_data.get('features', [])
    ti.xcom_push(key='earthquake_records', value=earthquake_records)
    
    logger.info(f"Successfully read JSON data from '{blob_name}' in bucket '{bucket_name}'.")

def extract_properties(ti):
    records = ti.xcom_pull(task_ids='read_json_data_from_gcs', key='earthquake_records')
    extracted_properties = []
    for record in records:
        properties = record['properties']
        geometry = record.get('geometry', {}).get('coordinates', [None, None, None])
        
        properties['longitude'] = float(geometry[0]) if geometry[0] is not None else None
        properties['latitude'] = float(geometry[1]) if geometry[1] is not None else None
        properties['depth'] = float(geometry[2]) if geometry[2] is not None else None
        
        extracted_properties.append(properties)
        
    ti.xcom_push(key='property_data', value=extracted_properties)
    logger.info(f"Extracted properties from {len(records)} earthquake records.")


blob_name = f'{folder_path}{destination_blob_name}'

task_4 = PythonOperator(
    task_id='read_json_data_from_gcs',
    python_callable=read_json_from_gcs,
    op_kwargs={
        'bucket_name': bucket_name,
        'blob_name': blob_name
    },
    dag=dag
)

task_5 = PythonOperator(
    task_id='extract_properties_from_earthquake_data',
    python_callable=extract_properties,
    dag=dag
)