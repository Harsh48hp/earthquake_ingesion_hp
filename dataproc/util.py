###########################################################################################################
"""
file_name = util.py
description = utility folder containg all the necessary functions for historical data to upload to bigquery
date = 2024/11/02
version = 1

"""
############################################################################################################
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound, Conflict
import requests
import json
import logging



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def request_url(url):
    """Makes a GET request and retrieves earthquake data in JSON format.
    
    Args:
        url (str): The URL to make the request to.
    
    Returns:
        dict: JSON response data or None if the request fails.
    """
    try:
        response = requests.get(url=url, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully retrieved data from {url}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to {url} failed: {e}")
        return None
    

def create_bucket(bucket_name):
    """Creates a new bucket or retrieves the if existed.
    
    Args:
        bucket_name (str): Name of the bucket to create.
    
    Returns:
        storage.Bucket: Newly created bucket or existing bucket if it already exists.
    """
    storage_client = storage.Client()
    
    try:
        bucket = storage_client.create_bucket(bucket_name, location="us-central1")
        logger.info(f"Created bucket '{bucket.name}' in location '{bucket.location}'")
    except Conflict:
        bucket = storage_client.bucket(bucket_name)
        if bucket.exists():
            logger.warning(f"Bucket '{bucket_name}' already exists. Retrieved existing bucket.")
        else:
            logger.error(f"Bucket '{bucket_name}' could not be created or retrieved.")
            return None
    except Exception as e:
        logger.error(f"An error occurred while creating or retrieving bucket '{bucket_name}': {e}")
        return None

    return bucket


def upload_to_gcs(bucket_name, data, destination_blob_name, folder_path):
    """Uploads JSON data to Google Cloud Storage.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        data (str): JSON data to upload.
        destination_blob_name (str): Name of the file in GCS.
        folder_path (str): Folder path within the bucket.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_path}{destination_blob_name}")

    blob.upload_from_string(data, content_type='application/json')
    logger.info(f"Uploaded response data to {destination_blob_name}.")


def write_df_to_gcs_parquet(dataframe, bucket_name, folder_path, destination_blob_name, str_date):
    """Writes a DataFrame in Parquet format to Google Cloud Storage.
    
    Args:
        dataframe (DataFrame): The DataFrame to be saved.
        bucket_name (str): Name of the GCS bucket.
        folder_path (str): Folder path within the bucket.
        destination_blob_name (str): Name of the Parquet file to be saved.
    """
    gcs_path = f'gs://{bucket_name}/{folder_path}{str_date}/{destination_blob_name}'
    
    try:
        # Write DataFrame to GCS in Parquet format
        dataframe.write.mode('overwrite').parquet(gcs_path)
        logger.info(f"Wrote DataFrame to {gcs_path}.")
    except Exception as e:
            logging.error(f"Error uploading DataFrame: {e}")

def read_json_from_gcs(bucket_name, blob_name):
    """Reads a JSON file from GCS and returns it as a dictionary or list.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Path to the JSON file in the bucket.
    
    Returns:
        dict: Parsed JSON data.
    """    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data_string = blob.download_as_string()
    json_data = json.loads(data_string)
    
    logger.info(f"Read JSON data from {blob_name}.")
    return json_data
                
                
def load_parquet_data_to_bigquery_from_gcs(gcs_uri, project_id, dataset_id, table_id, schema):
    """Loads Parquet data from GCS into a BigQuery table.
    
    Args:
        gcs_uri (str): GCS URI for the Parquet data.
        project_id (str): Project ID for the BigQuery dataset.
        dataset_id (str): Dataset ID in BigQuery.
        table_id (str): Table ID in BigQuery.
        schema (list): Schema of the BigQuery table.
    """
    client = bigquery.Client(project=project_id)
    
    logger.info("Currently working in project: %s", client.project)
    
    dataset_full_id = f"{client.project}.{dataset_id}"
    
    try:
        dataset = client.get_dataset(dataset_full_id)  
        logger.info(f"Dataset {dataset_full_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_full_id)
        dataset.location = "us-central1"  
        dataset = client.create_dataset(dataset)  
        logger.info(f"Created dataset {dataset_full_id}")
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    load_job = client.load_table_from_uri(
        f"{gcs_uri}*.parquet",
        table_id,
        location="us-central1",  
        job_config=job_config,
    )

    try:
        load_job.result()  # Wait for the job to complete
        logger.info(f"Loaded {load_job.output_rows} rows into {gcs_uri}.")
    except Exception as e:
        logger.error("Job failed with error: %s", e)
        if hasattr(e, 'errors'):
            for error in e.errors:
                logger.error("Error: %s", error)

