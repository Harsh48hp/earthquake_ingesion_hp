###########################################################################################################
"""
file_name = util.py
description = utility folder containg all the necessary functions for historical data to upload to bigquery
date = 2024/11/02
version = 1

"""
############################################################################################################
import requests
import logging
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound, Conflict


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


# # Function to check if file exists in GCS
# def check_file_exists(bucket_name, file_path):
#     """
#     This function checks if a file exists in a specified bucket.
    
#     :param bucket_name: The `bucket_name` parameter refers to the name of the bucket in a cloud storage
#     service where the file is expected to be located
#     :param file_path: The `check_file_exists` function is typically used to verify if a file exists in a
#     specific location within a bucket. The `bucket_name` parameter refers to the name of the bucket
#     where the file is located, and the `file_path` parameter specifies the path to the file within the
#     bucket
#     """
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blob = bucket.blob(file_path)
#     return blob.exists()