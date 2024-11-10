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
    response = requests.get(url=url)
    
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Request failed with status code: {response.status_code}")   
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
        logger.info(f"Created bucket {bucket.name} in location {bucket.location}")
    except Conflict:
        logger.warning(f"Bucket {bucket_name} already exists. Retrieving existing bucket.")
        bucket = storage_client.bucket(bucket_name)

    return bucket