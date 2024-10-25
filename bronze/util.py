import requests, json
from google.cloud import storage, bigquery



def request_url(url):
    # This function makes a GET request and retrieves earthquake data in JSON format
    response = requests.get(url=url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Request failed with status code: {response.status_code}")   
        return None
    

def create_bucket(bucket_name):
    """Creates a new bucket
    
    Args:
        bucket_name : give a name of a bucket you want to create
    Returns:
        bucket: newly created bucket
    """  
    storage_client = storage.Client()
    
    try:
        bucket = storage_client.create_bucket(bucket_name, location="us-central1")
        print(f"Created bucket {bucket.name} in location {bucket.location}")
    except Exception as e:
        print(f"Failed to create bucket {bucket_name}: {e}")
        bucket = storage_client.bucket(bucket_name)  # Get the existing bucket

    return bucket


def upload_to_gcs(bucket_name, data, destination_blob_name, folder_path):
    """
    The function `upload_to_gcs` uploads converted json data file to Google Cloud Storage with the
    specified bucket and destination blob names.
    
    :param bucket_name: The `bucket_name` parameter is the name of the Google Cloud Storage bucket where
    you want to upload the file. This function uses the Google Cloud Storage Python client library to
    interact with the specified bucket
    :param data: The `data` parameter in the `upload_to_gcs` function refers to
    the api data that you want to upload to Google Cloud Storage (GCS) in json format. This is the
    data that will be uploaded to the specified GCS bucket with the given `destination_blob_name`
    :param destination_blob_name: The `destination_blob_name` parameter in the `upload_to_gcs` function
    refers to the name that you want to give to the file when it is uploaded to Google Cloud Storage
    (GCS). This name will be used to identify the file within the specified GCS bucket
    :param folder_path: The `folder_path` parameter is the folder path within the bucket where the file
    will be uploaded
    """
    # convert data to gcs
    # json_data = json.dumps(data)
    
    # uploading to gcs
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(folder_path + destination_blob_name)

    blob.upload_from_string(data)
    print(f"Uploaded response data to {destination_blob_name}.")


def read_json_from_gcs(bucket_name, blob_name):
    """This function is used to read a json data from gcs bucket as a string and then parsing it as JSON data 
        and converts it to python dict or list
    Args:
        bucket_name : give the bucket name from whic you want to fetch json file
        blob_name : give the path of our json file from the bucket
    """    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data_string = blob.download_as_string()
    json_data = json.loads(data_string)
    
    return json_data

def load_data_to_bigquery_from_gcs(gcs_uri, project_id, dataset_id, table_id, schema):
    """
    Load data from a Google Cloud Storage URI into a BigQuery table in a specified project and dataset,
    creating the dataset if it does not exist.

    :param gcs_uri: The Google Cloud Storage URI where the data to be loaded into BigQuery is located.
    :param project_id: The ID of the Google Cloud project where the BigQuery dataset and table are located.
    :param dataset_id: The ID of the dataset in BigQuery where the data will be loaded from Google Cloud Storage (GCS).
    :param table_id: The ID of the BigQuery table where the data from the GCS URI will be loaded.
    :param schema: The schema of the BigQuery table, defined as a list of bigquery.SchemaField.
    """
    client = bigquery.Client(project=project_id)
    
    print("Currently working in project:", client.project)
    
    dataset_id = f"{client.project}.{dataset_id}"
    
    try:
        dataset = client.get_dataset(dataset_id)  
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "us-central1"  
        dataset = client.create_dataset(dataset)  
        print(f"Created dataset {dataset_id}")
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        max_bad_records=50
    )
    
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        location="us-central1",  
        job_config=job_config,
    )

    try:
        load_job.result()  # Wait for the job to complete
        destination_table_id = f"{project_id}.{dataset_id}.{table_id}"
        print(f"Loaded {load_job.output_rows} rows into {destination_table_id}.")
    except Exception as e:
        print("Job failed with error: {}".format(e))
        if hasattr(e, 'errors'):
            for error in e.errors:
                print(f"Error: {error}")

    
 