from datetime import datetime
import logging, requests, json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
from airflow.models import Variable
from google.cloud.exceptions import NotFound, Conflict
from google.cloud import storage, bigquery



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
    """
    The function `request_url` makes a GET request to a specified URL to retrieve earthquake data in
    JSON format and pushes the data to an XCom variable in Apache Airflow.
    
    :param url: 
        The `url` parameter is a string that represents the URL to make the GET request to in order to retrieve earthquake data in JSON format
    :param ti: 
        The `ti` parameter in the `request_url` function is likely an instance of the `TaskInstance` class in Apache Airflow. This class provides access to task-specific information and
        allows you to push XCom data, which is a way to exchange messages between tasks in Apache Airflow.
    In
    :return: 
        a dictionary containing the JSON response data retrieved from the specified URL. If the request fails, it returns None.
    """
    try:
        response = requests.get(url=url)
        response.raise_for_status()  # Raises an error for bad responses
        data = response.json()
        
        ti.xcom_push(key='earthquake_data', value=data)
        logger.info('Successfully fetched the data and pushed to XCom.')
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to retrieve data: {e}")
        return None
    
    
url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

retrieve_data_task_1  = PythonOperator(
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

bucket_name = 'earthquake_analysis_by_hp_24'

create_bucket_task_2  = PythonOperator(
    task_id='create_or_retreive_new_gcs_bucket',
    python_callable=create_bucket,
    op_args=[bucket_name],
    dag=dag
)


def upload_to_gcs(bucket_name, destination_blob_name, folder_path, ti):
    """
    The function `upload_to_gcs` uploads JSON data to Google Cloud Storage from an XCom variable in an
    Airflow task.
    
        :param bucket_name: 
        The `bucket_name` parameter is the name of the Google Cloud Storage (GCS) bucket where you want to upload the JSON data. This is the destination bucket where the file will be stored
    :param destination_blob_name: 
        The `destination_blob_name` parameter in the `upload_to_gcs` function refers to the name of the file that will be created in Google Cloud Storage (GCS) where the JSON
        data will be uploaded. It is the name of the file within the specified folder path in the GCS bucket
    :param folder_path: 
        The `folder_path` parameter in the `upload_to_gcs` function represents the folder path within the Google Cloud Storage (GCS) bucket where you want to store the uploaded file.

    :return: 
    If there is no data found in the XCom, the function will log an error message "No data found in XCom. Skipping Upload" and return without uploading anything.
    """
    try:
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
    except Exception as e:
        logger.error(f"Failed to upload data to gcs: {e}")
    
    
bucket_name = 'earthquake_analysis_by_hp_24'
str_date = datetime.now().strftime('%Y%m%d')
folder_path = "apache_airflow/landing/"
destination_blob_name = f'historical_data_{str_date}.json'

upload_json_to_gcs_task_3 = PythonOperator(
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
    """
    The function `read_json_from_gcs` reads a JSON file from Google Cloud Storage (GCS) and returns it
    as a dictionary.
    
    :param bucket_name: 
        The `bucket_name` parameter is the name of the Google Cloud Storage (GCS) bucket from which you want to read the JSON file. This bucket should already exist in your 
        Google Cloud Platform (GCP) project and should contain the JSON file you want to read
    :param blob_name: 
        The `blob_name` parameter in the `read_json_from_gcs` function refers to the path to the JSON file within the specified GCS bucket. 
        It is the location of the JSON file that you want to read from Google Cloud Storage (GCS). This path includes the directory structure within the
    :param ti: 
        The `ti` parameter in the `read_json_from_gcs` function is typically an instance of the `TaskInstance` class in Apache Airflow. It is used to access 
        and manipulate information related to the task instance, such as XComs (cross-communication) which are used for sharing
    """
    try:
        logger.info(f"Attempting to read JSON data from GCS: bucket '{bucket_name}', blob '{blob_name}'")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        data_string = blob.download_as_string()
        json_data = json.loads(data_string)
        
        earthquake_records = json_data.get('features', [])
        ti.xcom_push(key='earthquake_records', value=earthquake_records)
        
        logger.info(f"Successfully read JSON data from '{blob_name}' in bucket '{bucket_name}'.")
    except Exception as e:
        logger.error(f"Failed to read JSON data: {e}")


def extract_properties(ti):
    """Extracts earthquake properties and coordinates and pushes to XCom."""
    try:
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
    except Exception as e:
        logger.error(f"Failed to extract properties: {e}")


def flatten_json(ti):
    """ Flattens the earthquake JSON data, ensuring appropriate types and default values."""
    try:
        data = ti.xcom_pull(task_ids='extract_properties_from_earthquake_data', key='property_data')
        flattened_data = []
        
        field_types = {
            "mag": (float, 0.0),
            "place": (str, ''),
            "time": (str, '0'),
            "updated": (str, '0'),
            "tz": (str, ''),
            "url": (str, ''),
            "detail": (str, ''),
            "felt": (int, 0),
            "cdi": (float, 0.0),
            "mmi": (float, 0.0),
            "alert": (str, ''),
            "status": (str, ''),
            "tsunami": (int, 0),
            "sig": (int, 0),
            "net": (str, ''),
            "code": (str, ''),
            "ids": (str, ''),
            "sources": (str, ''),
            "types": (str, ''),
            "nst": (int, 0),
            "dmin": (float, 0.0),
            "rms": (float, 0.0),
            "gap": (float, 0.0),
            "magType": (str, ''),
            "type": (str, ''),
            "title": (str, ''),
            "longitude": (float, 0.0),
            "latitude": (float, 0.0),
            "depth": (float, 0.0)
        }
        
        for record in data:
            flat_record = {}
            # Convert fields to appropriate types using the field_types dictionary
            for field, (field_type, default_value) in field_types.items():
                flat_record[field] = field_type(record.get(field, default_value)) if record.get(field) is not None else default_value
            flattened_data.append(flat_record)
            
        ti.xcom_push(key='flattened_data', value=flattened_data)
        logger.info("Flattened earthquake data and pushed to XCom.")
        
    except Exception as e:
        logger.error(f"Failed to flatten data: {e}")


blob_name = f'{folder_path}{destination_blob_name}'

read_json_from_gcs_task_4 = PythonOperator(
    task_id='read_json_data_from_gcs',
    python_callable=read_json_from_gcs,
    op_kwargs={
        'bucket_name': bucket_name,
        'blob_name': blob_name
    },
    dag=dag
)

extract_properties_task_5 = PythonOperator(
    task_id='extract_properties_from_earthquake_data',
    python_callable=extract_properties,
    dag=dag
)

flatten_json_task_6 = PythonOperator(
    task_id='flatten_the_earthquake_data', 
    python_callable=flatten_json,
    dag=dag
)

def convert_to_df(ti):
    """
    The function `convert_to_df` creates a DataFrame from flattened earthquake data, applies
    transformations, and stores it in XCom.
    
    :param ti: 
        `ti` is typically an abbreviation for "task instance" in Apache Airflow. It is an object that provides access to task-specific information and context during task execution. 
        In this context, `ti` is used to interact with XCom (cross-communication) to pull and push data between tasks
    
    :return: 
        The `convert_to_df` function returns either the created and transformed earthquake DataFrame or None if there is no flattened data available in XCom.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_unixtime, col, when, regexp_extract, current_timestamp
    from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType
    spark = SparkSession.builder.appName("EarthquakeAnalysis").getOrCreate()
    
    flattened_data = ti.xcom_pull(task_ids='flatten_the_earthquake_data', key='flattened_data')
    
    if not flattened_data:
        logger.error("No flattened data available in XCom. Skipping DataFrame creation.")
        return
    
    earthquake_schema = StructType([
        StructField("mag", FloatType(), True),
        StructField("place", StringType(), True),
        StructField("time", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("tz", StringType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", FloatType(), True),
        StructField("mmi", FloatType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", FloatType(), True),
        StructField("rms", FloatType(), True),
        StructField("gap", FloatType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("depth", FloatType(), True)
    ])


    # Create the DataFrame with the flattened data and schema
    earthquake_df = spark.createDataFrame(flattened_data, schema=earthquake_schema)


    # This block of code is performing the following trasnformation operations on the `earthquake_df` DataFrame:
    earthquake_df = earthquake_df.withColumn('time', from_unixtime(col('time').cast('long')/1000, format="yyyy-MM-dd HH:mm:ss")) \
                                   .withColumn('updated', from_unixtime(col('updated').cast('long')/1000, format="yyyy-MM-dd HH:mm:ss")) \
                                   .withColumn('area',
                                                when(col("place").contains(" of "), regexp_extract(col("place"), r' of\s*(.*)', 1))
                                                .otherwise(col("place")))\
                                   .withColumn('ingestion_dt', current_timestamp())
                                   
    earthquake_df.show()  # Display the DataFrame for debugging purposes
    earthquake_df.printSchema()
    ti.xcom_push(key='earthquake_dataframe', value=earthquake_df)
    logger.info("Successfully created and transformed the earthquake DataFrame.")
    return earthquake_df
    
    
convert_flattened_data_to_df_task_7 = PythonOperator(
    task_id='convert_flattened_data_to_df',
    python_callable=convert_to_df,
    dag=dag
)


def write_df_to_gcs_parquet(bucket_name, folder_path, destination_blob_name, ti):
    """
    The function `write_df_to_gcs_parquet` writes a DataFrame in Parquet format to Google Cloud Storage.
    
    :param ti: 
        In the provided function `write_df_to_gcs_parquet`, the parameter `ti` is typically an instance of the Airflow TaskInstance class. It is used to access task-specific information 
        and XCom data in an Airflow DAG. The `ti` parameter allows you to pull XCom data
    :param bucket_name: 
        The `bucket_name` parameter in the `write_df_to_gcs_parquet` function refers to the name of the Google Cloud Storage (GCS) bucket where you want to store the Parquet file. 
        This is the destination bucket where the Parquet file will be saved
    :param folder_path: 
        The `folder_path` parameter refers to the specific folder path within the Google Cloud Storage (GCS) bucket where you want to save the Parquet file.
    :param destination_blob_name: 
        The `destination_blob_name` parameter in the `write_df_to_gcs_parquet` function refers to the name of the Parquet file that will be saved in Google Cloud Storage (GCS).
        This parameter specifies the name of the file within the specified `folder_path` where the DataFrame will be
    """
    
    dataframe = ti.xcom_pull(task_ids='convert_flattened_data_to_df', key='earthquake_dataframe')
    
    if dataframe is None:
        raise ValueError("DataFrame not found in XCom. Ensure previous task pushed it correctly.")
    
    # Write DataFrame to GCS in Parquet format
    try:
        gcs_path = f'gs://{bucket_name}/{folder_path}{destination_blob_name}'
        dataframe.write.mode('overwrite').parquet(gcs_path)
        logger.info(f"Successfully Wrote DataFrame to {gcs_path}.")
    except Exception as e:
        logger.error(f"Failed to write DataFrame to GCS: {e}")
        raise


folder_path = "apache_airflow/Silver/parquet/"
destination_blob_name = f'flattened_and_transformed_historical_data_{str_date}.parquet'


write_df_to_gcs_task_8 = PythonOperator(
    task_id='writing_df_in_parquet_to_gcs',
    python_callable=write_df_to_gcs_parquet,
    op_kwargs={
        'bucket_name': bucket_name,
        'folder_path': folder_path,
        'destination_blob_name': destination_blob_name
    },
    dag=dag
)    

create_dataset_task_9 = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset',
    dataset_id='airflow_earthquake_analysis',
    project_id=Variable.get('project_id'),
    location='us-central1',
    gcp_conn_id='airflow_gcs_admin',
    dag=dag
)


def load_gcs_parquet_to_bigquery():
    project_id = "earthquake-analysis-440806"
    dataset_id = "airflow_earthquake_analysis"
    table_id = f"{project_id}.{dataset_id}.flattned_historical_data_by_airflow"
    bucket_name = "earthquake_analysis_by_hp_24"
    folder_path = "apache_airflow/Silver/parquet/"
    destination_blob_name = f'flattened_and_transformed_historical_data_{str_date}.parquet'
    gcs_uri = f'gs://{bucket_name}/{folder_path}{destination_blob_name}'

    # BigQuery load job configuration
    load_job_config = {
        "configuration": {
            "load": {
                "sourceUris": [gcs_uri],
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": "flattned_historical_data_by_airflow",
                },
                "schema": [
                    {"name": "mag", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "place", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "time", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "tz", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "url", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "detail", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "felt", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "cdi", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "mmi", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "alert", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "status", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "tsunami", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "sig", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "net", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "code", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "ids", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "sources", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "types", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "nst", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "dmin", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "rms", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "gap", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "type", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "title", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "depth", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "area", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "ingestion_dt", "type": "TIMESTAMP", "mode": "NULLABLE"},
                ],
                "sourceFormat": "PARQUET",
            }
        }
    }
    return load_job_config

# Define the task to load data into BigQuery
load_to_bigquery_task_10 = BigQueryInsertJobOperator(
    task_id='load_parquet_to_bigquery',
    configuration=load_gcs_parquet_to_bigquery(),
    location='us-central1',  
    project_id="earthquake-analysis-440806",  
    gcp_conn_id='airflow_gcs_admin',  # Connection ID for GCP
    dag=dag
)


# Set task dependencies
retrieve_data_task_1 >> create_bucket_task_2 >> upload_json_to_gcs_task_3 >> read_json_from_gcs_task_4 >> extract_properties_task_5 >> flatten_json_task_6 >> convert_flattened_data_to_df_task_7 >> write_df_to_gcs_task_8 >> create_dataset_task_9 >> load_to_bigquery_task_10