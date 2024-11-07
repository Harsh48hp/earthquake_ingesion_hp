from pyspark.sql import SparkSession
from util import request_url, create_bucket, upload_to_gcs, read_json_from_gcs, write_df_to_gcs_parquet, load_parquet_data_to_bigquery_from_gcs
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType
import json
from datetime import datetime
from pyspark.sql.functions import from_unixtime, col, regexp_extract, current_timestamp, when
from google.cloud import bigquery


# Function to process the response and extract the required fields
def extract_properties(response: dict) -> dict:
    """
    Extracts properties and coordinates from the earthquake response data.

    Args:
    response (dict): Single earthquake event response from JSON.

    Returns:
    dict: A processed dictionary containing earthquake details with longitude, latitude, and depth.
    """
    properties = response['properties']
    properties['longitude'] = float(response['geometry']['coordinates'][0])
    properties['latitude'] = float(response['geometry']['coordinates'][1])
    properties['depth'] = float(response['geometry']['coordinates'][2])
    return properties


# Function to flatten and normalize the JSON data for structured storage
def flatten_json(data: dict) -> dict:
    """
    Flattens the earthquake JSON data, ensuring appropriate types and default values.

    Args:
    data (dict): A single earthquake record.

    Returns:
    dict: A flattened and normalized record.
    """
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
    
    # Convert fields to appropriate types using the field_types dictionary
    for field, (field_type, default_value) in field_types.items():
        data[field] = field_type(data.get(field, default_value)) if data.get(field) is not None else default_value
    
    return data


if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('Historical Data Load By Parquet') \
        .getOrCreate()
    
    # Set log level directly for the Spark application
    spark.sparkContext.setLogLevel("ERROR")


    # Step 2: Fetch earthquake data from the API
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    data = request_url(url)
    
    
    # Step 3: creating a new gcs bucket
    bucket_name = 'earthquake_analysis_by_hp_24'
    creating_bucket_object = create_bucket(bucket_name)
    
    
    # Step 4: Upload raw data to Google Cloud Storage (GCS)
    # This block of code is responsible for uploading the raw earthquake data fetched from the API to
    # Google Cloud Storage (GCS) as a JSON file. Here's a breakdown of what each step is doing:
    bucket_name = 'earthquake_analysis_by_hp_24'
    str_date = datetime.now().strftime('%Y%m%d')
    folder_path = "pyspark_dataproc/landing/"
    json_data = json.dumps(data)
    destination_blob_name = f'historical_data_{str_date}.json'
    upload_to_gcs(bucket_name, json_data, destination_blob_name, folder_path)
    

    # Step 5: Read the uploaded JSON data from GCS
    bucket_name = 'earthquake_analysis_by_hp_24'
    blob_name = f'pyspark_dataproc/landing/{destination_blob_name}'
    gcs_json_data = read_json_from_gcs(bucket_name, blob_name)


    # Step 6: Extract the 'features' section from the JSON response
    earthquake_records = gcs_json_data['features']
    
    
    # Step 7: Process the data into a list of flattened records
    processed_records = [flatten_json(extract_properties(records)) for records in earthquake_records]


    # Step 8: Define the schema for the DataFrame
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


    # Step 9: Create the DataFrame with the flattened data and schema
    earthquake_df = spark.createDataFrame(processed_records, earthquake_schema)


    # Step 10: This block of code is performing the following trasnformation operations on the `earthquake_df` DataFrame:
    earthquake_df = earthquake_df.withColumn('time', from_unixtime(col('time').cast('long')/1000, format="yyyy-MM-dd HH:mm:ss")) \
                                   .withColumn('updated', from_unixtime(col('updated').cast('long')/1000, format="yyyy-MM-dd HH:mm:ss")) \
                                   .withColumn('area',
                                                when(col("place").contains(" of "), regexp_extract(col("place"), r' of\s*(.*)', 1))
                                                .otherwise(col("place")))\
                                   .withColumn('ingestion_dt', current_timestamp())

    
    # Step 11: Show the DataFrame and its schema
    earthquake_df.show(truncate=False)
    earthquake_df.printSchema()


    # Step 12: Upload fltanned and transformed data to Google Cloud Storage (GCS) as Parquet
    # This block of code is responsible for uploading the flattened and transformed earthquake data
    # stored in a DataFrame (`earthquake_df`) to Google Cloud Storage (GCS) as a Parquet file. 
    bucket_name = 'earthquake_analysis_by_hp_24'
    folder_path = "pyspark_dataproc/Silver/parquet/"
    destination_blob_name = f'flattened_and_transformed_historical_data_{str_date}.parquet'
    gcs_path = f'gs://{bucket_name}/{folder_path}{destination_blob_name}'
    write_df_to_gcs_parquet(earthquake_df, bucket_name, folder_path, destination_blob_name)
    print(f"DataFrame successfully written to {gcs_path}")
    
    
    # step 13: Loading the data from gcs to bigquery
    # The code is defining a schema for a BigQuery table and then loading data from a Google
    # Cloud Storage (GCS) URI into that BigQuery table.
    project_id = "earthquake-analysis-440806"
    dataset_id = "earthquake_analysis"
    table_id = f"{project_id}.{dataset_id}.flattned_historical_data_by_parquet"
    bucket_name = "earthquake_analysis_by_hp_24"
    gcs_blob_name = "pyspark_dataproc/Silver/parquet/"
    gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"
    
    schema = [
        bigquery.SchemaField("mag", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("place", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("time", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("updated", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tz", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("detail", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("felt", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("cdi", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("mmi", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("alert", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tsunami", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sig", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("net", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ids", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sources", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("types", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("nst", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("dmin", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("rms", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("gap", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("magType", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("longitude", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("latitude", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("depth", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("area", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ingestion_dt", "TIMESTAMP", mode="REQUIRED")
    ]
    
    load_parquet_data_to_bigquery_from_gcs(gcs_uri, project_id, dataset_id, table_id, schema)
    
    # Stop Spark session
    spark.stop()

# Run below command in gcloud console
# 
# gcloud dataproc jobs submit pyspark gs://earthquake_analysis_by_hp_24/pyspark_dataproc/bronze/load_historical_data_pyspark_parquet.py --cluster=harshal-bwt-session-dataproc-cluster-24 --region=us-central1 --files=gs://earthquake_analysis_by_hp_24/pyspark_dataproc/bronze/util.py --properties="spark.executor.memory=2g,spark.driver.memory=2g"