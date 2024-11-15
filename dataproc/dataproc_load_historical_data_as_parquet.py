###########################################################################################################
"""
file_name = dataproc_load_historical_data_as_parquet.py
description = flow of code to upload a historical earthquake analysis data to bigquery
date = 2024/11/02
version = 1

"""
############################################################################################################
from util import request_url, create_bucket, upload_to_gcs, read_json_from_gcs, write_df_to_gcs_parquet, load_parquet_data_to_bigquery_from_gcs
from dynamic_methods import DataExtractor, DataProcessor
from google.cloud import bigquery
from datetime import datetime
import logging, json
# import os 
from google.cloud import storage, bigquery


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == '__main__': 
    # Set the environment for Google Cloud authentication
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\harsh\Downloads\Study\Spark Lectures\Projects\earthquake_ingesion_hp\earthquake-analysis-440806-e4fcdf0763f4.json'
    
    try:
        # generate string representation of the current date in the format 'YYYYMMDD'.
        str_date = datetime.now().strftime('%Y%m%d')
        
        # This URL provides a summary of all earthquakes that occurred within the past month.
        url_historical = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

        # The variables components of the data processing and storage pipeline
        bucket_name = 'earthquake_analysis_by_hp_24'
        project_id = "earthquake-analysis-440806"
        dataset_id = "earthquake_analysis"
        
        # Step 1: Fetch earthquake data from the API
        logger.info("Starting data fetch from historical earthquake API.")
        data = request_url(url_historical)
        
        if data:
            logger.info("Successfully fetched earthquake data.")
        else:
            logger.error("Failed to fetch earthquake data.")
        
        
        # Step 2: Create or retrieve a GCS bucket
        logger.info("Creating or retrieving GCS bucket.")
        creating_bucket_object = create_bucket(bucket_name)

        
        # Step 3: Upload raw earthquake data to Google Cloud Storage (GCS) as a JSON file
        # Here we upload the earthquake data fetched from the API to GCS.
        logger.info("Uploading raw JSON data to GCS.")
        folder_path = "pyspark_dataproc_testing/landing/"
        destination_blob_name = f'historical_data_{str_date}.json'
        json_data = json.dumps(data)
        upload_to_gcs(bucket_name, json_data, destination_blob_name, folder_path)
        

        # Step 4: Read the uploaded JSON data from GCS
        logger.info("Reading uploaded JSON data from GCS.")
        blob_name = f'pyspark_dataproc_testing/landing/{destination_blob_name}'
        gcs_json_data = read_json_from_gcs(bucket_name, blob_name)
        

        
        # Step 5: Extract the 'features' section from the JSON response
        if 'features' in gcs_json_data:
            earthquake_records = gcs_json_data['features']
        
            # Step 6: Process the data into a list of flattened records
            # This creates a list of JSON objects with key-value pairs representing each earthquake record.
            processed_records = [DataExtractor.flatten_json(DataExtractor.extract_properties(records)) for records in earthquake_records]

            # Step 7: Create the DataFrame with the flattened data and apply transformations
            data_processor = DataProcessor()  # Instantiate the DataProcessor class
            earthquake_creation_df = data_processor.earthquake_df_creation(processed_records)
        
            earthquake_df = data_processor.earthquake_transformation_df(earthquake_creation_df)
            
        else:
            logger.error("No 'features' key in JSON data.")
            processed_records = []


        # Step 8: Upload flattened and transformed data to Google Cloud Storage (GCS) as Parquet
        # This uploads the transformed earthquake data stored in a DataFrame (`earthquake_df`) to GCS as a Parquet file.
        logger.info("Uploading transformed data as Parquet to GCS.")
        folder_path = "pyspark_dataproc_testing/Silver/parquet/"
        destination_blob_name = f'flattened_and_transformed_historical_data_{str_date}.parquet'
        gcs_path = f'gs://{bucket_name}/{folder_path}{str_date}/{destination_blob_name}'
        write_df_to_gcs_parquet(earthquake_df, bucket_name, folder_path, destination_blob_name, str_date)
        logger.info(f"DataFrame successfully written to {gcs_path}")
        
        
        # Step 9: Load the data from GCS to BigQuery
        # Define the schema and load the transformed Parquet data from GCS into BigQuery.
        logger.info("Loading data from GCS Parquet to BigQuery.")
        table_id = f"{project_id}.{dataset_id}.new_flattened_historical_data_by_parquet"
        # destination_blob_name = f'flattened_and_transformed_historical_data_{str_date}*'
        gcs_blob_name = "pyspark_dataproc_testing/Silver/parquet/"
        gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}{str_date}/"
        
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
        
        
        # Step 10: for count-match testing converting df to json and uploading it to gcs
        earthquake_df_json_array = earthquake_df.toJSON().collect() 
        logger.info("Uploading earthquake_df JSON data to GCS for unit testing purpose.")
        folder_path = f"pyspark_dataproc_testing/Silver/earthquake_df_to_json/{str_date}/"
        destination_blob_name = f'earthquake_df_json_data_{str_date}.json'
        json_data = json.dumps(earthquake_df_json_array)
        upload_to_gcs(bucket_name, json_data, destination_blob_name, folder_path)
        
        
        logger.info("All tasks completed successfully")
        
    except Exception as e:
        logger.error("An error occurred: %s", str(e))   
    
    
# Run below command in gcloud console

# gcloud dataproc jobs submit pyspark gs://earthquake_analysis_by_hp_24/pyspark_dataproc_testing/bronze/dataproc_load_historical_data_as_parquet.py --cluster=harshal-bwt-session-dataproc-cluster-24 --region=us-central1 --files=gs://earthquake_analysis_by_hp_24/pyspark_dataproc_testing/bronze/util.py,gs://earthquake_analysis_by_hp_24/pyspark_dataproc_testing/bronze/dynamic_methods.py --properties="spark.executor.memory=2g,spark.driver.memory=2g"


# spark-submit \
#   --jars "C:/Users/harsh/Downloads/gcs-connector-hadoop2-2.2.0.jar,C:/Users/harsh/Downloads/spark-bigquery-with-dependencies_2.13-0.41.0.jar,C:/spark/spark-3.3.2-bin-hadoop2/jars/hadoop-common-2.7.4.jar" \
#   --conf "spark.hadoop.google.cloud.auth.service.account.enable=true" \
#   --conf "spark.hadoop.google.cloud.auth.service.account.json.keyfile=C:/Users/harsh/Downloads/Study/Spark Lectures/Projects/earthquake_ingesion_hp/earthquake-analysis-440806-e4fcdf0763f4.json" \
#   --conf "spark.executor.memory=2g" \
#   --conf "spark.driver.memory=2g" \
#   'C:\Users\harsh\Downloads\Study\Spark Lectures\Projects\earthquake_ingesion_hp\dataproc copy\dataproc_load_historical_data_as_parquet.py'
