import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from dynamic_methods import FetchDataFromUrl, ExtractFeatures, FlattenJSON, UnixToIst, IngestionDate, AddingAreaField
from schema_data import SchemaConverter
from util import create_bucket
import os
import logging
from datetime import datetime


def set_pipeline_options():
    # Initialize PipelineOptions and GoogleCloudOptions
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "earthquake-analysis-440806"
    google_cloud_options.job_name = "loadParquetToBigQuery"
    google_cloud_options.region = "us-central1"
    google_cloud_options.staging_location = "gs://dataproc-staging-us-central1-1041991067679-wndc42dq/stage_loc/"
    google_cloud_options.temp_location = "gs://dataproc-temp-us-central1-1041991067679-qkiizwmb/temp_loc/"
    return options


def fetch_data_from_url(pipeline, url):
    # Step 1: Fetch Data from URL
    fetch_results = (
        pipeline
        | 'Create API request' >> beam.Create([None])  # Trigger the pipeline
        | 'Fetch Data From API' >> beam.ParDo(FetchDataFromUrl(url)).with_outputs('error', main='main')
    )
    logging.info("Step 1: Fetching data from API")
    return fetch_results


def write_to_gcs_as_json(pipeline, fetch_results, bucket_name, destination_blob_name):
    # Step 2: Write Data to GCS as JSON
    written_data = (
        fetch_results.main  # Use the main output (successful fetches)
        | 'Write To GCS in JSON' >> beam.io.WriteToText(
            f'gs://{bucket_name}/{destination_blob_name}',
            file_name_suffix='',  # No suffix added to the file name
            shard_name_template='',  # No sharding (single file)
            num_shards=1  # Single shard (single file output)
        )
    )
    logging.info("Step 2: Data written to GCS as JSON")


def process_and_transform_data(pipeline, bucket_name, destination_blob_name):
    # Step 3: Read and Process Data
    logging.info("Step 3: Processing and transforming the data")
    lines = (
        pipeline
        | 'Read From GCS' >> beam.io.ReadFromText(f'gs://{bucket_name}/{destination_blob_name}')
        | 'Extract Features' >> beam.ParDo(ExtractFeatures())
        | 'Flatten The Data' >> beam.ParDo(FlattenJSON())
        | 'Converting UnixTime to IST' >> beam.ParDo(UnixToIst())
        | 'Adding Area Field' >> beam.ParDo(AddingAreaField())
        | 'Adding Ingestion Date' >> beam.ParDo(IngestionDate())
    )
    return lines


def write_to_gcs_as_parquet(pipeline, lines, bucket_name, str_date):
    # Step 4: Write Transformed Data to GCS as Parquet
    logging.info("Step 4: Writing transformed data to GCS as Parquet")
    destination_parquet_blob_name = f'dataflow/silver/{str_date}/historical_flatten_data_{str_date}.parquet'

    # Define the schema for Parquet file
    parquet_schema = SchemaConverter.get_pyarrow_parquet_schema()

    written_parquet_data = (
        lines
        | 'Write Flatten Data to GCS in Parquet' >> beam.io.parquetio.WriteToParquet(
            f'gs://{bucket_name}/{destination_parquet_blob_name}',
            schema=parquet_schema,
            num_shards=1  # Single file output
        )
    )
    logging.info("Step 4: Data transformed and written to GCS in Parquet")


def write_to_bigquery(pipeline, bucket_name):
    # Step 5: Write Parquet Data from GCS to BigQuery
    logging.info("Step 5: Writing Parquet data from GCS to BigQuery")
    gcs_uri = f"gs://{bucket_name}/dataflow/silver/{datetime.now().strftime('%Y%m%d')}/"
    
    # Define the schema for BigQuery table
    bq_schema = SchemaConverter.get_bigquery_schema()

    write_to_bigquery = (
        pipeline
        | 'Read Parquet from GCS' >> beam.io.ReadFromParquet(gcs_uri)
        | 'Write Parquet Data to BigQuery' >> beam.io.WriteToBigQuery(
            table="earthquake-analysis-440806.earthquake_analysis.flattened_historical_parquet_data_by_dataflow_pipeline",
            schema=bq_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,  # Overwrite existing data
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED  # Create table if not exists
        )
    )
    logging.info("Step 5: Parquet data written to BigQuery")


if __name__ == '__main__':
    # Set the environment for Google Cloud authentication
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\harsh\Downloads\Study\Spark Lectures\Projects\earthquake_ingesion_hp\earthquake-analysis-440806-e4fcdf0763f4.json'

    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    # Define the source URL for earthquake data
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    logging.info(f"Data source URL: {url}")
    
    # Create a GCS bucket (if not already created)
    bucket_name = 'earthquake_analysis_by_hp_24'
    create_bucket(bucket_name)
    logging.info(f"Created GCS bucket: {bucket_name}")
    
    # Define the destination path and file name
    str_date = datetime.now().strftime('%Y%m%d')
    destination_blob_name = f'dataflow/landing/historical_data_{str_date}.json'

    # Set pipeline options
    options = set_pipeline_options()

    # Run the pipeline steps
    with beam.Pipeline(options=options) as p:
        # Step 1: Fetch Data from URL 
        fetch_results = fetch_data_from_url(p, url)
        
        # Step 2: Write Data to GCS as JSON
        write_to_gcs_as_json(p, fetch_results, bucket_name, destination_blob_name)

    with beam.Pipeline(options=options) as p2:
        # Step 3: Process and transform the data
        lines = process_and_transform_data(p2, bucket_name, destination_blob_name)

        # Step 4: Write Transformed Data to GCS as Parquet
        write_to_gcs_as_parquet(p2, lines, bucket_name, str_date)
    
    with beam.Pipeline(options=options) as p3:
        # Step 5: Write Parquet Data from GCS to BigQuery
        write_to_bigquery(p3, bucket_name)


    logging.info("Pipeline execution completed successfully")

