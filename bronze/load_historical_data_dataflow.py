import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from util import request_url, create_bucket
import os
import logging
import json
import pyarrow
from datetime import datetime

# The class `FetchDataFromUrl` is a Beam `DoFn` subclass that fetches data from a specified URL
class FetchDataFromUrl(beam.DoFn):
    def __init__(self, url):
        self.url = url

    def process(self):
    # yields the JSON representation of the data or an error message if the data retrieval fails.
        data = request_url(self.url)
        if data is not None:
            yield json.dumps(data)
        else:
            yield beam.pvalue.TaggedOutput('error', f'Error fetching data from {self.url}')


# The `ExtractFeatures` class defines a `process` method that extracts features from JSON data.
class ExtractFeatures(beam.DoFn):
    def process(self, record):
        try:
            json_data = json.loads(record)
            features = json_data.get('features', [])
            for feature in features:
                yield feature
        except json.JSONDecodeError:
            logging.error(f'Error decoding JSON: {record}')


# flatten nested JSON fields into a flat dictionary
class FlattenJSON(beam.DoFn):
    def process(self, element):
        output = {
            "mag": element.get("properties", {}).get("mag", 0.0),
            "place": element.get("properties", {}).get("place", ''),
            "time": element.get("properties", {}).get("time", '0'),
            "updated": element.get("properties", {}).get("updated", '0'),
            "tz": element.get("properties", {}).get("tz", ''),
            "url": element.get("properties", {}).get("url", ''),
            "detail": element.get("properties", {}).get("detail", ''),
            "felt": element.get("properties", {}).get("felt", 0),
            "cdi": element.get("properties", {}).get("cdi", 0.0),
            "mmi": element.get("properties", {}).get("mmi", 0.0),
            "alert": element.get("properties", {}).get("alert", ''),
            "status": element.get("properties", {}).get("status", ''),
            "tsunami": element.get("properties", {}).get("tsunami", 0),
            "sig": element.get("properties", {}).get("sig", 0),
            "net": element.get("properties", {}).get("net", ''),
            "code": element.get("properties", {}).get("code", ''),
            "ids": element.get("properties", {}).get("ids", ''),
            "sources": element.get("properties", {}).get("sources", ''),
            "types": element.get("properties", {}).get("types", ''),
            "nst": element.get("properties", {}).get("nst", 0),
            "dmin": element.get("properties", {}).get("dmin", 0.0),
            "rms": element.get("properties", {}).get("rms", 0.0),
            "gap": element.get("properties", {}).get("gap", 0.0),
            "magType": element.get("properties", {}).get("magType", ''),
            "type": element.get("properties", {}).get("type", ''),
            "title": element.get("properties", {}).get("title", ''),
            "longitude": element.get("geometry", {}).get("coordinates", [0.0, 0.0, 0.0])[0],
            "latitude": element.get("geometry", {}).get("coordinates", [0.0, 0.0, 0.0])[1],
            "depth": element.get("geometry", {}).get("coordinates", [0.0, 0.0, 0.0])[2]
        }
        yield output


# convert Unix timestamps to ISO format in IST timezone
class UnixToIst(beam.DoFn):
    def process(self, record):
        time = record.get('time')
        updated = record.get('updated')
        if time and updated:
            record['time'] = datetime.fromtimestamp(time / 1000)  
            record['updated'] = datetime.fromtimestamp(updated / 1000) 
        yield record


# add an 'area' field based on the 'place' field
class AddingAreaField(beam.DoFn):
    def process(self, record):
        place = record.get('place')
        if place and 'of' in place:
            record['area'] = place.split('of')[1].strip(' ')
        else:
            record['area'] = None
        yield record


# add the current ingestion date to each record
class IngestionDate(beam.DoFn):
    def process(self, record):
        record['ingestion_dt'] = datetime.now()  # Convert to ISO format
        yield record


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\harsh\Downloads\Study\Spark Lectures\GCP_Practice\gcp_learning_project_1\harshal-learning-08-24-64e3eed93ca1.json'

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "harshal-learning-08-24"
    google_cloud_options.job_name = "loadParquetToBigQuery"
    google_cloud_options.region = "us-central1"
    google_cloud_options.staging_location = "gs://dataproc-temp-us-central1-182872872661-b6tcgbau/stage_loc/"
    google_cloud_options.temp_location = "gs://dataproc-temp-us-central1-182872872661-b6tcgbau/temp_loc/"
    
    # Setup logging and error level
    logging.basicConfig(level=logging.ERROR)
    
    #  define earthquake data source url
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    
    # Create a new GCS bucket
    bucket_name = 'earthquake_analysis_by_hp'
    create_bucket(bucket_name)
    
    str_date = datetime.now().strftime('%Y%m%d')
    destination_blob_name = f'dataflow/landing/historical_data_{str_date}.json'
    
    
    # define the beam pipeline
    with beam.Pipeline(options=options) as p:
        
        # step 1: Fetch the data from url
        fetch_results = (
            p
            | 'Create API request' >> beam.Create([None])  # Trigger the pipeline
            | 'Fetch Data From API' >> beam.ParDo(FetchDataFromUrl(url)).with_outputs('error', main='main')
        )
        
        # step 2: write the data to gcs as json
        written_data = (
            fetch_results.main 
            | 'Write To GCS In JSON' >> beam.io.WriteToText(
                f'gs://{bucket_name}/{destination_blob_name}',
                file_name_suffix='',
                shard_name_template='',  # No sharding
                num_shards=1  # Single file output
            )
        )

        # step 3: Read from GCS then process and transform the data
        lines = (
            p
            | 'Read From GCS' >> beam.io.ReadFromText(f'gs://{bucket_name}/{destination_blob_name}')
            | 'Extract Features' >> beam.ParDo(ExtractFeatures())
            | 'Flatten The Data' >> beam.ParDo(FlattenJSON())
            | 'Converting time and updated From UnixTime to IST' >> beam.ParDo(UnixToIst())
            | 'Adding area Field to Data From place field' >> beam.ParDo(AddingAreaField())
            | 'Adding Ingestion Date field' >> beam.ParDo(IngestionDate())
        )
        
        
        # Step 4: Write transformed data back to GCS as Parquet
        destination_blob_name = f'dataflow/silver/historical_flatten_data_{str_date}.parquet'
        
        # Define the schema for Parquet file
        schema = pyarrow.schema([
            ('mag', pyarrow.float32()),
            ('place', pyarrow.string()),
            ('time', pyarrow.timestamp('ms')),  
            ('updated', pyarrow.timestamp('ms')),  
            ('tz', pyarrow.string()),
            ('url', pyarrow.string()),
            ('detail', pyarrow.string()),
            ('felt', pyarrow.int32()),
            ('cdi', pyarrow.float32()),
            ('mmi', pyarrow.float32()),
            ('alert', pyarrow.string()),
            ('status', pyarrow.string()),
            ('tsunami', pyarrow.int32()),
            ('sig', pyarrow.int32()),
            ('net', pyarrow.string()),
            ('code', pyarrow.string()),
            ('ids', pyarrow.string()),
            ('sources', pyarrow.string()),
            ('types', pyarrow.string()),
            ('nst', pyarrow.int32()),
            ('dmin', pyarrow.float32()),
            ('rms', pyarrow.float32()),
            ('gap', pyarrow.float32()),
            ('magType', pyarrow.string()),
            ('type', pyarrow.string()),
            ('title', pyarrow.string()),
            ('longitude', pyarrow.float32()),
            ('latitude', pyarrow.float32()),
            ('depth', pyarrow.float32()),
            ('area', pyarrow.string()),
            ('ingestion_dt', pyarrow.timestamp('ms'))  
        ])
        
        # Convert to Parquet format and write to GCS
        written_flatten_data_gcs = (
            lines 
            | 'Write Flatten Data To GCS in Parquet' >> beam.io.parquetio.WriteToParquet(
                f'gs://{bucket_name}/{destination_blob_name}',
                schema=schema,
                num_shards=1  # To write a single file
            )
        )
        
        # step 5: Write the parquet file from gcs to bigQuery table
        bucket_name = "earthquake_analysis_by_hp"
        gcs_blob_name = "dataflow/silver/"
        gcs_uri = f"gs://{bucket_name}/{gcs_blob_name}"
        
        # Define the schema for bq table
        bq_schema = {
            "fields": [
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
            ]
        }
        
        # reading the parquet data from gcs and loading into bq table
        write_to_bigquery = (
            p
            | 'Reading Parquet Data From GCS' >> beam.io.ReadFromParquet(gcs_uri)
            | 'Writing Parquet Data To Bigquery' >> beam.io.WriteToBigQuery(
                table="harshal-learning-08-24.earthquake_analysis.flattned_historical_data_by_parquet_by_dataflow",
                schema=bq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,  # Overwrite existing data
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
        