import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os
import json

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\harsh\Downloads\Study\Spark Lectures\GCP_Practice\gcp_learning_project_1\harshal-learning-08-24-64e3eed93ca1.json'

    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "harshal-learning-08-24"
    google_cloud_options.job_name = "loadTxtToBigQuery"
    google_cloud_options.region = "us-central1"
    google_cloud_options.staging_location = "gs://dataproc-temp-us-central1-182872872661-b6tcgbau/stage_loc/"
    google_cloud_options.temp_location = "gs://dataproc-temp-us-central1-182872872661-b6tcgbau/temp_loc/"
    