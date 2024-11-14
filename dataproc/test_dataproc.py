import pytest
import logging
import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from google.cloud import bigquery
from util import read_json_from_gcs


# Set the environment for Google Cloud authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\harsh\Downloads\Study\Spark Lectures\Projects\earthquake_ingesion_hp\earthquake-analysis-440806-e4fcdf0763f4.json'

# Configure logging for the application
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Generate string representation of the current date in the format 'YYYYMMDD'
str_date = datetime.now().strftime('%Y%m%d')

# GCS bucket and project configuration
bucket_name = 'earthquake_analysis_by_hp_24'
project_id = "earthquake-analysis-440806"
dataset_id = "earthquake_analysis"


# Pytest fixture to create a SparkSession (scope set to "session" to reuse for multiple tests)
@pytest.fixture(scope="session")
def spark_fixture():
    # Create a SparkSession to be used across test cases
    spark = SparkSession.builder.appName("Testing PySpark Fun") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.jars", r"C:\Users\harsh\Downloads\spark-3.3-bigquery-0.41.0.jar") \
    .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")  # Set log level to ERROR 
    
    yield spark
    spark.stop() 


@pytest.fixture
def bigquery_df_data(spark_fixture):
    """Fixture to load BigQuery data."""
    bigquery_df = spark_fixture.read \
        .format("bigquery") \
        .option("project", project_id) \
        .option("dataset", dataset_id) \
        .option("table", "new_flattened_historical_data_by_parquet") \
        .load()
    return bigquery_df


@pytest.fixture
def earthquake_df(spark_fixture):
    """Fixture to read and return the DataFrames for earthquake JSON and BigQuery data."""
    # Step 1: Read the uploaded earthquake JSON data from GCS for expected count
    logger.info("Reading uploaded Earthquake DataFrame JSON data from GCS.")
    destination_blob_name = f'earthquake_df_json_data_{str_date}.json'
    blob_name = f'pyspark_dataproc_testing/Silver/earthquake_df_to_json/{str_date}/{destination_blob_name}'
    
    earthquake_df_json_data = read_json_from_gcs(bucket_name, blob_name)
    
    # Parse JSON strings to dictionaries if the data is a list of JSON strings
    earthquake_df_json_data = [json.loads(record) if isinstance(record, str) else record for record in earthquake_df_json_data] 

    # Create DataFrame from the parsed JSON data
    earthquake_json_df = spark_fixture.createDataFrame(earthquake_df_json_data)
    
    return earthquake_json_df
    


# Test case to check if the row count of JSON data from Earthquake DF matches the row count from BigQuery data
def test_row_count_match(bigquery_df_data, earthquake_df):
    """
    Compares the row count of Earthquake JSON data with that of BigQuery data.
    """
    expected_row_count = earthquake_df.count()
    actual_row_count = bigquery_df_data.count()

    logger.info(f"Earthquake JSON data row count: {expected_row_count}")
    logger.info(f"BigQuery JSON data row count: {actual_row_count}")

    assert expected_row_count == actual_row_count, \
        f"Row count mismatch: Expected {expected_row_count}, but found {actual_row_count}."


# Test case to check if the column names in JSON data from Earthquake DF matches the column names from BigQuery data
def test_column_match(bigquery_df_data, earthquake_df):
    """
    Test function to check column names in both DataFrames.
    """
    earthquake_columns = set(earthquake_df.columns)
    bigquery_columns = set(bigquery_df_data.columns)
    
    logger.info(f"Earthquake JSON columns: {earthquake_columns}")
    logger.info(f"BigQuery columns: {bigquery_columns}")

    missing_columns = earthquake_columns - bigquery_columns
    extra_columns = bigquery_columns - earthquake_columns
    
    if missing_columns or extra_columns:
        logger.error(f"Missing columns: {missing_columns}")
        logger.error(f"Extra columns: {extra_columns}")
        
    assert earthquake_columns == bigquery_columns, \
        f"Column names mismatch: {earthquake_columns} != {bigquery_columns}"


# Test case to check if the data types in JSON data from Earthquake DF matches the data types from BigQuery data
def test_data_dtype():
    """
    The function `test_data_dtype` tests that DataFrame columns have the correct data types after
    transformation by comparing actual and expected data types.
    """
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Specify your project ID and dataset/table name
    dataset = f'{project_id}.{dataset_id}'
    table_id = 'new_flattened_historical_data_by_parquet'

    # Get the table schema
    table = client.get_table(f"{dataset}.{table_id}")  # Fetch the table
    schema = table.schema  # This gives the schema

    # get empty dictionary
    actual_dtypes = dict()
    
    # Print out the schema
    for field in schema:
        actual_dtypes[field.name] = field.field_type
    
    # Test data types (ensure the dtypes match expected schema)
    expected_dtypes = {
        "mag": "FLOAT", 
        "place": "STRING", 
        "time": "STRING", 
        "updated": "STRING", 
        "tz": "STRING", 
        "url": "STRING", 
        "detail": "STRING", 
        "felt": "INTEGER", 
        "cdi": "FLOAT", 
        "mmi": "FLOAT", 
        "alert": "STRING", 
        "status": "STRING", 
        "tsunami": "INTEGER", 
        "sig": "INTEGER", 
        "net": "STRING", 
        "code": "STRING", 
        "ids": "STRING", 
        "sources": "STRING", 
        "types": "STRING", 
        "nst": "INTEGER", 
        "dmin": "FLOAT", 
        "rms": "FLOAT", 
        "gap": "FLOAT", 
        "magType": "STRING", 
        "type": "STRING", 
        "title": "STRING", 
        "longitude": "FLOAT", 
        "latitude": "FLOAT", 
        "depth": "FLOAT", 
        "area": "STRING", 
        "ingestion_dt": "TIMESTAMP"
    }

    assert actual_dtypes == expected_dtypes, (f"DataTypes mismatch: {actual_dtypes} != {expected_dtypes}")


def test_final_goal(bigquery_df_data, earthquake_df, sample_size=100):
    """Test that the final transformed DataFrame matches the expected values by sorting and then taking a random sample."""

    # List of columns that are of type STRING, excluding 'time' and 'updated'
    string_columns = [
        "place", "tz", "url", "detail", 
        "alert", "status", "ids", "sources", "types", 
        "magType", "type", "title", "area"
    ]

    # Filter the DataFrames to include only string columns as data from INT, Float is rounded off and Timestamp data is in diffrent format while converting earthquake_df to JSON by system
    def filter_string_columns(df, string_columns):
        return df.select([col for col in string_columns])

    earthquake_df_str = filter_string_columns(earthquake_df, string_columns)
    bigquery_df_str = filter_string_columns(bigquery_df_data, string_columns)

    # Sort both DataFrames by the 'ids' column
    sorted_expected_data_df = earthquake_df_str.sort("ids")
    sorted_actual_data_df = bigquery_df_str.sort("ids")

    # Take a randomized sample from both DataFrames
    expected_data_sample = sorted_expected_data_df.limit(sample_size).collect()
    actual_data_sample = sorted_actual_data_df.limit(sample_size).collect()

    # Compare the data samples directly
    if expected_data_sample != actual_data_sample:
        # If samples do not match, log the mismatch details
        logger.error("Final Goal Mismatch:")
        logger.error(f"Expected sample data: {expected_data_sample[:10]}...")  
        logger.error(f"Actual sample data: {actual_data_sample[:10]}...")
        raise AssertionError(f"Final Goal Mismatch: Sample data mismatch between expected and actual results.")
    
    
def test_cell_by_cell_string_comparison(bigquery_df_data, earthquake_df):
    """
    Perform cell-by-cell testing for string columns only (excluding 'time' and 'updated').
    Compare values row by row after sorting by the 'ids' column.
    """
    
    # List of string columns to compare (excluding 'time' and 'updated')
    string_columns = [
        "place", "tz", "url", "detail", "alert", "status", "code", "ids", 
        "sources", "types", "magType", "type", "title", "area"
    ]
    
    # Sort both DataFrames by 'ids' column
    sorted_earthquake_df = earthquake_df.sort("ids")
    sorted_bigquery_df = bigquery_df_data.sort("ids")
    
    # Collect data as lists of rows to compare
    expected_data = sorted_earthquake_df.select(*string_columns).collect()
    actual_data = sorted_bigquery_df.select(*string_columns).collect()

    # Perform cell-by-cell comparison for each string column
    for col in string_columns:
        logger.info(f"Comparing column: {col}")
        for expected_row, actual_row in zip(expected_data, actual_data):
            expected_value = expected_row[col]
            actual_value = actual_row[col]

            # Assert equality of the cell-by-cell values for string columns
            assert expected_value == actual_value, (
                f"Mismatch in column '{col}' for row with id '{expected_row['ids']}': "
                f"Expected '{expected_value}' but found '{actual_value}'"
            )

    # If no assertion is raised, it means the cell-by-cell comparison has passed.
    logger.info("Cell-by-cell comparison for string columns passed successfully.")


# To RUN
# pytest test_dataproc.py -vv     