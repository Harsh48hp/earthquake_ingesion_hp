###########################################################################################################
"""
file_name = dynamic_methods.py
description = dynamic functions used for the transformation of the data
date = 2024/11/02
version = 1

"""
############################################################################################################
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType
from pyspark.sql.functions import from_unixtime, col, regexp_extract, current_timestamp, when
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Class for extracting and flattening data
class DataExtractor:
    @staticmethod
    def extract_properties(response: dict) -> dict:
        """
        Extracts properties and coordinates from the earthquake response data.
        Returns:
            dict: Processed dictionary containing earthquake details with longitude, latitude, and depth.
        """
        # logger.info("Extracting earthquake properties.")
        properties = response['properties']
        properties['longitude'] = float(response['geometry']['coordinates'][0])
        properties['latitude'] = float(response['geometry']['coordinates'][1])
        properties['depth'] = float(response['geometry']['coordinates'][2])
        return properties

    @staticmethod
    def flatten_json(data: dict) -> dict:
        """
        Flattens earthquake JSON data, ensuring appropriate types and default values.
        Args:
            data (dict): A single earthquake record.
        Returns:
            dict: Flattened and normalized record.
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

        # Convert fields to appropriate types
        for field, (field_type, default_value) in field_types.items():
            data[field] = field_type(data.get(field, default_value)) if data.get(field) is not None else default_value
        
        return data


# Class for creating and transforming DataFrame
class DataProcessor:
    def __init__(self):
        """ 
        Initializes DataProcessor and sets up a Spark session.
        """
        self.spark = SparkSession.builder \
            .master("local[*]") \
            .appName('Historical Data Load By Parquet') \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

    def earthquake_df_creation(self, processed_records):
        """
        Creates a DataFrame with a predefined schema from processed earthquake records.
        Args:
            processed_records (list): List of flattened and normalized earthquake records.
        Returns:
            DataFrame: Created DataFrame with specified schema.
        """
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

        earthquake_df = self.spark.createDataFrame(processed_records, earthquake_schema)
        return earthquake_df

    def earthquake_transformation_df(self, df):
        """
        Applies transformations to the DataFrame, such as converting timestamps and extracting area information.
        Args:
            df (DataFrame): Input DataFrame with raw earthquake data.
        Returns:
            DataFrame: Transformed DataFrame.
        """
        earthquake_df = df.withColumn('time', from_unixtime(col('time').cast('long')/1000, format="yyyy-MM-dd HH:mm:ss")) \
                          .withColumn('updated', from_unixtime(col('updated').cast('long')/1000, format="yyyy-MM-dd HH:mm:ss")) \
                          .withColumn('area',
                                      when(col("place").contains(" of "), regexp_extract(col("place"), r' of\s*(.*)', 1))
                                      .otherwise(col("place"))) \
                          .withColumn('ingestion_dt', current_timestamp())

        earthquake_df.show(truncate=False)
        earthquake_df.printSchema()
        return earthquake_df





