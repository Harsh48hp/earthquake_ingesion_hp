###########################################################################################################
"""
file_name = dynamic_methods.py
description = dynamic functions used for the transformation of the data
date = 2024/11/02
version = 1

"""
############################################################################################################
import apache_beam as beam
from util import request_url
from datetime import datetime
import json
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# The class `FetchDataFromUrl` is a Beam `DoFn` subclass that fetches data from a specified URL
class FetchDataFromUrl(beam.DoFn):
    def __init__(self, url):
        self.url = url

    def process(self, elment):
        """Fetch JSON data from a URL and yield it, or output an error if it fails."""
        logger.info(f"Fetching data from URL: {self.url}")
        data = request_url(self.url)
        if data is not None:
            logger.info("Data fetched successfully")
            yield json.dumps(data)
        else:
            logger.error(f"Failed to fetch data from {self.url}")
            yield beam.pvalue.TaggedOutput('error', f'Error fetching data from {self.url}')


# The `ExtractFeatures` class defines a `process` method that extracts features from JSON data.
class ExtractFeatures(beam.DoFn):
    def process(self, record):
        """Extract features from JSON data."""
        try:
            json_data = json.loads(record)
            features = json_data.get('features', [])
            logger.info(f"Extracted {len(features)} features")
            for feature in features:
                yield feature
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON: {record}")


# flatten nested JSON fields into a flat dictionary
class FlattenJSON(beam.DoFn):
    def process(self, element):
        """Flatten JSON to extract specific fields."""
        try:
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
            logger.debug("Flattened JSON element")
            yield output
        except Exception as e:
            logger.error(f"Error flattening JSON: {e}")


# convert Unix timestamps to ISO format in IST timezone
class UnixToIst(beam.DoFn):
    def process(self, record):
        try:
            time = record.get('time', 0)
            updated = record.get('updated', 0)
            if time and updated:
                record['time'] = datetime.fromtimestamp(time / 1000)  
                record['updated'] = datetime.fromtimestamp(updated / 1000) 
            logger.debug("Converted timestamps to ISO format")
            yield record
        except Exception as e:
            logger.error(f"Error converting timestamps: {e}")


# Add an 'area' field based on the 'place' field
class AddingAreaField(beam.DoFn):
    def process(self, record):
        place = record.get('place')
        if place and ' of ' in place:
            record['area'] = place.split(' of ', 1)[1].strip()
        else:
            record['area'] = place
        
        yield record


# Add the current ingestion date to each record
class IngestionDate(beam.DoFn):
    def process(self, record):
        record['ingestion_dt'] = datetime.now()  # Convert to ISO format
        yield record
