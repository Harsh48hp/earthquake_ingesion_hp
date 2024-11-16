###########################################################################################################
"""
file_name = schema_data.py
description = flow of code to upload a historical earthquake analysis data to bigquery
date = 2024/11/02
version = 1

"""
############################################################################################################

import pyarrow

class SchemaConverter:
    @staticmethod
    def get_pyarrow_parquet_schema():
        """Returns the schema in PyArrow format for Parquet."""
        return pyarrow.schema([
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

    @staticmethod
    def get_bigquery_schema():
        """Returns the schema in BigQuery format."""
        return {
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
