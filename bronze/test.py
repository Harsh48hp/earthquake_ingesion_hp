

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Create a DataFrame with an Epoch time column
data = [(1729785371710,), (1729785483151,)]
df = spark.createDataFrame(data, ["epoch_time"])

df = df.withColumn('time', from_unixtime(col('epoch_time').cast('long')/1000))
df.toJSON()

print(df.first())

x = '5 km NE of Puebla, B.C., MX' 

print(x.split('of', 1)[1].strip())

# df.toJSON()



schema=[
            bigquery.SchemaField("mag", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("Age", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Gender", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Loyalty Member", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Product Type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("SKU", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Rating", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Order Status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Payment Method", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Total Price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("Unit Price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("Quantity", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Purchase Date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("Shipping Type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Add-ons Purchased", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Add-on Total", "FLOAT", mode="NULLABLE"),
        ]

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