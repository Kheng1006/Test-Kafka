from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

HDFS_ENDPOINT = os.getenv("HDFS_ENDPOINT")

# Initialize Spark session with Delta Lake configurations and Hive support
spark = SparkSession.builder \
    .appName("DeltaLakeToHDFS") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("hive.metastore.uris", "thrift://demo-hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Path to CSV file and HDFS Delta table location
csv_file_path = "./data/apartments.csv"
table_name = 'apartments_new_new'
table_location = f'hdfs://{HDFS_ENDPOINT}/delta-lake/{table_name}'

# Read CSV into DataFrame
df = spark.read.option("delimiter", ";").csv(csv_file_path, header=True, inferSchema=True)

# Show the DataFrame
df.show()

# Create table
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_location}'")


# Write DataFrame to Delta table and register it in Hive metastore
df.write.format("delta").option("mergeSchema", "true").mode("append").insertInto(table_name)

# Stop the Spark session
spark.stop()
