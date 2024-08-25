from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquetFromHDFS") \
    .getOrCreate()

# Define the HDFS path to the Parquet file
parquet_path = "hdfs://demo-hadoop-namenode:9000/output_test"

# Read the Parquet file
df = spark.read.parquet(parquet_path)
# Show the DataFrame content
df.show()

# Print the schema of the DataFrame
df.printSchema()
