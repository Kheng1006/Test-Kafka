from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaLakeToHDFS") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("hdfs://demo-hadoop-namenode:9000/delta-lake/apartments_new_new")
df.show()

spark.stop()

