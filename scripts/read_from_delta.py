from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaLakeToHDFS") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("hdfs://test-kafka-namenode-1:9000/delta-lake/img-topic")
df.show()

spark.stop()

