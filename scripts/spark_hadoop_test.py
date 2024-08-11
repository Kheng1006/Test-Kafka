from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToHadoop") \
    .getOrCreate()

# Define Kafka parameters
kafka_broker = "test-kafka-kafka-broker-1:29092"
kafka_topic = "test-topic"

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
     .option("startingOffsets", "earliest") \
    .load()

# The Kafka data is in binary format, so we need to cast the value column to string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
hadoop_path = "hdfs://test-kafka-namenode-1:9000/output_test"

# Write data to Hadoop in append mode
query = df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/kafka-to-hadoop-checkpoint") \
    .option("path", hadoop_path) \
    .outputMode("append") \
    .start()

# Await termination
query.awaitTermination()
