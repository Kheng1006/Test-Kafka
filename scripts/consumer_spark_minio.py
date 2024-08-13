from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, map_from_entries, decode
from pyspark.sql.types import BinaryType, StringType, StructType, StructField
from io import BytesIO
from dotenv import load_dotenv
import cv2
import numpy as np
import os
import base64
from minio import Minio
from PIL import Image
import io

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
HDFS_ENDPOINT = os.getenv("HDFS_ENDPOINT")

def save_image_to_minio(batch_df, batch_id):
     MINIO_CLIENT = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
      
     for row in batch_df.collect():
          media_name = row['media_name']
          image_data = row['image_data']
          original_width = row['original_width']
          original_height = row['original_height']

          metadata = {
               "media_name": row['media_name'],
               "original_width": row['original_width'],
               "original_height": row['original_height']
          }

          extension = os.path.splitext(media_name)[-1]
          
          # Convert the byte array to image
          nparr = np.frombuffer(image_data, np.uint8)
          img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
          img = cv2.resize(img, (original_width, original_height))
          
          pil_img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
          
          img_bytes_io = io.BytesIO()
          pil_img.save(img_bytes_io, format=extension.strip('.').upper())
          img_bytes_io.seek(0)  
          
          file_path = f"{media_name}"
          MINIO_CLIENT.put_object(bucket_name=MINIO_BUCKET, object_name=file_path, metadata=metadata,
                                  data=img_bytes_io, length=img_bytes_io.getbuffer().nbytes)
          
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaImageStream") \
    .getOrCreate()

# Define Kafka source
kafka_stream = spark.readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
     .option("subscribe", KAFKA_TOPIC) \
     .option("includeHeaders", "true") \
     .option("startingOffsets", "latest") \
     .load()

kafka_stream_with_header = kafka_stream.withColumn("headers_dict", map_from_entries(col("headers"))) \
                                        .withColumn("media_name", decode(col("headers_dict").getItem("media_name"), "utf-8")) \
                                        .withColumn("original_height", decode(col("headers_dict").getItem("original_height"), "utf-8").cast("int")) \
                                        .withColumn("original_width", decode(col("headers_dict").getItem("original_width"), "utf-8").cast("int")) \
                                        .selectExpr("media_name", "original_height", "original_width", "CAST(value AS BINARY) AS image_data") 

image_stream = kafka_stream_with_header \
    .selectExpr("media_name", "original_height", "original_width", "image_data")

metadata_stream = kafka_stream_with_header \
     .selectExpr("media_name", "original_height", "original_width")

image_stream = image_stream.writeStream \
                              .foreachBatch(save_image_to_minio) \
                              .start() \
                              

delta_table_path = f"hdfs://{HDFS_ENDPOINT}/delta-lake/{KAFKA_TOPIC}"
delta_table_checkpoint_path = f"hdfs://{HDFS_ENDPOINT}/delta-lake/{KAFKA_TOPIC}/checkpoint"
delta_stream = metadata_stream.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", delta_table_checkpoint_path) \
    .start(delta_table_path) \

image_stream.awaitTermination()
delta_stream.awaitTermination()

