from kafka import KafkaConsumer
from hdfs import InsecureClient

consumer = KafkaConsumer(
    'csv-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group'
)

client = InsecureClient('http://localhost:9870', user='hdfs')

with client.write('/output.csv', overwrite=True) as writer:
    for message in consumer:
        writer.write(message.value.decode('utf-8') + '\n')
