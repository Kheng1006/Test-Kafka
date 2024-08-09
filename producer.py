from confluent_kafka import Producer
import csv

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'csv-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

csv_file_path = 'test.csv'
topic = 'test-topic'

with open(csv_file_path, mode='r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        message = ','.join(row)  
        producer.produce(topic, value=message, callback=delivery_report)
        producer.poll(0)  

producer.flush()