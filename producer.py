from kafka import KafkaProducer
import csv

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open('./test.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        message = ','.join(row)
        producer.send('csv-data', value=message.encode('utf-8'))
producer.flush()
