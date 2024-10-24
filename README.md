# Test-Kafka
This is the repository for implementing demo for Thesis.

## Requirements
+ Docker Compose is required. 

## Initial Setup
In order to start the services:
1. Access the folder with the service's name.
2. Use Makefile to start the service. The detailed instructions are provided in each folder.

## Test Image Fetching feature
The current flow is: read image from a folder => stream to Kafka => read the messages and reconstruct the image using Spark => store the images in MinIO, the metadata in Hadoop (Delta Lake)
To test this feature:
1. Create virtualenv for Kafka Producer
```
python3 -m venv venv
```
2. Install the packages
```
pip install -r requirements.txt
```
3. Initiate Spark Consumer. The detailed instruction for this step is provided in Spark README.md 

4. Initiate Kafka Producer.
```
python3 producer.py
```

5. Check the outcome via:
- MinIO UI (localhost:9000)
- submit read_from_delta.py to view images metadata
### NOTE:
- The consumer MUST be run BEFORE the producer, as the Spark code is configured to read the latest offsets. 
- MinIO bucket and Access Keys must be created and put in .env file. 

## Test Delta Lake - Superset connection
To test the connection between Delta Lake and Superset:
1. Start Services:
- Start Hadoop, Spark, Hive, Trino, and Superset services. 
- In order for the system to work correctly, the services should be started in the same order as suggested. 
2. Import the apartment.csv 
- Run Spark Notebook push_to_delta.ipynb for pushing the data to Delta Lake. 
- Run Spark Notebook register_delta_table.ipynb for registering the apartment Delta Table to Hive. 
3. Connect Trino to Delta Lake:
- Follow the instruction in Trino service for this step.
4. Connect Superset and Trino:
- Follow the instruction in Superset service for this step.


