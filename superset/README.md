# Superset service 
## Start the service for the first time
NOTE: If you already create the network while starting an another service, you can skip the first step
1. Create the custom-network network
```
make network
```
2. Build the image 
```
make build
```
3. Run docker compose
```
make up
```

## Restart the service
```
make restart
```
## Stop the service without deleting containers
```
make stop
```
## Stop the service and delete containers
```
make down

```

## Connect to Delta Lake
In order to connect to Delta Lake, we first need to make Trino connect to Delta Lake. Please refer to Trino section for more detail instructions. 
For connecting to Trino:
1. Access Superset UI:
- Access Superset UI via localhost:8090. The default username and password are admin-admin, respectively.
2. Create a database connection:
- Click on the Settings >> Data Connections on top-right corner of the screen. 
- Click on +DATABASE
- Choose Trino as a database
- Input the Trino SQLAlchemy. If you use the default configurations, it should be: trino://trino@demo-trino:8080/delta
- Click Test Connection; if the connection is good, click Create. 
3. Create a dataset: A dataset is typically equal to a table in your database; Superset needs to register it in order to read the data from it. 
- Click on Plus Sign on the top-right corner of the screen.
- Click on +DATASET
- Fill in the information, including the Database, Schema and Table. 
- Click Create Dataset and Create Chart.

## Connect to other databases
Connecting to other databases requires similar procedure; however, you are required to do some extra work to install the PyPi packages for the corresponding database first. 
1. Modify requirements-local.txt:
- Access docker >> requirements-local.txt
- Add the PyPi driver for the corresponding database to the files. The list of supported databases can be found [here](https://superset.apache.org/docs/configuration/databases/).
2. Restart the service:
- Use make restart to restart Superset service. 

