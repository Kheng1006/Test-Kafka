# Spark service 
## Start the service for the first time
NOTE: If you already create the network while starting an another service, you can skip the first step
1. Create the test-network network
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
Trino needs to connect to Hive in order to read data from Delta Lake. The default configurations already allow users to read from the Hive instance in the system. However, in case you want to change the Hive server:
1. Modify catalog:
- Access config/catalog/delta.properties.
- Change the hive.metastore.uri to the corresponding metastore.
2. Restart the Trino service:
- Use make restart to restart the service. 
3. Check the connection:
- Access Trino container using docker exec -it demo-trino bash
- Access Trino cli by typing trino
- Try to query the data. The correct syntax is SELECT * FROM <catalog_name>.<table_name>; In our case, the catalog_name is the name of properties file, which is delta. 

