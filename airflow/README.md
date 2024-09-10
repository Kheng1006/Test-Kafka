# Airflow service 
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

## Ingest Image feature
To test this feature:
1. Start Minio service
```
Go to minio folder and start the service as described in README
``` 
2. Start Airflow service
```
Go to airflow folder and start the service

```
3. Go to Airflow UI and trigger manually

Note: alter the google.env element according to your folder's id and google account's secret key.
