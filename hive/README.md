# Hive service 
## Start the service for the first time
NOTE: If you already create the network while starting an another service, you can skip the second step
1. Create a .env file with the following attributes: 
	- POSTGRES_DB: name of postgres database for storing hive data
	- POSTGRES_USER: name of postgres user 
	- POSTGRES_PASSWORD: password for postgres user
2. Create the custom-network network
```
make network
```
3. Build the image 
```
make build
```
4. Run docker compose
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


