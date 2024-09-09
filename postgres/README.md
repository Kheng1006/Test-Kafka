# Postgres service 
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

## Integration between Delta Lake and Superset
### Push data to Delta Lake
Run the notebook "test_simple_push_delta" in scripts folder. The detail instruction is provided in the folder.

### Connect to Superset
1. Start psql cmd
Access Postgres Container using
```
docker exec -it demo-postgres bash
```
Start psql cmd using 
```
psql -U postgres
```
2. Enable PL/Python3U extension
PL/Python3U is a PostgreSQL procedural language that allows you to write PostgreSQL functions using Python 3. 
In our case, we utilize this extension to read the data from Delta Lake and return as sql result.
```
CREATE EXTENSION plpython3u;
```
3. Create Function and View for reading data
Delta Lake data can be accessed via Postgres Functions, and Views are necessary if we want to access these functions from Superset.
In psql cmd, run:
```
\i /sql-scripts/create_people_function.sql
```
Then:
```
\i /sql-scripts/create_people_view.sql
```

4. Create Database Connection in Superset
- Access Superset UI via localhost:8088
- Access Database Connections Menu via Settings > Database Connections
- Create a new connection by clicking the icon "+ Database" on top right corner
- Choose PostgreSQL
- Fill in the information. If you use the default settings then:
	- Host: demo-postgres
	- Port: 5432
	- Database name: default
	- User: postgres
	- Password: postgres
- Click Connect

5. Create new dataset
- Click on Dataset menu on navigation bar
- Click "+ Dataset" icon on top right corner
- Fill in the information. If you use the default settings then:
	- Database: the name of connection in the previous step
	- Schema: public
	- Table: people_view






