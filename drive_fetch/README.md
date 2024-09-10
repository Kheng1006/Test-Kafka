## Ingest Image feature
To test this feature:
1. Start Minio service
```
Go to minio folder and start the service as described in README
``` 
2. Replace .env with your credentials
```
Search for Google Cloud Console

Log in with your Google Account

Create a new project

Go to the API & Services Dashboard.

Click Enable APIs and Services

Search for Google Drive API and enable it.

Go to APIs & Services > Credentials.

Click Create Credentials and choose OAuth 2.0 Client ID.

Set the application type to Desktop App.

Download the credentials.json file.

Delete REFRESH_TOKEN from .env

Replace CLIENT_ID and CLIENT_SECRET in .env with your credentials in the credentials.json file

```
3. Run python script
```
python download.py
```

