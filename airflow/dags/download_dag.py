from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import io
from dotenv import load_dotenv, set_key
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from minio import Minio
from minio.error import S3Error

# env_path = r'C:\Data\BK HK241\Thesis\New folder\Test-Kafka\airflow\.env'

# load_dotenv()

dotenv_path = '/airflow/google.env'
load_dotenv(dotenv_path=dotenv_path)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'minio_user'
MINIO_SECRET_KEY = 'minio_password'
BUCKET_NAME = 'drive-fetch'
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
FOLDER_ID = os.getenv('FOLDER_ID')

# Initialize MinIO client using credentials from .env
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def authenticate_gdrive():
    # creds = Credentials(
    #     None,
    #     refresh_token=REFRESH_TOKEN,
    #     client_id=CLIENT_ID,
    #     client_secret=CLIENT_SECRET,
    #     token_uri='https://oauth2.googleapis.com/token'
    # )

    # if creds and creds.expired and creds.refresh_token:
    #     creds.refresh(Request())

    creds = None

    if REFRESH_TOKEN:
        creds = Credentials(
            None,
            refresh_token=REFRESH_TOKEN,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            token_uri='https://oauth2.googleapis.com/token'
        )
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
    else:
        # Perform OAuth2 flow for the first time to get credentials
        flow = InstalledAppFlow.from_client_config({
            "installed": {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }, SCOPES)
        creds = flow.run_local_server(port=0)
        
        # Save the refresh token to the .env file
        refresh_token = creds.refresh_token
        if refresh_token:
            set_key(dotenv_path, "REFRESH_TOKEN", refresh_token)
            print(f"New refresh token saved to .env: {refresh_token}")

    return build('drive', 'v3', credentials=creds)

def download_to_minio(service, folder_id):
    query = f"'{folder_id}' in parents"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        print("No files found.")
        return

    for file in files:
        file_id = file['id']
        file_name = file['name']

        request = service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()

        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Downloading {file_name}: {int(status.progress() * 100)}%.")

        file_stream.seek(0)

        try:
            minio_client.put_object(
                BUCKET_NAME,
                file_name,
                data=file_stream,
                length=-1,
                part_size=10*1024*1024 
            )
            print(f"Successfully uploaded {file_name} to MinIO.")
        except S3Error as e:
            print(f"Failed to upload {file_name} to MinIO: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'download_to_minio_dag',
    default_args=default_args,
    description='Download files from Google Drive to MinIO',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 28),
    catchup=False,
) as dag:

    # Task to download and upload files
    download_task = PythonOperator(
        task_id='download_and_upload_to_minio',
        python_callable=download_to_minio(authenticate_gdrive(), FOLDER_ID),
    )

    download_task
