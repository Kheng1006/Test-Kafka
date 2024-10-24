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

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv('MINIO_BUCKET')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
FOLDER_ID = os.getenv('FOLDER_ID')
ENV_PATH = os.path.join(os.path.dirname(__file__), '.env')

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
            set_key(ENV_PATH, "REFRESH_TOKEN", refresh_token)
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

def main():
    service = authenticate_gdrive()
    
    folder_id = FOLDER_ID
    
    download_to_minio(service, folder_id)

if __name__ == '__main__':
    main()
