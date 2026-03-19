import os
import pandas as pd
from google.cloud import storage

DEFAULT_CREDENTIAL_PATH = "~/Desktop/big-data-lab-2-project-7698e9c38781.json"

class GCSHelper:
    """A reusable utility class for interacting with Google Cloud Storage."""
    
    def __init__(self, credentials_path=DEFAULT_CREDENTIAL_PATH):
        """Initializes the GCS client and sets up authentication natively."""
        if credentials_path:
            # Expand the path (handles '~') and set the environment variable
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.expanduser(credentials_path)
        
        if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            raise ValueError("GCP credentials not found. Please set GOOGLE_APPLICATION_CREDENTIALS.")
            
        self.client = storage.Client()

    def upload_local_file(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a local file to a GCS bucket."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"Successfully uploaded {source_file_name} to gs://{bucket_name}/{destination_blob_name}")

    def upload_dataframe(self, df, bucket_name, destination_blob_name):
        """Uploads a Pandas DataFrame directly to GCS without saving it locally first."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        csv_data = df.to_csv(index=False, encoding='utf-8')
        blob.upload_from_string(csv_data, content_type='text/csv')
        print(f"Successfully uploaded DataFrame directly to gs://{bucket_name}/{destination_blob_name}")

    def download_file(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a file from a GCS bucket to the local machine."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f"Successfully downloaded gs://{bucket_name}/{source_blob_name} to {destination_file_name}")