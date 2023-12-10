from google.cloud import storage
import os
import concurrent.futures

def list_files(directory):
    """Recursively list all files in a directory."""
    for root, dirs, files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)

def upload_file_to_gcs(local_file, bucket_name, local_path):
    """Upload a single file to a GCS bucket."""
    # Create a storage client
    storage_client = storage.Client()
    # Get the bucket object
    bucket = storage_client.bucket(bucket_name)
    # Remove the local directory structure to get the remote path
    remote_path = os.path.relpath(local_file, local_path)
    # Create a blob and upload the file
    blob = bucket.blob(os.path.join(remote_path))
    blob.upload_from_filename(local_file)
    print(f"Uploaded {local_file} to {bucket_name}/{remote_path}")

def upload_to_gcs(local_path, bucket_name):
    """Upload files to a GCS bucket in parallel."""
    # Create a ThreadPoolExecutor for parallel uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Create a future to GCS upload for each file
        futures = [executor.submit(upload_file_to_gcs, local_file, bucket_name, local_path)
                   for local_file in list_files(local_path)]
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                # Retrieve the result (for exception handling)
                future.result()
            except Exception as e:
                print(f"An exception occurred: {e}")

# Configure these variables
LOCAL_DIRECTORY = "dataset"
GCP_BUCKET_NAME = "s3dataset"

upload_to_gcs(LOCAL_DIRECTORY, GCP_BUCKET_NAME)