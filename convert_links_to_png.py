from google.cloud import storage
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize Google Cloud Storage Client
client = storage.Client()  # You may need to pass credentials if not running on GCP

# Name of your bucket
bucket_name = 'compileddataset'
bucket = client.bucket(bucket_name)

# List all .json files in the bucket
blobs = bucket.list_blobs()  # Add prefix filter if necessary
json_blobs = [blob for blob in blobs if blob.name.endswith('.json')]

# Function to replace .pdf with .png in 'image_filename'
def replace_pdf_with_png(data):
    for item in data:
        if 'image_filename' in item and item['image_filename'].endswith('.pdf'):
            item['image_filename'] = item['image_filename'].replace('.pdf', '.png')
    return data

# Function to modify and upload a single blob
def process_blob(blob):
    content = blob.download_as_string()  # Download the file contents as string
    data = json.loads(content)  # Parse JSON

    # Replace .pdf with .png
    modified_data = replace_pdf_with_png(data)

    # Convert the modified data back to JSON string
    modified_content = json.dumps(modified_data)

    # Upload the modified JSON string back to the blob
    blob.upload_from_string(modified_content)

# Create a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
    # Submit tasks to the executor
    future_to_blob = {executor.submit(process_blob, blob): blob for blob in json_blobs}

    # Wait for the futures to complete and handle any exceptions
    for future in as_completed(future_to_blob):
        blob = future_to_blob[future]
        try:
            _ = future.result()  # Get the result of the future, if needed
        except Exception as exc:
            print(f"Blob {blob.name} generated an exception: {exc}")
        else:
            print(f"Blob {blob.name} has been processed")

print("All blobs have been processed.")