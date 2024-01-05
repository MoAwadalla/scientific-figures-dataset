import os
import shutil
import tarfile
from concurrent.futures import ProcessPoolExecutor
from TexSoup import TexSoup
import pandas as pd
import logging
from time import time
from google.cloud import storage
from tqdm import tqdm

# Define logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the GCS client and bucket reference
gcs_client = storage.Client()
bucket = gcs_client.bucket('raw_gz_arxivs')

# Define the directories for storing datasets and extracted figures
dataset_dir = 'dataset'
figures_dir = os.path.join(dataset_dir, 'figures')

# Create directories if they do not exist
if not os.path.exists(dataset_dir):
    os.makedirs(dataset_dir)
    logging.debug(f"Created dataset directory: {dataset_dir}")
if not os.path.exists(figures_dir):
    os.makedirs(figures_dir)
    logging.debug(f"Created figures directory: {figures_dir}")

def list_gz_files_from_gcs():
    """List all .gz files in the GCS bucket."""
    blobs = bucket.list_blobs()
    return [blob.name for blob in blobs if blob.name.endswith('.gz')]

def download_gz_file_from_gcs(gz_file, tmp_file_path):
    """Download .gz file from GCS to a local temporary directory."""
    blob = bucket.blob(gz_file)
    blob.download_to_filename(tmp_file_path)

def extract_figures_from_gz(gz_file):
    paper_id = os.path.splitext(gz_file)[0]
    dataset_path = os.path.join(dataset_dir, f'{paper_id}.parquet')

    if os.path.exists(dataset_path):
        logging.debug(f"Dataset for {paper_id} already exists. Skipping...")
        return
    else:
        logging.debug(f"Processing {gz_file}...")

    try:
        # Set up the full path for the local temporary file
        tmp_file_path = os.path.join('/tmp', gz_file)

        # Download file from GCS to temporary file
        download_gz_file_from_gcs(gz_file, tmp_file_path)

        with tarfile.open(tmp_file_path, mode='r') as tar:
            tmp_dir = os.path.join("./tmp", paper_id)
            tar.extractall(path=tmp_dir)
            logging.debug(f"Extracted {gz_file} to {tmp_dir}")

            # (Rest of existing code for processing .tex files goes here.)

            # Delete the temporary directory and downloaded .gz file after processing
            shutil.rmtree(tmp_dir)
            os.remove(tmp_file_path)
            logging.debug(f"Removed temporary directory {tmp_dir} and .gz file {tmp_file_path}")

    except Exception as e:
        logging.debug(f"Error processing {gz_file}: {e}")

def process_all_gz_files():
    gz_files = list_gz_files_from_gcs()
    gz_files = gz_files[:10]
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Wrap gz_files with tqdm for the progress bar display
        list(tqdm(executor.map(extract_figures_from_gz, gz_files), total=len(gz_files), desc='Processing .gz files'))


def save_dataset(dataset, paper_id):
    if not dataset or len(dataset) == 0:
        return

    paper_id = paper_id.replace('.', '_')

    dataset_path = os.path.join(dataset_dir, f'{paper_id}.parquet')
    # Create a new file if it does not exist
    if not os.path.exists(dataset_path):
        logging.debug(f"Creating dataset for {paper_id}")
        df = pd.DataFrame(dataset)
        df.to_parquet(dataset_path)
    else:
        # Extend the existing dataset if the pd file already exists
        cur = pd.read_parquet(dataset_path)
        df = pd.concat([cur,pd.DataFrame(dataset)], ignore_index=True)
        logging.debug(f"Updating dataset for {paper_id}")
        df.to_parquet(dataset_path)

def get_image_link(tmp_dir, image_filename, paper_id):
    # Construct the path to the image file
    if not image_filename:
        return None

    image_path = os.path.join(tmp_dir, image_filename)
    if not os.path.exists(image_path):
        return None

    # Replace periods in paper ID with underscores for consistency in the filename
    paper_id = paper_id.replace('.', '_')
    image_filename = image_filename.replace('figures/', '')
    new_image_path = os.path.join(dataset_dir, 'figures', f'{paper_id}_{image_filename}')

    try:
        shutil.copy(image_path, new_image_path)
        return new_image_path
    except Exception as e:
        logging.debug(f"Error processing image {image_filename}: {e}")
        return None

def process_tex(content, paper_id, tmp_dir):
    soup = TexSoup(content, tolerance=1)
    figures = soup.find_all("figure")
    dataset = []

    # Process each 'figure' element found in the TeX content
    for figure in figures:
        image_filename = figure.find('includegraphics')
        if image_filename:
            image_filename = image_filename.text
        else:
            continue

        # Extract the appropriate filename from the 'includegraphics' tag
        if (len(image_filename) > 1):
            image_filename = image_filename[-1]
        else:
            image_filename = image_filename[0]

        # Extract the 'caption' tag's text from the figure
        caption = figure.find('caption')
        if caption:
            caption = caption.text
            caption = "".join(caption)
        else:
            continue

        # Get the path to the destination image
        dest_image_path = get_image_link(tmp_dir, image_filename, paper_id)
        if not dest_image_path:
            continue

        if not (dest_image_path and caption):
            continue

        # Add the extracted information to the dataset
        dataset.append({'image_filename': dest_image_path, 'caption': caption})

    # Save the processed dataset
    save_dataset(dataset, paper_id)

def run():
    process_all_gz_files()

if __name__ == '__main__':
    run()