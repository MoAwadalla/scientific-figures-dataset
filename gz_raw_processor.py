import os
import shutil
import tarfile
import tempfile
from concurrent.futures import ProcessPoolExecutor
from TexSoup import TexSoup
import pandas as pd
import logging
from time import time
from google.cloud import storage

# Define logger
logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(levelname)s - %(message)s')

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

def download_gz_from_gcp(bucket_name, gz_file, destination):
    # Initialize a GCP storage client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # Download the .gz file from GCP to the local destination
    blob = bucket.blob(gz_file)
    blob.download_to_filename(destination)
    logging.debug(f"Downloaded {gz_file} from GCP bucket to {destination}")

def extract_figures_from_gz(gz_file):
    start_time = time()
    with tempfile.TemporaryDirectory() as down_dir:  # Temporary directory for downloading and extraction
        gz_local_path = os.path.join(down_dir, gz_file)

        # Download the .gz file before processing
        download_gz_from_gcp('raw_gz_arxivs', gz_file, gz_local_path)

        # Extract the paper ID from the file name
        paper_id = os.path.splitext(gz_file)[0]

        # JSON file path for the extracted figures and potential dataset for the current paper
        dataset_path = os.path.join(dataset_dir, f'{paper_id}.parquet')

        # Check if the dataset JSON already exists for the current paper ID and skip processing if it does
        if os.path.exists(dataset_path):
            logging.debug(f"Dataset for {paper_id} already exists. Skipping...")
            return
        else:
            logging.debug(f"Processing {gz_file}...")

        try:
            with tarfile.open(gz_local_path, mode='r') as tar:
                tmp_dir = os.path.join("./tmp", paper_id)
                tar.extractall(path=tmp_dir)
                logging.debug(f"Extracted {gz_file} to {tmp_dir}")

                # Look for .tex files within the temporary directory
                tex_files = [os.path.join(root, name)
                                for root, dirs, files in os.walk(tmp_dir)
                                for name in files if name.endswith(".tex")]
                for tex_file_path in tex_files:
                    try:
                        with open(tex_file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            process_tex(content, paper_id, tmp_dir)
                    except Exception as e:
                        logging.debug(f"Error reading {tex_file_path}: {e}")

                # Delete the temporary directory after processing
                shutil.rmtree(tmp_dir)
                logging.debug(f"Removed temporary directory {tmp_dir}")
        except Exception as e:
            logging.debug(f"Error extracting {gz_file}: {e}")
    end_time = time()
    processing_time = end_time - start_time
    logging.info(f"Processed {gz_file} in {processing_time:.2f} seconds.")


def process_all_gz_files():
    # Initialize a GCP storage client
    client = storage.Client()
    bucket = client.get_bucket('raw_gz_arxivs')

    # Get a list of .gz files in the GCP bucket
    blobs = list(bucket.list_blobs())
    gz_files = [blob.name for blob in blobs]

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(extract_figures_from_gz, gz_files)

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

    image_filename = os.path.basename(image_filename)

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
        if len(image_filename) > 1:
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

        if len(caption) == 0:
            continue


        # Get the path to the destination image
        dest_image_path = get_image_link(tmp_dir, image_filename, paper_id)

        if dest_image_path:
            dataset.append({'image_filename': dest_image_path, 'caption': caption})

    # Save the processed dataset
    save_dataset(dataset, paper_id)

if __name__ == '__main__':
    process_all_gz_files()