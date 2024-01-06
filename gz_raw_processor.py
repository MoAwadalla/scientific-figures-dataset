import os
import shutil
import gzip
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from TexSoup import TexSoup
import pandas as pd
import logging
from tqdm import tqdm
from google.cloud import storage

# Define logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the GCS client and bucket reference
gcs_client = storage.Client(project="scientific-dataset-figures")
bucket = gcs_client.bucket('raw_gz_arxivs')

# Define the directories for storing datasets and extracted figures
dataset_dir = 'dataset'
figures_dir = os.path.join(dataset_dir, 'figures')

# Create directories if they do not exist
if not os.path.exists(dataset_dir):
    os.makedirs(dataset_dir)
if not os.path.exists(figures_dir):
    os.makedirs(figures_dir)

def list_gz_files_from_gcs():
    """List all .gz files in the GCS bucket."""
    blobs = bucket.list_blobs()
    return [blob.name for blob in blobs if blob.name.endswith('.gz')]

def download_gz_file_from_gcs(gz_file):
    """Download .gz file from GCS."""
    tmp_file_path = os.path.join('/tmp', gz_file)
    blob = bucket.blob(gz_file)
    blob.download_to_filename(tmp_file_path)
    return tmp_file_path

def extract_figures_from_gz(gz_file):
    paper_id = os.path.splitext(os.path.basename(gz_file))[0]
    dataset_path = os.path.join(dataset_dir, f'{paper_id}.parquet')

    if os.path.exists(dataset_path):
        logging.info(f"Dataset for {paper_id} already exists. Skipping...")
        return

    logging.info(f"Processing {gz_file}...")

    try:
        # Download .gz file from GCS
        tmp_file_path = download_gz_file_from_gcs(gz_file)
        tmp_dir = os.path.join("/tmp", paper_id)

        # Create a directory to extract the file contents
        os.makedirs(tmp_dir, exist_ok=True)

        # Extract the .gz file
        if tarfile.is_tarfile(tmp_file_path):
            with tarfile.open(tmp_file_path, mode='r') as tar:
                tar.extractall(path=tmp_dir)
        else:
            with gzip.open(tmp_file_path, 'rb') as gz_file:
                with open(os.path.join(tmp_dir, paper_id), 'wb') as out_file:
                    shutil.copyfileobj(gz_file, out_file)

        # Process the extracted contents
        process_directory(tmp_dir, paper_id)

        # Clean up: remove the temporary directory and file
        shutil.rmtree(tmp_dir)
        os.remove(tmp_file_path)

    except Exception as e:
        logging.error(f"Error processing {gz_file}: {e}")

def process_directory(tmp_dir, paper_id):
    # Process files in the temporary directory (e.g., .tex files)
    for root, _, files in os.walk(tmp_dir):
        for name in files:
            if name.endswith('.tex'):
                tex_file_path = os.path.join(root, name)
                process_tex_file(tex_file_path, paper_id)

def process_tex_file(tex_file_path, paper_id):
    with open(tex_file_path, 'r', encoding='utf-8') as tex_file:
        content = tex_file.read()
        process_tex_content(content, paper_id)

def process_tex_content(content, paper_id):
    soup = TexSoup(content, tolerance=1)
    figures = soup.find_all('figure')
    dataset = []

    for figure in figures:
        img_tag = figure.find('includegraphics')
        if img_tag:
            img_path = img_tag.get('src')  # Assuming the source is in 'src' attribute
            caption_tag = figure.find('caption')
            if caption_tag and img_path:
                dataset.append({
                    'image_filename': img_path,
                    'caption': ''.join(caption_tag.contents)
                })

    # Save the processed dataset to a file
    save_dataset(dataset, paper_id)

def save_dataset(dataset, paper_id):
    if not dataset:
        return

    # Replace periods in paper ID for consistency
    paper_id = paper_id.replace('.', '_')
    dataset_path = os.path.join(dataset_dir, f'{paper_id}.parquet')

    # Save or update the dataset
    if os.path.exists(dataset_path):
        cur_df = pd.read_parquet(dataset_path)
        new_df = pd.DataFrame(dataset)
        updated_df = pd.concat([cur_df, new_df], ignore_index=True)
        updated_df.to_parquet(dataset_path)
    else:
        pd.DataFrame(dataset).to_parquet(dataset_path)

def process_all_gz_files():
    gz_files = list_gz_files_from_gcs()
    gz_files = gz_files[:10]
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(extract_figures_from_gz, gz_file) for gz_file in gz_files]
        for future in tqdm(as_completed(futures), total=len(gz_files), desc="Processing .gz files", unit="file"):
            pass

if __name__ == '__main__':
    process_all_gz_files()