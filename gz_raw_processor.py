import os
import json
import shutil
from pdf2image import convert_from_path
import tarfile
from concurrent.futures import ProcessPoolExecutor
from TexSoup import TexSoup
import pandas as pd


# Define the directories for storing datasets and extracted figures
dataset_dir = 'dataset'
figures_dir = os.path.join(dataset_dir, 'figures')
RAW_DIR = 's3raw' # Path to directory containing .gz files

# Create directories if they do not exist
if not os.path.exists(dataset_dir):
    os.makedirs(dataset_dir)
    print(f"Created dataset directory: {dataset_dir}")
if not os.path.exists(figures_dir):
    os.makedirs(figures_dir)
    print(f"Created figures directory: {figures_dir}")

def extract_figures_from_gz(gz_file):
    # Extract the paper ID from the file name
    paper_id = os.path.splitext(gz_file)[0]

    # JSON file path for the extracted figures and potential dataset for the current paper
    dataset_path = os.path.join(dataset_dir, f'{paper_id}.parquet')

    # Check if the dataset JSON already exists for the current paper ID and skip processing if it does
    if os.path.exists(dataset_path):
        print(f"Dataset for {paper_id} already exists. Skipping...")
        return
    else:
        print(f"Processing {gz_file}...")

    try:
        with tarfile.open(os.path.join(RAW_DIR, gz_file), mode='r') as tar:
            tmp_dir = os.path.join("./tmp", paper_id)
            tar.extractall(path=tmp_dir)
            print(f"Extracted {gz_file} to {tmp_dir}")

            # Look for .tex files within the temporary directory
            tex_files = [os.path.join(root, name)
                         for root, dirs, files in os.walk(tmp_dir)
                         for name in files if name.endswith(".tex")]
            content = ""
            for tex_file_path in tex_files:
                try:
                    with open(tex_file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        process_tex(content, paper_id, tmp_dir)
                except Exception as e:
                    print(f"Error reading {tex_file_path}: {e}")

            # Delete the temporary directory after processing
            shutil.rmtree(tmp_dir)
            print(f"Removed temporary directory {tmp_dir}")
    except Exception as e:
        print(f"Error extracting {gz_file}: {e}")

def process_all_gz_files():
    # Get a list of .gz files in the raw directory
    gz_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".gz")]
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(extract_figures_from_gz, gz_files)

def save_dataset(dataset, paper_id):
    if not dataset or len(dataset) == 0:
        return

    dataset_path = os.path.join(dataset_dir, f'{paper_id}.parquet')
    # Create a new file if it does not exist
    if not os.path.exists(dataset_path):
        print(f"Creating dataset for {paper_id}")
        df = pd.DataFrame(dataset)
        df.to_parquet(dataset_path)
    else:
        # Extend the existing dataset if the pd file already exists
        cur = pd.read_parquet(dataset_path)
        df = pd.concat([cur,pd.DataFrame(dataset)], ignore_index=True)
        print(f"Updating dataset for {paper_id}")
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
    image_filename = image_filename.replace('figures/', '')[:-4]
    new_image_path = os.path.join(dataset_dir, 'figures', f'{paper_id}_{image_filename}.png')

    try:
        # If the image is a PDF, convert it to PNG using the pdf2image library
        if image_path.lower().endswith('.pdf'):
            images = convert_from_path(image_path)
            pil_image = images[0]
            pil_image.save(new_image_path, format="PNG")
        else:
            # If it's not a PDF, copy the image to the new path
            shutil.copy(image_path, new_image_path)
        #print(f"Saved image as {new_image_path}")
        return new_image_path
    except Exception as e:
        print(f"Error processing image {image_filename}: {e}")
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