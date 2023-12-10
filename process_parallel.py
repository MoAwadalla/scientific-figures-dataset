import base64
import os
import re
import json
from io import BytesIO
import shutil
import tarfile
from concurrent.futures import ProcessPoolExecutor
from google.cloud import logging as cloud_logging
from TexSoup import TexSoup
from PIL import Image as PILImage
from pdf2image import convert_from_path


logging_client = cloud_logging.Client()

log_name = 'dataset-creation'

logger = logging_client.logger(log_name)

# Create dataset directories if they don't exist
dataset_dir = 'dataset'
RAW_DIR = 's3raw'
if not os.path.exists(dataset_dir):
    os.makedirs(dataset_dir)


def extract_figures_from_gz(gz_file):
    paper_id = os.path.splitext(gz_file)[0]
    print(paper_id)
    try:
        res = False
        with tarfile.open(os.path.join(RAW_DIR, gz_file), mode='r:gz') as tar:
            tmp_dir = os.path.join("./tmp", paper_id)
            tar.extractall(path=tmp_dir)
            tex_files = [os.path.join(root, name)
                         for root, dirs, files in os.walk(tmp_dir)
                         for name in files if name.endswith(".tex")]
            content = ""
            for tex_file_path in tex_files:
                try:
                    with open(tex_file_path, 'r', encoding='utf-8') as file:
                        content += file.read()
                except Exception as e:
                    logger.log_text(f"Error reading {tex_file_path}: {e}")
                    print(e)
            res = process_tex(content, paper_id, tmp_dir)
            logger.log_text(f"processed paper {paper_id}")
            shutil.rmtree(tmp_dir)
        if res:
            pass
            os.remove(os.path.join(RAW_DIR, gz_file))
    except Exception as e:
        logger.log_text(f"Error processing gz file {gz_file}: {e}")
        print(e)

def process_all_gz_files():
    gz_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".gz")]
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(extract_figures_from_gz, gz_files)

def save_dataset(dataset, paper_id, suffix='full'):
    if dataset == []: return
    dataset_path = os.path.join(dataset_dir, f'{paper_id}_{suffix}.json')
    with open(dataset_path, 'w', encoding='utf-8') as f:
        json.dump(dataset, f, ensure_ascii=False, indent=4)


def process_tex(content, paper_id, tmp_dir):
    try:
        soup = TexSoup(content)
        items = []  # This will contain both text and figures

        # Iterate through the elements in the document
        for elem in soup.contents:
            if isinstance(elem, TexSoup.utils.TokenWithPosition):
                # If the element is text, add it to the dataset
                text = str(elem).strip()
                if text:
                    items.append({'text': text})
            elif elem.name == 'figure':
                image_tag = elem.find('includegraphics')
                image_filename = None
                image_bytes = None
                if image_tag:
                    image_filename = image_tag.args[-1]  # The last argument should be the filename
                    # Further functionality to handle image conversion and base64 encoding...

                label = elem.label.string if elem.label else None
                caption = elem.caption.string if elem.caption else None
                
                image_data = {
                    'image': image_bytes,  # Replace with actual image data
                    'label': label,
                    'caption': caption
                }
                items.append(image_data)

        # Split the full_dataset into full_dataset and image_caption_dataset
        full_dataset, image_caption_dataset = [], []
        for item in items:
            if 'image' in item:
                image_caption_dataset.append(item)
            full_dataset.append(item)

        # Save the datasets
        save_dataset(full_dataset, paper_id)
        save_dataset(image_caption_dataset, paper_id, suffix='image_caption')
        return True

    except Exception as e:
        logger.log_text(f"Error processing LaTeX content: {e}")
        return False

def run():
    process_all_gz_files()

if __name__ == '__main__':
    run()
