import os
import json
from io import BytesIO
import shutil
import nltk
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import logging as cloud_logging
from TexSoup import TexSoup

logging_client = cloud_logging.Client()

log_name = 's3scraper'

logger = logging_client.logger(log_name)


# Create dataset directories if they don't exist
dataset_dir = 'dataset'
figures_dir = os.path.join(dataset_dir, 'figures')
RAW_DIR = 's3raw'
if not os.path.exists(dataset_dir):
    os.makedirs(dataset_dir)
if not os.path.exists(figures_dir):
    os.makedirs(figures_dir)


def extract_figures_from_gz(gz_file):
    paper_id = os.path.splitext(gz_file)[0]
    try:
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
            process_tex(content, paper_id, tmp_dir)
            logger.log_text(f"processed paper {paper_id}")
            shutil.rmtree(tmp_dir)
    except Exception as e:
        logger.log_text(f"Error processing gz file {gz_file}: {e}")

def process_all_gz_files():
    gz_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".gz")]
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(extract_figures_from_gz, gz_files)

def save_dataset(dataset, paper_id):
    if dataset == []: return
    dataset_path = os.path.join(dataset_dir, f'{paper_id}.json')
    with open(dataset_path, 'w', encoding='utf-8') as f:
        json.dump(dataset, f, ensure_ascii=False, indent=4)


def process_tex(content, paper_id, tmp_dir):
    soup = TexSoup(content)
    figures = soup.find_all("figure")
    dataset = []
    for figure in figures:
        image_filename = figure.find('includegraphics')
        if image_filename:
            image_filename = image_filename.text
        else:
            print("no filename")
            continue
        if (len(image_filename) > 1): image_filename = image_filename[-1]
        else: image_filename = image_filename[0]
        dest_image_path = None
        if image_filename:
            image_path = os.path.join(tmp_dir, image_filename)
            unique_image_name = f"{paper_id}_{image_filename}".replace('/', '_')
            dest_image_path = os.path.join(figures_dir, unique_image_name)
            if os.path.exists(image_path):
                shutil.copy(image_path, dest_image_path)
        else:
            continue
        label = figure.find('label')
        if label:
            label = ''.join(label.text)
        else:
            print("no label")

        caption = figure.find('caption')

        if caption:
            caption = caption.text
            if (caption[0] == label): caption = caption[1:]
            caption = ''.join(caption) #here
        else:
            print("no caption")

        dataset.append({
            'image_filename': dest_image_path,
            'label': label,
            'caption': caption
        })


    print("for paper " + paper_id + " found " + str(len(dataset)) + " figures")
    # logger.log_text("for paper " + paper_id + " found " + str(len(dataset)) + " figures")

    save_dataset(dataset, paper_id)

def run():
    process_all_gz_files()

if __name__ == '__main__':
    run()