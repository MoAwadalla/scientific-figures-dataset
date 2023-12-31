# Script for processing source files of research papers
# these source files are .gz files in a directory called s3raw/
# Through the .tex files, look for and find the figures
# Create a .json file such that we replace the figure tags with a json object in the form of
# {
#     'image_filename': 'path/to/image.png',
#     'label': 'label',
#     'caption': 'caption'
# }
# The output, per paper, should be a properly and uniquely named .json using the research paper number
# which is something like 2043.1234
# This .json is essentially a list of json objects that alternate:
# [
#     {'text': 'text'},
#     {
#      'image_filename': 'path/to/image.png',
#      'label': 'label',
#      'caption': 'caption'
#     },
#     {'text': 'text'},
#     ...
# ]

import os
import json
import shutil
import subprocess
import re
import gzip
import glob
import tarfile
import tempfile
import logging
import argparse
import sys
import traceback
import multiprocessing
from multiprocessing import Pool
from PIL import Image as PILImage
from pdf2image import convert_from_path
from TexSoup import TexSoup
from tqdm import tqdm

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dataset_dir = 'dataset'
RAW_DIR = 's3raw'
TMP_DIR = './tmp'
figures_dir = os.path.join(dataset_dir, 'figures')

if not os.path.exists(dataset_dir):
    os.makedirs(dataset_dir)
if not os.path.exists(figures_dir):
    os.makedirs(figures_dir)

def extract_figures_from_gz(arg):
    gz_file, dir = arg
    paper_id = gz_file[:-3]
    # print(paper_id)
    try:
        if any(f.startswith(paper_id.replace('.','_')) for f in dir):
            print(f'skipped {paper_id}')
            return
        print(os.path.join(RAW_DIR, gz_file))
        with tarfile.open(os.path.join(RAW_DIR, gz_file), mode='r:gz') as gz:
            gz.extractall(path=os.path.join(TMP_DIR, paper_id))
            tex_files = [os.path.join(root, name)
                          for root, dirs, files in os.walk(os.path.join(TMP_DIR, paper_id))
                          for name in files if name.endswith(".tex")]
            content = ""
            for tex_file_path in tex_files:
                try:
                    with open(tex_file_path, 'r') as file:
                        content += file.read()
                except Exception as e:
                    print(e)

            process_tex(content, paper_id)
        shutil.rmtree(os.path.join(TMP_DIR, paper_id))
    except Exception as e:
        print(e)

def process_tex(content, paper_id):
    image_caption_dataset = []

    raw_soup = TexSoup(content, tolerance=1)
    figures = raw_soup.find_all('figure')

    for i, figure in enumerate(figures):
        image_filename = figure.find('includegraphics')
        if image_filename:
            image_filename = image_filename.text
        else:
          continue
        if isinstance(image_filename, list):
            image_filename = image_filename[-1]

        caption = figure.find('caption')
        if caption:
            caption = caption.text
        else:
            continue

        label = figure.find('label')
        if label:
            label = label.text

        image_filename = get_image_link(os.path.join(TMP_DIR, paper_id), image_filename, paper_id, i)

        image_caption_dataset.append({
            'image_filename': image_filename,
            'caption': caption,
            'label': label
        })

    save_dataset(image_caption_dataset, paper_id + "_imagesonly")

def save_dataset(dataset, paper_id):
    if dataset == []: return
    paper_id.replace('.', '_')
    dataset_path = os.path.join(dataset_dir, f'{paper_id}.json')
    with open(dataset_path, 'w', encoding='utf-8') as f:
        json.dump(dataset, f, ensure_ascii=False, indent=4)

def get_image_link(tmp_dir, image_filename, paper_id, i):
    if not image_filename:
        return None

    image_path = os.path.join(tmp_dir, image_filename)
    if not os.path.exists(image_path):
        return None

    paper_id = paper_id.replace('.', '_')

    pil_image = None
    try:
        if image_path.lower().endswith('.pdf'):
            images = convert_from_path(image_path)
            pil_image = images[0]
        else:
            pil_image = PILImage.open(image_path)


        new_image_path = os.path.join(dataset_dir, 'figures', f'{paper_id}_{i}.png')

        pil_image.save(new_image_path, format="PNG")

        return new_image_path

    except Exception as e:
        print(e)
        return None

def process_all_gz_files(dir):
    gz_files = [i for i in os.listdir(RAW_DIR) if i.endswith('.gz')]
    for gz_file in tqdm(gz_files):
        extract_figures_from_gz((gz_file, dir))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', type=int, default=1)
    args = parser.parse_args()
    dir = os.listdir(figures_dir)
    dir = set(dir)
    if args.p == 1:
        process_all_gz_files(dir)
    else:
        with Pool(args.p) as p:
            p.map(extract_figures_from_gz, [(i, dir) for i in os.listdir(RAW_DIR) if i.endswith('.gz')])


