import base64
import os
import re
import json
from io import BytesIO
import shutil
import tarfile
from concurrent.futures import ProcessPoolExecutor
#from google.cloud import logging as cloud_logging
from TexSoup import TexSoup
from PIL import Image as PILImage
from pdf2image import convert_from_path


# logging_client = cloud_logging.Client()

# log_name = 'dataset-creation'

# #logger = logging_client.#logger(log_name)

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
        with tarfile.open(os.path.join(RAW_DIR, gz_file), mode='r') as tar:
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
                    #logger.log_text(f"Error reading {tex_file_path}: {e}")
                    print(e)
            res = process_tex(content, paper_id, tmp_dir)
            #logger.log_text(f"processed paper {paper_id}")
            shutil.rmtree(tmp_dir)
        if res:
            pass
            #os.remove(os.path.join(RAW_DIR, gz_file))
    except Exception as e:
        #logger.log_text(f"Error processing gz file {gz_file}: {e}")
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
    full_dataset = []
    image_caption_dataset = []

    figure_pattern = r"(\\begin\{figure\}.*?\\end\{figure\})"

    # Split the text based on the figure pattern, capturing the delimiters (figures)
    parts = re.split(figure_pattern, content, flags=re.DOTALL)
    parts = [part for part in parts if part.strip() != ""]

    bitmap = [1 if re.match(figure_pattern, part, flags=re.DOTALL) else 0 for part in parts]

    for i in range(len(bitmap)):
        if bitmap[i] == 0:
            soup = TexSoup(parts[i])
            fulltext = ""
            for t in soup.text:
                fulltext += t.lstrip().rstrip()
            ''.join(fulltext.split())
            full_dataset.append({'text': fulltext})
        else:
            figure = TexSoup(parts[i])
            image_filename = figure.find('includegraphics')
            if image_filename:
                image_filename = image_filename.text
            else:
                print("no filename")
                continue
            if (len(image_filename) > 1): image_filename = image_filename[-1]
            else: image_filename = image_filename[0]
            image_bytes = None
            if image_filename:
                image_path = os.path.join(tmp_dir, image_filename)
                if os.path.exists(image_path):
                    #convert to png if pdf
                    if image_path.lower().endswith('.pdf'):
                        try:
                            images = convert_from_path(image_path)
                            pil_image = images[0]
                            buff = BytesIO()
                            pil_image.save(buff, format="PNG")
                            image_bytes = base64.b64encode(buff.getvalue()).decode("utf-8")
                        except Exception as e:
                            print(e)
                            continue
                    else:
                        try:
                            pil_image = PILImage.open(image_path)
                            # serialize the image to bytes
                            buff = BytesIO()
                            pil_image.save(buff, format="PNG")
                            image_bytes = base64.b64encode(buff.getvalue()).decode("utf-8")
                        except Exception as e:
                            print(e)
                            continue
                else:
                    continue
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
                caption = ''.join(caption)
            image_data = {
                'image': image_bytes,
                'label': label,
                'caption': caption
            }
            full_dataset.append(image_data)
            image_caption_dataset.append(image_data)

    save_dataset(full_dataset, paper_id)
    save_dataset(image_caption_dataset, paper_id, suffix='image_caption')
    return True

def run():
    process_all_gz_files()

if __name__ == '__main__':
    run()

