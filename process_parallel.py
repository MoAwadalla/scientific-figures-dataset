import os
import re
import json
import base64
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from PIL import Image as PILImage
from pdf2image import convert_from_path
from TexSoup import TexSoup
import shutil

dataset_dir = 'dataset'
RAW_DIR = 's3raw'
TMP_DIR = './tmp'

if not os.path.exists(dataset_dir):
    os.makedirs(dataset_dir)


def extract_figures_from_gz(gz_file):
    paper_id = os.path.splitext(gz_file)[0]
    print(paper_id)
    try:
        with tarfile.open(os.path.join(RAW_DIR, gz_file), mode='r') as tar:
            tmp_dir = os.path.join(TMP_DIR, paper_id)
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
                    print(e)
            process_tex(content, paper_id, tmp_dir)
            shutil.rmtree(tmp_dir)
    except Exception as e:
        print(e)


def process_tex(content, paper_id, tmp_dir):
    image_caption_dataset = []

    figure_pattern = re.compile(r"(\\begin\{figure\}.*?\\end\{figure\})")
    
    parts = re.split(figure_pattern, content, flags=re.DOTALL)
    parts = [part.strip() for part in parts if part.strip()]

    bitmap = [1 if re.match(figure_pattern, part, flags=re.DOTALL) else 0 for part in parts]

    raw_soup = TexSoup(content)
    figures = raw_soup.find_all('figure')

    reconstruct = ""

    for i in range(len(bitmap)):
        reconstruct += parts[i] if bitmap[i] == 0 else " REPLACE ME WITH A FIGURE "

    content_soup = TexSoup(reconstruct)
    fulltext = "".join(t.lstrip().rstrip() for t in content_soup.text if t)

    fulltext_split = [t.strip() for t in fulltext.split("REPLACE ME WITH A FIGURE")]

    for i, split in enumerate(fulltext_split):
        text_with_image_embedded = [{'text': split}]
        if i < len(figures):
            figure = figures[i]
            image_filename = figure.find('includegraphics', recursive=False)
            image_filename = image_filename.text if image_filename else None

            image_bytes = get_image_bytes(tmp_dir, image_filename)

            label = "".join(figure.find('label').text) if figure.find('label') else ''
            caption = "".join(figure.find('caption').text) if figure.find('caption') else ''

            image_data = {
                'image': image_bytes,
                'label': label,
                'caption': caption
            }
            text_with_image_embedded.append(image_data)
            image_caption_dataset.append(image_data)

    save_dataset(text_with_image_embedded, paper_id)
    save_dataset(image_caption_dataset, paper_id, suffix='image_caption')


def get_image_bytes(tmp_dir, image_filename):
    if not image_filename:
        return None

    image_path = os.path.join(tmp_dir, image_filename)
    if not os.path.exists(image_path):
        return None

    try:
        if image_path.lower().endswith('.pdf'):
            images = convert_from_path(image_path)
            pil_image = images[0]
        else:
            pil_image = PILImage.open(image_path)

        buff = BytesIO()
        pil_image.save(buff, format="PNG")
        return base64.b64encode(buff.getvalue()).decode("utf-8")

    except Exception as e:
        print(e)
        return None


def save_dataset(dataset, paper_id, suffix='full'):
    if dataset:
        dataset_path = os.path.join(dataset_dir, f'{paper_id}_{suffix}.json')
        with open(dataset_path, 'w', encoding='utf-8') as f:
            json.dump(dataset, f, ensure_ascii=False, indent=4)


def process_all_gz_files():
    gz_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".gz")]
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(extract_figures_from_gz, gz_files)


def run():
    process_all_gz_files()


if __name__ == '__main__':
    run()