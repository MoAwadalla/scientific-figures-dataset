
import os
import json
from concurrent.futures import ThreadPoolExecutor
from PIL import Image as PILImage
from pdf2image import convert_from_path
from TexSoup import TexSoup
import shutil
import tarfile

dataset_dir = 'dataset'
RAW_DIR = 's3raw'
TMP_DIR = './tmp'

if not os.path.exists(dataset_dir):
    os.makedirs(dataset_dir)
if not os.path.exists(os.path.join(dataset_dir, 'figures')):
    os.makedirs(os.path.join(dataset_dir, 'figures'))


def extract_figures_from_gz(gz_file):
    paper_id = gz_file[:-3]
    print(paper_id)
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
                    with open(tex_file_path, 'r') as file:
                        content += file.read()
                except Exception as e:
                    print(e)
            process_tex(content, paper_id, tmp_dir)
            shutil.rmtree(tmp_dir)
    except Exception as e:
        print(e)


def process_tex(content, paper_id, tmp_dir):
    image_caption_dataset = []
    text_with_image_embedded = []

    raw_soup = TexSoup(content, tolerance=1)
    figures = raw_soup.find_all('figure')

    res_begin = [i for i in range(len(content)) if content.startswith("\\begin{figure}", i)]
    res_end = [i for i in range(len(content)) if content.startswith("\\end{figure}", i)]
    for i in range(len(res_begin)):
        r = res_begin[i]
        e = res_end[i]
        content = content[:r] + " REPLACE ME WITH FIGURE " + content[e:]

    raw_soup = TexSoup(content, tolerance=1)

    fulltext = "".join(raw_soup.text).lstrip().rstrip()

    fulltext_split = fulltext.split("REPLACE ME WITH FIGURE")

    for i, split in enumerate(fulltext_split):
        text_with_image_embedded.append({'text': split})
        if i < len(figures):
            try:
                figure = figures[i]
                image_filename = figure.find('includegraphics')
                image_filename = image_filename.text[1] if image_filename else None
                if not image_filename:
                    continue

                newImageCaption = get_image_link(tmp_dir, image_filename, paper_id, i)

                if not newImageCaption:
                    continue

                label = "".join(figure.find('label').text) if figure.find('label') else ''
                caption = "".join(figure.find('caption').text) if figure.find('caption') else ''

                image_data = {
                    'image': newImageCaption,
                    'label': label,
                    'caption': caption
                }
                text_with_image_embedded.append(image_data)
                image_caption_dataset.append(image_data)
            except Exception as e:
                print(e)
                continue

    save_dataset(text_with_image_embedded, paper_id)
    save_dataset(image_caption_dataset, paper_id, suffix='image_caption')


def get_image_link(tmp_dir, image_filename, paper_id: str, i):
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


def save_dataset(dataset, paper_id, suffix='full'):
    if dataset:
        paper_id = paper_id.replace('.', '_')
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