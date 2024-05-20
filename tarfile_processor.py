# %%
import os
import tarfile
import tempfile
from TexSoup import TexSoup, TexNode
import json
import shutil
import re
from collections import defaultdict
from io import BytesIO
from PIL import Image
from pdf2image import convert_from_path


# %%
PAPERS = 'papers'
OUTPUT = 'output'

# %%
failed_tars = set()

# %%
def process_tar_gz_file(tar_gz_path):
    paper_id = ''

    tar_gz_file = os.path.basename(tar_gz_path)

    if tar_gz_file.startswith('arXiv-'):
            match = re.search(r'(?:arXiv-)?(\d+\.\d+)', tar_gz_file)
    else:
        # remove .tar.gz and replace . with _
        match = re.search(r'(\d+\.\d+)', os.path.splitext(tar_gz_file)[0])
    if match:
        paper_id = match.group(1).replace('.', '_')

    # is there a file that starts with the paper id in output?
    if any([file.startswith(f"{paper_id}_") for file in os.listdir(OUTPUT)]):
        print(f"Skipping {tar_gz_file}, {paper_id} already processed")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        extract_tar_gz(tar_gz_path, temp_dir)
        process_extracted_files(temp_dir, tar_gz_file)

def extract_tar_gz(tar_gz_path, extract_path):
    try:
        with tarfile.open(tar_gz_path) as tar:
            tar.extractall(path=extract_path)
    except Exception as e:
        #print(f"Error extracting {tar_gz_path}: {e}")
        failed_tars.add(tar_gz_path)

def process_extracted_files(extract_path, tar_gz_file):
    for root, dirs, files in os.walk(extract_path):
        for file in files:
            if file.endswith(".tex"):
                tex_path = os.path.join(root, file)
                try:
                    process_tex_file(tex_path, tar_gz_file, extract_path)
                except Exception as e:
                    #print(f"Error processing {tar_gz_file} for {tex_path}: {e}")
                    pass

def process_tex_file(tex_path, tar_gz_file, extract_path):
    store_res = defaultdict(list)
    store_res['texts'] = []
    store_res['images'] = []
    store_res['captions'] = []
    image_paths = []
    with open(tex_path, 'r', encoding='utf-8') as f:
        tex_content = f.read()

        if tex_content.find(r'\begin{document}') != -1:
            tex_content = tex_content[tex_content.find(r'\begin{document}'):]

        soup = TexSoup(tex_content, tolerance=1)

        # remove .tar.gz and replace . with _
        paper_id = os.path.splitext(tar_gz_file)[0].replace('.', '_')

        def traverse_and_interleave(node):
            if isinstance(node, TexNode):
                if node.name == 'section':
                    section_title = node.string
                    if section_title:
                        store_res['texts'].append(section_title)
                        store_res['images'].append(None)
                elif node.name in ['figure', 'includegraphics', 'epsfig', 'epsfbox']:
                    image_filenames = []
                    caption = None
                    if node.name == 'figure':
                        includegraphics_nodes = node.find_all('includegraphics')
                        epsfig_nodes = node.find_all('epsfig')
                        epsfbox_nodes = node.find_all('epsfbox')
                        caption_node = node.find('caption')
                        if caption_node:
                            caption = caption_node.text
                            if isinstance(caption, list):
                                caption = ' '.join(caption)
                            caption = clean_text_content(caption)

                        for includegraphics_node in includegraphics_nodes:
                            image_filename = extract_image_filename(includegraphics_node)
                            if image_filename:
                                image_filenames.append(image_filename)

                        for epsfig_node in epsfig_nodes:
                            image_filename = extract_image_filename(epsfig_node)
                            if image_filename:
                                image_filenames.append(image_filename)

                        for epsfbox_node in epsfbox_nodes:
                            image_filename = extract_image_filename(epsfbox_node)
                            if image_filename:
                                image_filenames.append(image_filename)
                    else:
                        image_filename = extract_image_filename(node)
                        if image_filename:
                            image_filenames.append(image_filename)

                    image_filenames = list(set(image_filenames))

                    for image_filename in image_filenames:
                        prefixed_image_filename = f"{paper_id}_{os.path.basename(image_filename)}"
                        #make extension .jpeg
                        prefixed_image_filename = os.path.splitext(prefixed_image_filename)[0] + '.jpeg'

                        if prefixed_image_filename in store_res['images']:
                            continue

                        real_image_path = os.path.join(extract_path, image_filename)

                        image_paths.append(real_image_path)

                        store_res['texts'].append(None)
                        store_res['captions'].append(caption)
                        store_res['images'].append(prefixed_image_filename)

            elif isinstance(node, str):
                text_content = node.strip()
                if text_content:
                    text_content = clean_text_content(text_content)
                    if text_content:
                        store_res['texts'].append(text_content)
                        store_res['images'].append(None)

            for child in getattr(node, 'contents', []):
                traverse_and_interleave(child)

        traverse_and_interleave(soup)

        #combine any consecutive text elements, remove empty elements
        for i in range(len(store_res['texts'])-1, 0, -1):
            if store_res['texts'][i] is not None and store_res['texts'][i-1] is not None:
                store_res['texts'][i-1] += ' ' + store_res['texts'][i]
                store_res['texts'].pop(i)

        for i in range(len(store_res['images'])-1, 0, -1):
            if store_res['images'][i] is None and store_res['images'][i-1] is None:
                store_res['images'].pop(i)

        save_interleaved_list(paper_id, store_res, image_paths)

def extract_image_filename(node):
    if node.name == 'epsfbox':
        image_options = node.args
        if isinstance(image_options, list) and len(image_options) > 0:
            image_filename = str(image_options[0]).strip()
            if image_filename.startswith('{') and image_filename.endswith('}'):
                image_filename = image_filename[1:-1]
            return image_filename
    else:
        image_options = node.args
        if isinstance(image_options, list) and len(image_options) > 0:
            image_filename = str(image_options[-1]).strip()
            if image_filename.startswith('{') and image_filename.endswith('}'):
                image_filename = image_filename[1:-1]
            return image_filename
    return None

def copy_image_file(extract_path, image_filename, prefixed_image_filename):
    image_path = os.path.join(extract_path, image_filename)
    output_image_path = os.path.join(OUTPUT, 'figures', prefixed_image_filename)

    os.makedirs(os.path.dirname(output_image_path), exist_ok=True)

    if os.path.exists(image_path):
        shutil.copy(image_path, output_image_path)
        return True
    else:
        print(f"Image file not found: {image_path}")
        return False

def images_to_tiff_bytes(images, quality=90):
    tiff_bytes = BytesIO()
    images[0].save(tiff_bytes, format="TIFF", save_all=True, append_images=images[1:],
                             quality=quality, compression="jpeg")
    tiff_data = tiff_bytes.getvalue()
    tiff_bytes.close()
    return tiff_data

def save_interleaved_list(paper_id, res, image_paths):
    if len(image_paths) == 0:
        return

    output_filename = f"{paper_id}.json"
    output_path = os.path.join(OUTPUT, output_filename)

    # if pdf, convert then store as pillow, else store as pillow
    pillows = []
    for image_path in image_paths:
        if image_path.endswith('.pdf'):
            pdf_pillows = convert_from_path(image_path, fmt='jpeg')
            pillows.extend(pdf_pillows)
        else:
            image = Image.open(image_path)
            pillows.append(image)

    tiff = images_to_tiff_bytes(pillows)

    if os.path.exists(output_path):
        # open .json file and append to it
        with open(output_path, 'r') as f:
            existing_data = json.load(f)
            res['texts'] = existing_data['texts'] + res['texts']
            res['images'] = existing_data['images'] + res['images']
            res['captions'] = existing_data['captions'] + res['captions']

    #if tiff exists, append to it
    tiff_output_path = os.path.join(OUTPUT, f"{paper_id}.tiff")
    if os.path.exists(tiff_output_path):
        with open(tiff_output_path, 'rb') as f:
            existing_tiff = f.read()
            tiff = existing_tiff + tiff

    with open(output_path, 'w') as f:
        json.dump(res, f)
    with open(tiff_output_path, 'wb') as f:
        f.write(tiff)

def clean_text_content(text):
    text = re.sub(r'\\[a-zA-Z]+', '', text)
    text = re.sub(r'\\[^a-zA-Z]', '', text)
    text = re.sub(r'\{[^}]*\}', '', text)
    text = re.sub(r'\$.*?\$', '', text)
    text = re.sub(r'%.*', '', text)
    text = re.sub(r'width=[\d.]+', '', text)
    text = re.sub(r'[\d.]+pt', '', text)
    text = re.sub(r'[\d.]+in', '', text)
    text = re.sub(r'[\d.]+em', '', text)
    text = re.sub(r',\s*trim=[\d\s]+,\s*clip\s+figures/[\w.]+', '', text)
    text = re.sub(r',\s*trim=[\d\s]+\s+figures/[\w.]+', '', text)
    text = re.sub(r'[\w./]+\.(pdf|png|jpg|jpeg|gif|bmp|tiff|svg)', '', text, flags=re.IGNORECASE)
    text = re.sub(r'trim=[\d\s.]+_?', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^\s*-\s+', '', text)
    text = re.sub(r'^\s*\\\[\s*\\\]\s*', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'\\caption{([^}]*)}', r'\1', text)
    return text

# %%
from tqdm import tqdm

def process_files(tar_gz_files):
    for tar_gz_file in tqdm(tar_gz_files, total=len(tar_gz_files),desc='Processing', unit='file', ncols=80, bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]', colour='green'):
        process_tar_gz_file(tar_gz_file)

# tar_gz_files = [f for f in os.listdir(PAPERS) if f.endswith('.tar.gz')]
# process_files(tar_gz_files)

# %%
# extracts tar to PAPERS directory
# outputs json and tiff files to OUTPUT directory

def main(tar_path):
  # extract tar file
  os.makedirs(OUTPUT, exist_ok=True)
  with tarfile.open(tar_path, mode='r') as tar:
    if not os.path.exists(PAPERS):
      os.makedirs(PAPERS)
    tar.extractall(path=PAPERS)

  # process files
  # recursive listdir to get all files
  files = []
  for root, dirs, filenames in os.walk(PAPERS):
    for filename in filenames:
      files.append(os.path.join(root, filename))

  #tar_gz_files = [f for f in files if f.endswith('.tar.gz') or f.endswith('.gz')]
  tar_gz_files = files
  process_files(tar_gz_files)



# %%
if __name__ == '__main__':
  import sys
  tarfile_path = sys.argv[1]

  #help and usage
  if tarfile_path == '-h' or tarfile_path == '--help':
    print("Usage: python3 extract.py <path to tarfile>")
    sys.exit(0)

  main(tarfile_path)
  print(f"Failed tars: {failed_tars}")


