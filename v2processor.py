# %%
import os
import tarfile
import tempfile
from TexSoup import TexSoup, TexNode
import json
import shutil
import re
import argparse
from multiprocessing import Pool

# %%
def process_tar_gz_file(tar_gz_file):
    papers_dir = args.papers_dir
    tar_gz_path = os.path.join(papers_dir, tar_gz_file)
    with tempfile.TemporaryDirectory() as temp_dir:
        extract_tar_gz(tar_gz_path, temp_dir)
        process_extracted_files(temp_dir, tar_gz_file)

def extract_tar_gz(tar_gz_path, extract_path):
    with tarfile.open(tar_gz_path, mode='r:gz') as tar:
        tar.extractall(path=extract_path)

def process_extracted_files(extract_path, tar_gz_file):
    for root, dirs, files in os.walk(extract_path):
        for file in files:
            if file.endswith(".tex"):
                tex_path = os.path.join(root, file)
                process_tex_file(tex_path, tar_gz_file, extract_path)

def process_tex_file(tex_path, tar_gz_file, extract_path):
    with open(tex_path, 'r', encoding='utf-8') as f:
        tex_content = f.read()

        tex_content = tex_content[tex_content.find(r'\begin{document}'):]

        soup = TexSoup(tex_content, tolerance=1)
        interleaved_list = []

        match = re.search(r'(?:arXiv-)?(\d+\.\d+)', tar_gz_file)
        if match:
            paper_id = match.group(1).replace('.', '_')
        else:
            paper_id = 'unknown'

        def traverse_and_interleave(node):
            if isinstance(node, TexNode):
                if node.name == 'section':
                    section_title = node.string
                    if section_title:
                        interleaved_list.append(section_title)
                elif node.name == 'figure':
                    image_filename = node.find('includegraphics')
                    if image_filename:
                        image_options = image_filename.args
                        if isinstance(image_options, list) and len(image_options) > 0:
                            image_filename = str(image_options[-1]).strip()
                            if image_filename.startswith('{') and image_filename.endswith('}'):
                                image_filename = image_filename[1:-1]
                            if image_filename:
                                prefixed_image_filename = f"{paper_id}_{os.path.basename(image_filename)}"
                                copy_image_file(extract_path, image_filename, prefixed_image_filename)
                                img = ('FIGURE:', f'{prefixed_image_filename}')
                                interleaved_list.append(img)
            elif isinstance(node, str):
                text_content = node.strip()
                if text_content:
                    text_content = clean_text_content(text_content)
                    if text_content:
                        interleaved_list.append(text_content)

            for child in getattr(node, 'contents', []):
                traverse_and_interleave(child)

        traverse_and_interleave(soup)

        if not interleaved_list:
            return

        for i in range(len(interleaved_list) - 1):
            if i >= len(interleaved_list) - 1:
                break
            if isinstance(interleaved_list[i], str) and isinstance(interleaved_list[i + 1], str) and interleaved_list[i] == interleaved_list[i + 1]:
                interleaved_list.pop(i + 1)

        i = 0
        while i < len(interleaved_list) - 1:
            if isinstance(interleaved_list[i], str) and isinstance(interleaved_list[i + 1], str) and not interleaved_list[i].endswith(' ') and not interleaved_list[i + 1].startswith(' '):
                interleaved_list[i] = interleaved_list[i] + ' ' + interleaved_list[i + 1]
                interleaved_list.pop(i + 1)
            else:
                i += 1

        save_interleaved_list(tex_path, paper_id, interleaved_list)

def copy_image_file(extract_path, image_filename, prefixed_image_filename):
    image_path = os.path.join(extract_path, image_filename)
    output_image_path = os.path.join(args.output_dir, 'figures', prefixed_image_filename)

    os.makedirs(os.path.dirname(output_image_path), exist_ok=True)

    shutil.copy(image_path, output_image_path)

def save_interleaved_list(tex_path, paper_id, interleaved_list):
    output_filename = f"{paper_id}_{os.path.splitext(os.path.basename(tex_path))[0]}.json"
    output_path = os.path.join(args.output_dir, output_filename)

    os.makedirs(args.output_dir, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(interleaved_list, f, indent=2)

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
    text = re.sub(r'\s+', ' ', text).strip()

    return text

# %%
# papers_dir = "papers"
# process_tar_gz_files(papers_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process tar.gz files and extract interleaved text and images.')
    parser.add_argument('--papers_dir', type=str, required=True, help='Directory containing the tar.gz files.')
    parser.add_argument('--output_dir', type=str, required=True, help='Directory to store the output files.')
    parser.add_argument('--num_processes', type=int, default=4, help='Number of processes to use for parallel processing.')
    args = parser.parse_args()

    tar_gz_files = [file for file in os.listdir(args.papers_dir) if file.endswith(".tar.gz")]

    with Pool(processes=args.num_processes) as pool:
        pool.map(process_tar_gz_file, tar_gz_files)

# %%



