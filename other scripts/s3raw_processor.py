import os
import tarfile
from concurrent.futures import ProcessPoolExecutor

def extract_gz_from_tar(tar_path, extraction_path):
    try:
        with tarfile.open(tar_path, 'r') as tar:
            # Filter out all non-gz files
            gz_members = [m for m in tar.getmembers() if m.name.endswith('.gz')]
            # Extract .gz files and place them directly into the extraction_path
            for member in gz_members:
                member.name = os.path.basename(member.name)  # Remove the directory structure
                tar.extract(member, path=extraction_path)
        print(f"Extracted .gz files from {tar_path}")
    except Exception as e:
        print(f"Error processing {tar_path}: {e}")



def process_tar_file(tar_path, extraction_path):
    # Extract .gz files from tar
    extract_gz_from_tar(tar_path, extraction_path)


def main():
    directory = 's3raw'
    tar_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.tar')]

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(process_tar_file, tar_files, [directory] * len(tar_files))

    print("All tars extracted.")

if __name__ == "__main__":
    main()