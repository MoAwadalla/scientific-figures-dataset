# LaTeX Figure Extractor

This repository contains a Python script that automates the extraction of figures and their captions from a set of `.gz` compressed LaTeX source files. The script processes each file to identify figure environments within the LaTeX documents, convert any included figures (especially from PDF format) to PNG images, and collect the associated captions. The extracted data is then saved into a Parquet file for efficient storage and retrieval.

## Features

- Parallel processing of multiple `.gz` files for fast extraction.
- Identification and conversion of figures embedded in LaTeX documents.
- Collection and storage of figure captions alongside image paths.
- Efficient storage of data into Parquet files.

## Prerequisites

Before using this script, you must have Python installed on your system along with the following Python packages:

You can install these dependencies using `pip`:
```pip install -r requirements.txt```

## Directory Structure

- `dataset/` - Directory where the processed datasets will be stored as Parquet files.
- `dataset/figures/` - Directory where the extracted figures will be saved in PNG format.
- `s3raw/` - The source directory where the `.gz` compressed files containing LaTeX documents are located. This can be retrived from arXiv. The format of these files when downloaded are multiple large tar files, however, the tar files must be unzipped and the .gz in each of the tar files must be in the top directory. This can be done by running the ```s3raw_processor.py``` under other scripts.

## Usage

1. Clone this repository.
2. ```python gz_raw_processor.py```
3. Once the script has finished running, check the `dataset/` and `dataset/figures/` directories for the extracted data and images.

## Script Workflow

- On execution, the script checks for and creates necessary directories.
- It then scans the `s3raw/` directory for `.gz` files to process.
- Each file is examined for `.tex` source files from which figures and captions are extracted.
- Figures in PDF format are converted to PNG images and saved in the `dataset/figures/` directory.
- Extracted image metadata and captions are stored in a Parquet file in the `dataset/` directory, one for each paper ID.
- These parquet files can then be merged to form one dataframe, but the script will not do this.