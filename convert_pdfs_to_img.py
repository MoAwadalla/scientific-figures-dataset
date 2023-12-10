import os
from pdf2image import convert_from_path
from concurrent.futures import ProcessPoolExecutor

# Define the path to the directory containing the PDF files
pdf_dir = 'dataset/figures'

# Function to convert a single PDF to image and delete the PDF
def convert_and_delete(pdf_file):
    try:
        # Create the full path to the PDF file
        pdf_path = os.path.join(pdf_dir, pdf_file)
        # Convert the PDF to an image (PNG)
        images = convert_from_path(pdf_path)

        # Assuming only 1 page, save the image
        image_path = os.path.join(pdf_dir, pdf_file.rsplit(".", 1)[0] + '.png')
        images[0].save(image_path, 'PNG')

        # Delete the original PDF
        os.remove(pdf_path)

        return f"{pdf_file} has been converted and deleted."
    except Exception as e:
        return f"An error occurred with {pdf_file}: {e}"

# List all PDF files in the specified directory
pdf_files = [f for f in os.listdir(pdf_dir) if f.lower().endswith('.pdf')]

# The number of max_workers can be tuned to your specific machine
# A good starting point is the number of CPU cores available
if __name__ == '__main__':
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Map the convert_and_delete function to all PDF files
        results = executor.map(convert_and_delete, pdf_files)

        # Iterate through results (if needed)
        for result in results:
            print(result)

print("All PDF files have been processed.")