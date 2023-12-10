from io import BytesIO
import os
import json
from PIL import Image
import base64

dataset_folder = 'dataset/'

# Iterate over all files in the dataset folder
for j, filename in enumerate(os.listdir(dataset_folder)):
  if filename.endswith('.json'):
    file_path = os.path.join(dataset_folder, filename)

    # Open the JSON file
    with open(file_path, 'r') as json_file:
      data = json.load(json_file)
      for i, d in enumerate(data):
        if 'image' not in d:
          continue
        # Extract the image field
        image = d['image']

        image = Image.open(BytesIO(base64.b64decode(image.encode('utf-8'))))

        # # Open the image using PIL

        image.save(f'{j}_{i}file.png')