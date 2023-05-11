# Databricks notebook source
# MAGIC %sh 
# MAGIC apt-get update
# MAGIC apt-get  -f -y install libleptonica-dev 
# MAGIC apt-get  -f -y install tesseract-ocr
# MAGIC apt-get  -f -y install libtesseract-dev

# COMMAND ----------

# MAGIC %sh pip install pytesseract

# COMMAND ----------

# MAGIC %sh sudo find / -name "tessdata"

# COMMAND ----------

# MAGIC %sh ls /usr/share/tesseract-ocr/4.00/tessdata

# COMMAND ----------

# MAGIC %sh ls /usr/bin/tesseract

# COMMAND ----------

# MAGIC %sh export TESSDATA_PREFIX=/usr/share/tesseract-ocr/4.00/tessdata

# COMMAND ----------

import requests

image_url = "https://miro.medium.com/max/527/1*t4bo8ptFFmSNbYduTCrcKg.jpeg"
img_data = requests.get(image_url).content
with open('/databricks/driver/test-european.jpg', 'wb') as handler:
    handler.write(img_data)

# COMMAND ----------

#importing modules
import pytesseract
from PIL import Image

#reading french text from image
pytesseract.tesseract_cmd=r'/usr/bin/tesseract'
print(pytesseract.image_to_string(Image.open('test-european.jpg'), lang='eng'))

# COMMAND ----------

from PIL import Image

# If you don't have tesseract executable in your PATH, include the following:
#pytesseract.pytesseract.tesseract_cmd = r'<full_path_to_your_tesseract_executable>'
# Example tesseract_cmd = r'C:\Program Files (x86)\Tesseract-OCR\tesseract'

# Simple image to string
print(pytesseract.image_to_string(Image.open('/databricks/driver/test.png')))

# List of available languages
print(pytesseract.get_languages(config=''))

# French text image to string
print(pytesseract.image_to_string(Image.open('test-european.jpg'), lang='fra'))

# In order to bypass the image conversions of pytesseract, just use relative or absolute image path
# NOTE: In this case you should provide tesseract supported images or tesseract will return error
print(pytesseract.image_to_string('test.png'))

# Batch processing with a single file containing the list of multiple image file paths
print(pytesseract.image_to_string('images.txt'))

# Timeout/terminate the tesseract job after a period of time
try:
    print(pytesseract.image_to_string('test.jpg', timeout=2)) # Timeout after 2 seconds
    print(pytesseract.image_to_string('test.jpg', timeout=0.5)) # Timeout after half a second
except RuntimeError as timeout_error:
    # Tesseract processing is terminated
    pass

# Get bounding box estimates
print(pytesseract.image_to_boxes(Image.open('test.png')))

# Get verbose data including boxes, confidences, line and page numbers
print(pytesseract.image_to_data(Image.open('test.png')))

# Get information about orientation and script detection
print(pytesseract.image_to_osd(Image.open('test.png')))

# Get a searchable PDF
pdf = pytesseract.image_to_pdf_or_hocr('test.png', extension='pdf')
with open('test.pdf', 'w+b') as f:
    f.write(pdf) # pdf type is bytes by default

# Get HOCR output
hocr = pytesseract.image_to_pdf_or_hocr('test.png', extension='hocr')

# Get ALTO XML output
xml = pytesseract.image_to_alto_xml('test.png')

# COMMAND ----------

dbutils.fs.put("/home/miguel.peralvo@databricks.com/scripts/ul-init1.sh", """
#!/bin/bash

set -ex

echo "**** Tesseract ****"

apt-get -f -y install tesseract-ocr
pip install pytesseract
""")

# COMMAND ----------

dbutils.fs.ls("dbfs:///home/miguel.peralvo@databricks.com/scripts/ul-init.sh")

# COMMAND ----------

dbutils.fs.cat("dbfs:///home/miguel.peralvo@databricks.com/scripts/ul-init.sh")

# COMMAND ----------


