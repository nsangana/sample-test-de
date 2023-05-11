# Databricks notebook source
# DBTITLE 1,Init Script for the cluster  - this is most of the work
dbutils.fs.put("/home/alex.barreto@databricks.com/scripts/axa-init.sh", """
#!/bin/bash

set -ex

echo "**** Scrubadubdub and dependencies ****"

apt-get update

# Requirements for Scrubadub.
apt-get -y install  autoconf automake expect

# openjdk-14-jre

# The following should be installed already. Keep them to make the script self-contained.
apt-get -y install python-dev libsnappy-dev python-pip gcc curl libtool pkg-config
apt-get -y install python3 python3-dev python3-pip python3-setuptools poppler-utils tesseract-ocr libmagic1

echo "**** Downloading Libraries ****"


echo "**** Installing  Python dependencies ****"

#'spacy-nightly[transformers]>=3.0.0rc3'
pip install python-magic chardet typing_extensions tqdm 'pdfminer.six>=20201018' opencv-python pytesseract click stringcase python-docx nltk 
pip install spacy-nightly==3.0.0rc3x
pip install spacy-nightly[transformers]==3.0.0rc3
pip install thinc==8.0.0rc2



echo "**** Installing Scrubadubdub ****"

cd "/home/ubuntu/"
echo "install dependencies"
apt-get -y  install libtool
echo "rm libpostal"
rm -rf libpostal
echo "clone libpostal"
git clone "https://github.com/openvenues/libpostal"
cd "/home/ubuntu/libpostal"
echo "run bootstrap"
./bootstrap.sh
rm -rf "/home/ubuntu/libpostal_data"
mkdir "/home/ubuntu/libpostal_data"
echo "create data directory"
./configure --datadir="/home/ubuntu/libpostal_data"
echo "make"
make
echo "make install"
make install
echo "ldconfig"
ldconfig
echo "pip install"
pip install postal

python3 -c "import nltk; nltk.download('punkt')"
python3 -m spacy download en_core_web_trf
python3 -m spacy download en_core_web_sm

pip install git+https://github.com/LeapBeyond/scrubadub.git/#egg=scrubadub[spacy,address,stanford]
python3 -c "import scrubadub.detectors.stanford ; scrubadub.detectors.stanford.StanfordEntityDetector()._download() ; print('Downloaded Stanford NER model')"

""", True)


# COMMAND ----------

import scrubadub
scrubadub.VERSION

# COMMAND ----------

text = "My cat can be contacted on example@example.com, or 1800 555-5555"
scrubadub.clean(text)

# COMMAND ----------


