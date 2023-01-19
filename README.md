# Openaire-Ingest

This repository is to download, join and upload the Openaire data into Bigquery.

To download the Openaire data dump from Zenodo, please run the following bash script:

./download.sh

This will download the Openaire files into the following folder of data/data_zipped/(table).tar

To decompress the archived files, please use:

./decompress

which will place all of the decompressed files into data/data_decompress/(table). It will not delete the original \*.tar archive files.

--- In developement ---

Started work on script to upload the Openaire data to Google Cloud Storage and Bigquery.

Run python script by using

python main.py

Need to make sure that the list of part tables are correct.

--- Schemas ---

Each table needs a specified schema when it's transfered from GCS to Bigquery.
