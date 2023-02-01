# Openaire Data Ingest

This repository is for ingesting the Openaire data dump into Bigquery. Download and decompression scripts are made for unix systems with use of Bash, Curl and Python v3.8 .

## Downloading

The current tables (as of January 2023) are:

- communities_infrastructures
- organization
- software
- project
- otherresearchproduct
- datasource
- dataset (2 parts, dataset_1.tar and dataset_2.tar)
- publication (11 parts, from publciation_1 to publication_11.tar)
- relation (11 parts, from relation_1.tar to relation_11.tar)

Please read the following for more information:

https://zenodo.org/record/7488618#.Y8UGR-xBz0p

To download the Openaire data dump from Zenodo, please run the following bash script:

`./download.sh`

This will download all of the 9 Openaire tables into the following folder data/donwload/{table_name}\_{#}.tar using Curl sequentially and may take some time as Zendo is fairly slow.

## Decompress

To decompress the archived tables, please run the script:

`./decompress`

and will place all of the decompressed files into data/decompress/{table_name}. Tables come in parts from the archive, for example

```
data/decompress/relation/part_00000.txt.gz
data/decompress/relation/part_00001.txt.gz
...
...
```

This decompression script will NOT delete the orginal \*.tar archives so be wary of space limitations.

## Running the Python script

Before running the python script, please make a separate python 3.8 virtual environment and install the required packages.

Creating the python virtual environment:

`python3.8 -m venv /path/to/virtual/environment`

Enter the pyhton environment:

`source /path/to/virtual/environment/bin/activate`

Install the required packages:

`pip install -r requirements.txt`

The python script "main.py" contains functions for uploading to Google Cloud Storage and transfering to Google Bigquery. It contains a list of tables to loop through for uploading and tranfering into Google. If you do not wish to inlcude all the tables, please edit "list_of_tables" as needed.

You will also need to create a Google Cloud Storage bucket and Google Bigquery dataset for holding the Openaire data, both with the name of "openaire_data". You will also need to store this imformation as an environment variable in your bash shell so that the python script can pick it up. Run each of these lines separately:

`PROJECT_ID='<your google project ID>'`

`bucket_name='openaire_data'`

`dataset_name='openaire_data'`

### Upload to Google Cloud Storage

The function that uploads the \*.txt.gz and \*.json.gz part table files to Google Cloud Storage is upload_json_to_gcs and loops through all of the available part files in the data/decompress/{table_name} path.

It has some logic in it that will look at the exisiting files in Google Cloud Storage bucket and will not reupload them if they are already present. However, if the upload process is interrupted, then you may want to delete the last file it uploaded manually and restart the upload from that file.

### Preprocessing step

Please note that the "publication" table had issues in the "source" field when importing. Bigquery was not able to import the table with entries of:

`["Crossref",null]`

as there were both strings and nulls in the array. The python script will recognise the "publication" table and loop over all of the part table files and remove the unnecessary nulls from the "source" field. The output of this preprocessing step effectively duplicates the part table file without the nulls present and are stored as, for example:

/data/decompress/publication/part_00010_NR.txt.gz

where the "NR" stands for "nulls removed". These filtered version of the part table files are uploaded to GCS instead of the orginal part table files.

### Transferiing to Google Bigquery

Each table requires a schema for the data to be transfered into Bigquery. DO NOT rely on Google's "Auto detect" feature of the fields for the data. It can sometimes miss fields and loss of the data can occur.

The schemas for all of the tables are present in the "schemas" directory of this repo.

Although it has been implimented in the python script, I do not recommend using it to import the Openaire tables using the Google API. It is incredibly slow and has chances of errors if the process is interrupted, which is why it has been commented out.

Instead, use the web GUI of Bigquery to import the tables from GCS using a pattern match on all of the part table files, such as

`openaire_data/relation/part-*.json.gz`

Name the table appropriately and make sure that the correct schema is used for the table.

## Schemas

Only some schemas are provided with the Openaire data dump on Zenodo: https://zenodo.org/record/7492151

The rest of the table schemas were generated with the help of the following python package

https://github.com/bxparks/bigquery-schema-generator

and were edited as necessary.
