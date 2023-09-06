# OpenAIRE Data Ingest

This repository is for ingesting the OpenAIRE data dump into Google Bigquery. It is based off of how the Observatory Platform runs the Academic Observatory workflows. It has been reworked to make it easier to understand the task flow and to use it when future versions of the data dumps are released. 

## Tables

The current tables (v6.0.0, August 17 2023) are:

- communities_infrastructures
- organization
- software
- project
- otherresearchproduct_1 (just one part)
- datasource
- dataset (2 parts, dataset_1.tar and dataset_2.tar)
- publication (12 parts, from publciation_1 to publication_12.tar)
- relation (13 parts, from relation_1.tar to relation_13.tar)

Please read the following for more information:

https://doi.org/10.5281/zenodo.8238874


## Installation

Before running the workflow, please make sure you install the following packages:

On debian linux:

`sudo apt install libcurl4-openssl-dev libssl-dev`

Next, create a Python 3.8 virtual environment and install the required packages.

Creating the python virtual environment:

`python3.8 -m venv /path/to/virtual/environment`

Enter the pyhton environment:

`source /path/to/virtual/environment/bin/activate`

Install the required packages:

`pip install -r requirements.txt`

## Config file

### Workflow

The configuration file for the workflow is stored in the `config.yaml` file.

Please look through and edit the parameters as needed, for example; `zenodo_url_path` and the `release_date`, as these will change for each release of the data dump.

You will need to set an appropriate `working_path` of where the workflow will download and decompress the OpenAIRE data dump. The workflow requires approximately 500Gb of free disk space to run. You will also need to provide an absolute path to the credential file for the service account that can access Google's Cloud services. This is stored under the `google_secret_path` variable in the config file. Please see the following to assist with creating a Google service account with the required permissions for the workflow:

https://docs.observatory.academy/en/latest/tutorials/deploy_terraform.html#prepare-google-cloud-project

The list of tables that will be processed by the workflow is under the "tables" section of the config file. This is where the parameters for each table is set:

- The name of the table
  - num_parts: The number of tar parts of the table on Zenodo.
  - alt_name: Optional. Alternate name of the part files on Zenodo, if any.
  - remove_nulls: Optional. Suspect columns where nulls are required to be removed. 

### Cloud Workspace

This part of the config file defines the parameters used for connecting to the Google Cloud services. 

- project_id: The Google project ID of there the data will be uploaded/imported to.
- dataset_id: Name of the dataset in Google Bigquery where the final tables will be imported. This will be created if it doesn't already exist.
- bucket_id: The name of the Google Cloud Storage bucket of where the data will be imported. 
- bucket_folder: Path in the google cloud bucket of where the donwloaded Openaire data will be uploaded to.
- data_location: Location of the Google Cloud data centre's of where the data will be held.


## Running the workflow

To run the workflow, it is recommended that you run it in the backgroud, like so

`python3 main.py --config-path=config.yaml &> workflow_output.log &`

and then view the status of the workflow using the tail command,

`tail -f workflow_output.log`

The following are the tasks that the workflow performs:

1. Setup: The workflow will initialise the parameters for the workflow.
2. Download: Download the required part *.tar files of the table from Zenodo.
3. Decompress: Unpacks the \*.tar files to get the part-\*\*\*\*\*.json.gz files.
4. Transform: Removes any potential nulls/Nones from suspect columns defined in the config file and outputs them as part-\*\*\*\*\*_NR.json.gz, the 'NR' stands for 'nulls removed'. 
5. GCS Upload: Uploads the part files for each table to the bucket_id and bucket_folder provided.
6. BQ Import: Imports the table data from GCS to BQ, using the schemas defined in "openaire/schemas/".
7. Cleanup: Removes downloaded and decompressed files to free up disk space.

Please note that the "publication" table had issues in the "source" field when importing. Bigquery was not able to import the table with entries of:

`["Crossref",null]`

as there were both strings and nulls in the list. The source field is defined in the config.yaml file and the workflow will automatically process and use the cleaned data for the import to Bigquery.

## Schemas

Direct schemas (with descriptions) for the following tables are provided on Zenodo:

- community_infrastructure
- datasource
- organization
- project
- relation

The rest of the descriptions for the data for the software, publciation, dataset and otherresearchproduct tables are stored in the results schema since these are merged into a larger relational table. The relevant descriptions have been pulled out from the results schema from Zendo and put into the software, publication, dataset and otherresearchproduct schema files.

The following package was used to assist with making schemas for the Openaire dataset:

https://github.com/bxparks/bigquery-schema-generator

and were edited as necessary.
