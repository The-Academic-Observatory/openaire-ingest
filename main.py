# Copyright 2023 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Alex Massen-Hane

### Upload part tables of the Openaire database to Goolge Bigquery and join them together.

# For listing part files from decompression step
from os import listdir
from os.path import isfile, join

import pandas as pd
from typing import List

# Importing into Google Bigquery
from google.cloud import bigquery, storage
from google.cloud.bigquery import LoadJobConfig, QueryJob, SourceFormat, CopyJobConfig, CopyJob, dataset

data_path = "data/decompress"
schema_path = "schemas"

google_project = "alex-dev-356105"
dataset_name = "openaire_data"
bucket_name = "openaire_data"

# Define Google API clients.
bq_client = bigquery.Client()
gcs_client = storage.Client()


def create_bq_dataset(google_project, dataset_name):

    # Create dataset for all of the part tables.
    dataset = bq_client.dataset(dataset_name)
    dataset.location = "US"
    dataset = bq_client.create_dataset(f"{google_project}.{dataset_name}", timeout=30)


def upload_from_gcs_to_bq(part_table_name: str, full_table_id: str, uri: str, schema_file_path: str):

    """Uploads a table from GCS to Bigquery.

    :param full_table_id: Full table name to be uploaded to. project_name.dataset_name.table_name
    :param uri: uri path of the object to be transfered into Bigquery.
    :return success: True if success, false if upload did not complete.

    """

    # Create load job
    job_config = LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

    # For testing schemas
    # job_config = LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True)

    # Include schema when uploading to Bigquery
    job_config.schema = bq_client.schema_from_json(schema_file_path)

    # print(f"Schema of the data: {job_config.schema}")

    # TODO - Add special description if it's just a singular table.
    job_config.destination_table_description = f"Part table of greater table {part_table_name}"

    try:
        load_job = bq_client.load_table_from_uri(uri, full_table_id, job_config=job_config)
        print(f"{load_job.result()} - Successfully transfered {uri} to table: {full_table_id}")
    except:
        print(f"{load_job.result()}Unable to transfer {uri} to table: {full_table_id}")


def upload_json_to_gcs(table_name: str, path_to_json: str):
    """Uploads a json object to Google Cloud Storage.

    :param table_name: Name of greater table the object is from.
    :param path_to_json: Local file path to the json object.
    :return uri: Returns the uri path to the GCS object.

    """

    blob_object = path_to_json.split("/")[-1]

    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(f"{table_name}/{blob_object}")

    try:
        blob.upload_from_filename(path_to_json)
        uri = f"gs://{blob.bucket.name}/{blob.name}"
        print(f"Successfully uploaded part file to GCS: {uri}")

        return uri

    except:
        print(f"Failed to upload blob.")


def read_in_json_parts_and_join(num_read: int, list_of_paths_to_parts: List[str]) -> pd.DataFrame:

    """Function to read in part json file into a pandas dataframe.

    :param path_to_json: The file path to the json file to be read in.
    :return: Pandas dataframe object of the part json."""

    # Create list for dataframe parts
    list_of_df_parts = []
    total_rows = 0

    num_parts = len(list_of_paths_to_parts)
    if num_read > num_parts:
        num_read = num_parts

    print(f"Reading in {num_read}/{num_parts} files.")

    for i in range(0, num_read, 1):

        part_path = list_of_paths_to_parts[i]

        # Read in table into memory
        df_part = pd.read_json(part_path, lines=True)
        list_of_df_parts.append(df_part)

        # This count is to ensure that all rows have been imported.
        total_rows += len(df_part.index)
        print(len(df_part.index), total_rows)

    # If just one part to this table
    if num_parts == 1:
        df_main = list_of_df_parts[0]
    else:
        # Join the smaller part dataframes together.
        df_main = pd.concat(list_of_df_parts)

    return df_main


def list_files_in_dir(path_to_files: str):

    """List files in a given local directroy.

    :param path_to_files: Path to the files to list.

    :return: List of strings containing the names of files present.
    """

    return [f for f in listdir(path_to_files) if isfile(join(path_to_files, f))]


def main():

    # List of tables
    # To read in and upload to bigquery

    # Full list of tables to upload
    # list_of_tables = [
    #     'communities_infrastructures',
    #     'organization',
    #     'software',
    #     'project',
    #     'otherresearchproduct',
    #     'datasource',
    #     'dataset',
    #     'publication',
    #     'relation']

    # For testing
    # list_of_tables = ["communities_infrastructures"]
    # list_of_tables = ["communities_infrastructures", "organization", "publication_1"]
    # list_of_tables = ["organization"]
    # list_of_tables = ["communities_infrastructures", "organization"]

    list_of_tables = ["project"]

    for table_name in list_of_tables:

        # Get list of all parts
        list_of_parts = list_files_in_dir(f"{data_path}/{table_name}")
        list_of_paths_to_parts = [join(f"{data_path}/{table_name}", file) for file in list_of_parts]

        # Limit the number of uploads to reduce load.
        num_parts_to_upload = len(list_of_paths_to_parts)
        if num_parts_to_upload > len(list_of_paths_to_parts):
            num_parts_to_upload = len(list_of_paths_to_parts)

        print(f"Uploading {num_parts_to_upload} parts of table {table_name} to GCS Bucket {bucket_name}. \n")

        # List of uris to GCS objects
        gcs_object_list = []

        for i in range(0, num_parts_to_upload, 1):

            # Upload part object to cloud storage
            gcs_object_uri = upload_json_to_gcs(table_name, list_of_paths_to_parts[i])

            # Append to list of GCS objects
            gcs_object_list.append(gcs_object_uri)

        print(f"\nTransfering {num_parts_to_upload} parts of table {table_name} to bigquery dataset {dataset_name}. \n")

        # Transfer uploaded files from GCS to Bigquery
        # for i in range(0, num_parts_to_upload, 1):
        for gcs_object_to_transfer in gcs_object_list:

            # Transfer part file to bigquery.
            # This will append onto an existing table.

            upload_from_gcs_to_bq(
                table_name,
                f"{google_project}.{dataset_name}.{table_name}",
                gcs_object_to_transfer,
                f"{schema_path}/{table_name}.json",
            )

    ## TODO
    ### For publication / relation table - put all smaller parts into one large table.
    # Run query to append tables.


if __name__ == "__main__":

    main()
