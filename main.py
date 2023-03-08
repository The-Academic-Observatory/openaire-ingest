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

### Upload part tables of the Openaire database to Google Cloud Storage and transfer to Google Bigquery.

# For listing part files from decompression step
import os
from os import listdir
from os.path import isfile, join

import pandas as pd
from typing import List, Set

# Importing into Google Bigquery
from google.cloud import bigquery, storage
from google.cloud.bigquery import LoadJobConfig
from google.cloud.exceptions import NotFound

# Reading in files
import json
import gzip

# Initialise variables
data_path = "data/decompress"
schema_path = "schemas"

google_project = os.getenv("PROJECT_ID")
dataset_name = os.getenv("DATASET_NAME")
bucket_name = os.getenv("BUCKET_NAME")

# Define Google API clients.
bq_client = bigquery.Client()
gcs_client = storage.Client()


def append_string_to_table_description(full_table_name: str, string_to_add: str):

    """
    :param full_table_name: Full id of the table .e.g. project_id.dataset_id.table_id
    :param string_to_add: String to add to the table description.
    """

    table = bq_client.get_table(full_table_name)

    print(table.description)

    table.description += table.description + string_to_add

    table = bq_client.update_table(table, ["description"])

    print(f"Updated: {table.description}")


def check_if_table_exists(full_table_name: str):

    """
    Check if a table exists in Google Bigquery.

    :param full_table_name: Full id of the table .e.g. project_id.dataset_id.table_id
    :return table: If table exists, return the table proerties, otherwise None.
    """

    try:
        table = bq_client.get_table(full_table_name)
    except NotFound:
        table = None

    return table


def create_bigquery_table(full_table_name: str, table_description: str, path_to_table_schema: str):

    """
    Create Bigquery empty table with description and schema.

    :param full_table_name: Full id of the table .e.g. project_id.dataset_id.table_id
    :param table_description: Table description.
    :param path_to_table_schema: Path to the schema file for the table.
    """

    table = bigquery.Table(full_table_name)
    table.schema = bq_client.schema_from_json(path_to_table_schema)
    table.description = table_description

    bq_client.create_table(table)


def create_bq_dataset(google_project, dataset_name):

    # Create dataset for all of the part tables.
    dataset = bq_client.dataset(dataset_name)
    dataset.location = "US"
    dataset = bq_client.create_dataset(f"{google_project}.{dataset_name}", timeout=30)


def get_list_of_parts_already_in_table(full_table_name: str, file_type: str):

    """
    :param full_table_name: Full id of the table .e.g. project_id.dataset_id.table_id
    :param file_type: Type of file from the archive. (e.g. .json .json.gz .txt.gz )
    """

    table = bq_client.get_table(full_table_name)

    if table.description.split(":")[-1].split(",") == [""]:
        return []
    else:

        # description_split_end = table.description.split(":")[-1]
        parts_from_table_description = [f"{file}{file_type}" for file in table.description.split(":")[-1].split(",")]

        return parts_from_table_description


def upload_from_gcs_to_bq(part_table_name: str, full_table_id: str, uri: str, schema_file_path: str):

    """Uploads a table from GCS to Bigquery. Will append data to an existing table.

    :param full_table_id: Full table name to be uploaded to. project_name.dataset_name.table_name
    :param uri: uri path of the object to be transfered into Bigquery.
    """

    # Create bq load job
    job_config = LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

    # For testing schemas
    # job_config = LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True)

    # Include schema when uploading to Bigquery
    job_config.schema = bq_client.schema_from_json(schema_file_path)

    try:
        load_job = bq_client.load_table_from_uri(uri, full_table_id, job_config=job_config)
        print(f"{load_job.result()} - Successfully transfered {uri} to table: {full_table_id}")

    except:
        print(f"{load_job.result()} - Failed to transfer {uri} to table: {full_table_id}")


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

    # Full list of tables to upload
    list_of_tables = [
        "communities_infrastructures",
        "organization",
        "software",
        "project",
        "otherresearchproduct",
        "datasource",
        "dataset",
        "publication",
        "relation",
    ]

    # For testing
    # list_of_tables = ["relation"]

    # Loop through each of the tables to upload and transfer.
    for table_name in list_of_tables:

        # Initialise variables
        full_table_name = f"{google_project}.{dataset_name}.{table_name}"
        path_to_table_schema = f"{schema_path}/{table_name}.json"

        # Get list of all parts in the "decompress" folder
        list_of_parts = list_files_in_dir(f"{data_path}/{table_name}")
        list_of_paths_to_parts = [join(f"{data_path}/{table_name}", file) for file in list_of_parts]

        # Get list of only gz type files
        list_of_paths_to_parts_gz = [
            file for file in list_of_paths_to_parts if (file.split(".")[-1] == "gz" and not "NR" in file)
        ]
        list_of_paths_to_parts_gz.sort()

        # Get file type (for later)
        string_file_type = list_of_paths_to_parts_gz[0].split(".")[1:]
        file_type = ""
        for part in string_file_type:
            file_type += f".{part}"

        # Preprocessing step
        if table_name == "publication":

            # To tell the rest of the script to only find preprocessed files or not.
            preprocessed = True

            print(f"List of files to process: {list_of_paths_to_parts_gz}")

            list_of_paths_to_filtered_parts = []
            # Loops through each part file, removes nulls from suspected column to ensure ingest to Bigquery is OK
            for path_to_file in list_of_paths_to_parts_gz:

                part_num = path_to_file.split(".")[0].split("/")[-1]
                output_file_path = f"data/decompress/{table_name}/{part_num}_NR{file_type}"
                columns_where_Nones_exist = {"source"}

                print(f"Preprocessing table {table_name} and {part_num}")

                # If file already exisits, then it has already been preprocessed.
                if not isfile(output_file_path):

                    remove_nulls_from_field(path_to_file, columns_where_Nones_exist, output_file_path)

                    print(f"Successfully removed nulls from {columns_where_Nones_exist} in {table_name} - {part_num}")

                else:
                    print(f"Part {part_num} of {table_name} table has already been processed - {output_file_path}")

                list_of_paths_to_filtered_parts.append(output_file_path)

            # Replace with list of parts that are filtered instead
            if len(list_of_paths_to_filtered_parts) > 0:
                list_of_paths_to_parts_gz = list_of_paths_to_filtered_parts

        else:

            preprocessed = False

        parts_to_upload_local = [filepath.split("/")[-1] for filepath in list_of_paths_to_parts_gz]

        # For testing
        # Limit the number of uploads to reduce load.
        # num_parts_to_upload = len(list_of_paths_to_parts_gz)
        # if num_parts_to_upload > len(list_of_paths_to_parts_gz):
        #     num_parts_to_upload = len(list_of_paths_to_parts_gz)

        # Get list of blobs in GCS - only upload parts that are not already on GCS.
        list_gcs_blobs = gcs_client.list_blobs(bucket_name, prefix=f"{table_name}/")
        parts_already_in_gcs = [blob.name.split("/")[-1] for blob in list_gcs_blobs]

        # Resolve difference of local and cloud files
        parts_to_upload = list(set(parts_to_upload_local).difference(set(parts_already_in_gcs)))
        parts_to_upload.sort()

        if len(parts_to_upload) > 0:

            print(f"\nUploading {len(parts_to_upload)} parts of table '{table_name}' to GCS Bucket '{bucket_name}'. \n")
            print(parts_to_upload)

            # List of uris to GCS objects
            gcs_object_list_uploaded = []
            for to_upload in parts_to_upload:

                file_to_upload_to_gcs = f"{data_path}/{table_name}/{to_upload}"

                # Upload part object to cloud storage
                gcs_object_uri = upload_json_to_gcs(table_name, file_to_upload_to_gcs)

                # Append to list of GCS objects
                gcs_object_list_uploaded.append(gcs_object_uri)

            print(f"\nFinished uploading {len(parts_to_upload)} to GCS.")
            # print(f"\nChecking if need to add more parts to the {table_name} table... ")

        else:
            print(f"\nNo parts to upload.")

        ### Transferring to BQ has been removed as it is slow and unreliable.


def remove_nulls_from_field(
    path_to_file: str,
    suspect_columns: Set[str],
    output_file_name: str,
):

    """
    Removes unnecessary nulls/Nones from a suspect column or multiple colums that are causing issues with importing to Google Bigquery.
    Reads from the part *.gz files and writes to a similarly named output file. Only finds top level fields.

    :param path_to_file: Path to the file with the Nones.
    :oaram suspect_columns: Set of columns that have the Nones. Top level to the data only.
    :param output_file_name: Where to write the data to file.

    """

    # Open part table data
    with gzip.open(path_to_file, "rb") as f:
        result = [json.loads(jline) for jline in f.read().splitlines()]

    result_filtered = []
    # Go through each row of the data
    for row in result:

        # Loop through the suspect columns of data with Nones/null.
        for column in suspect_columns:

            # Sometimes this column does not exist in the data. Try is to avoid it.
            try:
                # Filter out the nones
                removed_Nones = [s for s in row[column] if s is not None]
                # Replace with filtered list.
                row[column] = removed_Nones
            except:
                None

        # Add filtered data row to a list.
        result_filtered.append(row)

    # Write data to file
    with gzip.open(output_file_name, "wt") as f:
        # In this loop with a '\n' so it is in a jsonl format.
        for row in result_filtered:
            json.dump(row, f)
            f.write("\n")


if __name__ == "__main__":

    main()
