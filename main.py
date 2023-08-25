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

import os
import argparse
from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor

from openaire.config import create_config
from openaire.gcs import gcs_upload_files
from openaire.bigquery import bq_create_dataset, bq_load_table
from openaire.data import download_from_zenodo
from openaire.files import decompress_tar_gz, get_chunks


class OpenaireWorkflow:

    """Openaire workflow"""

    def __init__(
        self,
        max_processes=cpu_count(),
        config_path: Optional[str] = "config.yaml",
    ):
        self.max_processes = max_processes
        self.config_path = config_path

    def setup(self):
        """Read in the config file and create the necessary Table and CloudWorkspace objects."""

        ### Read in the config file and get the required.
        self.cloud_workspace, self.workflow_config = create_config(self.config_path)
        self.tables = self.workflow_config.tables

    def download(self):
        """Download files for a list of given tables from Zenodo."""

        # TODO: Make parallel

        # Loop though the tables and download the part table files in parallel.
        for table in self.tables:
            for url, output_path in table.download_paths.items():
                download_from_zenodo(url=url, output_path=output_path)

            # # Download files in batches if possible.
            # for i, chunk in enumerate(get_chunks(input_list=table.download_paths, chunk_size=self.max_processes)):
            #     with ProcessPoolExecutor(max_workers=self.max_processes) as executor:

            #         futures = []
            #         for datafile in chunk:
            #             futures.append(executor.submit(download, input_path, upsert_path))

    def decompress(self):
        """Expand the downloaded files for each table."""

        # Decompress each of the downloaded files.
        for table in self.tables:
            for download_files in table.download_paths.values():
                decompress_tar_gz(file_path=download_files, extract_path=table.decompress_folder)

            # Add the list of parts to the table metadata object
            part_list_gz = [
                os.path.join(table.part_location, file)
                for file in os.listdir(table.part_location)
                if file.endswith(".gz")
            ]
            part_list_gz.sort()
            table.local_part_list_gz = part_list_gz

    def transform(self):
        """Transform - remove nulls from selected top level columns in the data."""

        # TODO: Make parallel

        for table in self.tables:
            print(table.name)
            print(table.schema_path)
            print(table.local_part_list_gz)

            if table.remove_nulls is not None:
                print(table.remove_nulls)

    def gcs_upload(self):
        """Uplaod local files to GCS bucket."""

        for table in self.tables:
            # Get parts list incase the previous step was not run.
            part_list_gz = [
                os.path.join(table.part_location, file)
                for file in os.listdir(table.part_location)
                if file.endswith(".gz")
            ]
            part_list_gz.sort()

            uri_part_list = [
                f"{self.cloud_workspace.bucket_folder}/{table.name}/{os.path.basename(file)}"
                for file in table.local_part_list_gz
            ]

            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.bucket_id,
                file_paths=part_list_gz,
                blob_names=uri_part_list,
            )

            assert success, f"Table {table.name} files were not successfully uploaded to GCS."

    def bq_import(self):
        """Ingest the tables from GCS to BQ."""

        bq_create_dataset(
            self.cloud_workspace.project_id,
            self.cloud_workspace.dataset_id,
            self.cloud_workspace.data_location,
            description="Openaire data dump",
        )

        for table in self.tables:
            bq_load_table(
                uri=table.gcs_uri_pattern,
                table_id=table.full_table_id,
                schema_file_path=table.schema_path,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            )

    def cleanup(self):
        """Remove any all of locally downlaoded, decompressed and transformed files."""


def main(config_path: str):
    ###############################################################################
    #
    # Openaire Workflow
    #
    #
    # This workflow is supposed to imitate other Academic Observatory Workflows.
    # Appropriate functions from the Observatory Platform have been used.
    #
    ################################################################################

    # Make sure that the config file exists.
    assert os.path.exists(config_path), f"Config path does not exist! {config_path}"
    workflow = OpenaireWorkflow(config_path)

    # Tasks
    workflow.setup()
    # workflow.download()
    # workflow.decompress()
    # workflow.transform()
    # workflow.gcs_upload()
    workflow.bq_import()
    workflow.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config-path", type=str, required=False, help="Path to the configuration file", default="config.yaml"
    )
    args = parser.parse_args()

    main(config_path=args.config_path)
