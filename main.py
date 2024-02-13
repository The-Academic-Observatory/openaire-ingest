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


import argparse
import os
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from openaire.bigquery import bq_create_dataset, bq_load_table
from openaire.config import create_config
from openaire.data import download_from_zenodo_wget, remove_nulls
from openaire.files import decompress_tar_gz, get_chunks
from openaire.gcs import gcs_upload_files


class OpenAIREWorkflow:

    """Openaire ingest workflow"""

    def __init__(
        self,
        max_processors: int = 7,
        config_path: Optional[str] = "config.yaml",
    ):
        self.max_processors = max_processors
        self.config_path = config_path

        ### Read in the config file and get the required.
        self.cloud_workspace, self.workflow_config = create_config(self.config_path)
        self.tables = self.workflow_config.tables

    def download(self):
        """Download files for a list of given tables from Zenodo."""

        print(f"----------------------------------------------------")
        print(f"Download - Downloads the *.tar parts for each table from Zenodo.")

        # Loop though the tables and download the part table files.
        for table in self.tables:
            for url, output_path in table.download_paths.items():
                download_from_zenodo_wget(url=url, output_path=output_path)

        print(f"----------------------------------------------------")

    def decompress(self):
        """Expand the downloaded files for each table."""

        print(f"----------------------------------------------------")
        print(f"Decompress - Decompress the table *.tar parts.")

        # Decompress each of the downloaded files.
        for table in self.tables:
            print(f"Processing table: {table.name}")

            for download_files in table.download_paths.values():
                print(f"Decompressing file: {download_files}")
                decompress_tar_gz(file_path=download_files, extract_path=table.decompress_folder)

    def transform(self):
        """Transform - remove nulls from selected top level columns in the data."""

        print(f"----------------------------------------------------")
        print(f"Transform - Removing nulls from suspect columns.")

        # Use list of gz parts from previous decompress step
        for table in self.tables:
            print(f"Processing table: {table.name}")
            print(f"Files to process: {table.extracted_files}")

            if table.remove_nulls:
                for i, chunk in enumerate(get_chunks(input_list=table.extracted_files, chunk_size=7)):
                    with ProcessPoolExecutor(max_workers=7) as executor:
                        print(f"In chunk {i}: {chunk}")

                        futures = {}
                        for file_path in chunk:
                            basename = f"{os.path.basename(file_path).split('.')[0]}_NR.json.gz"
                            output_path = os.path.join(os.path.dirname(file_path), basename)

                            future = executor.submit(remove_nulls, file_path, table.remove_nulls, output_path)
                            futures[future] = output_path

                        for future in as_completed(futures):
                            output_path = futures[future]
                            print(f"Finished removing nulls from column {table.remove_nulls}: {output_path}")

                assert len(table.extracted_files) == len(
                    table.transform_files
                ), f"Number of part gz files and NR are not the same: {len(table.extracted_files)} vs {len(table.transform_files)}"

        print(f"----------------------------------------------------")

    def gcs_upload(self):
        """Upload local files to GCS bucket."""

        print(f"----------------------------------------------------")
        print(f"GCS Upload - Uploading table files to Google Cloud Storage.")

        for table in self.tables:
            uri_part_list = [
                f"{self.cloud_workspace.bucket_folder}/{table.name}/{os.path.basename(file)}"
                for file in table.transform_files
            ]

            success = gcs_upload_files(
                bucket_name=self.cloud_workspace.bucket_id,
                file_paths=table.transform_files,
                blob_names=uri_part_list,
            )

            assert success, f"Table {table.name}: Files were not successfully uploaded to GCS."

        print(f"----------------------------------------------------")

    def bq_import(self):
        """Ingest the tables from GCS to BQ."""

        print(f"----------------------------------------------------")
        print(f"BQ Import - Import tables from Google Cloud Storage to Bigquery.")

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
                ignore_unknown_values=True
            )

            print(f"Done uploading to table! {table.full_table_id}")
        print(f"----------------------------------------------------")

    def cleanup(self):
        """Remove all of locally downlaoded and decompressed files."""

        print(f"----------------------------------------------------")
        print(f"Cleanup - Remove the downloaded and decompressed files for all tables.")

        data_dir = os.path.join(self.workflow_config.data_path, "data")

        print(f"Removing data directory: {data_dir}")

        shutil.rmtree(self.workflow_config.download_folder)
        shutil.rmtree(self.workflow_config.decompress_folder)
        os.rmdir(data_dir)

        assert not os.path.exists(data_dir), f"Data path directory still exists: {data_dir}"

        print(f"----------------------------------------------------")


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
    workflow = OpenAIREWorkflow(config_path=config_path)

    print(f"Starting the OpenAIRE Workflow.")

    # Tasks
    workflow.download()
    workflow.decompress()
    workflow.transform()
    workflow.gcs_upload()
    workflow.bq_import()
    workflow.cleanup()

    print(f"Workflow is finished!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config-path",
        type=str,
        required=False,
        help="Path to the configuration file",
        default="config.yaml",
    )
    args = parser.parse_args()

    main(config_path=args.config_path)
