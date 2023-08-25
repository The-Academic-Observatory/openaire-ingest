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

from multiprocessing import cpu_count
import os
import argparse

from typing import Optional
from openaire.gcs import gcs_upload_files
from openaire.bigquery import bq_load_table
from openaire.data import download_from_zenodo


from openaire.config import WorkflowConfig, CloudWorkspace
from openaire.config import create_config


class OpeniareRelease:
    def __init__(self):
        """Create a release instance for the workflow."""


class OpenaireWorkflow:
    def __init__(
        self,
        processes=cpu_count(),
        config_path: Optional[str] = "config.yaml",
    ):
        """Openaire workflow to ingest the"""

        self.processes = processes

        ### Read in the config file and get the required.
        self.cloud_workspace, self.workflow_config = create_config(config_path)

        print(self.cloud_workspace)
        print(self.workflow_config)

        # Create a release instance with the required config.

    def setup(self):
        """Read in the config file and create the necessary Table and CloudWorkspace objects."""

    def download(self):
        """Download files for a list of given tables from Zenodo."""

    def decompress(self):
        """Expand the downloaded files for each table."""

    def transform(self):
        """Transform the required"""

    def gcs_upload(self):
        """Uplaod local files to GCS bucket."""

    def bq_import(self):
        """Ingest the tables from GCS to BQ."""

    def cleanup(self):
        """Remove any all of locally downlaoded files."""

        # Delete download folder
        # and the transform folder.


def main(config_path: str):
    ###
    # Openaire Ingest Python Workflow
    ###

    # Make sure that the config file exists.
    assert os.path.exists(config_path), "Config path does not exist! {config_path}"

    # This workflow is supposed to imitate other Academic Observatory Workflows that ingest data into Bigquery.
    workflow = OpenaireWorkflow(config_path)

    # Tasks
    workflow.setup()
    workflow.download()
    workflow.decompress()
    workflow.transform()
    workflow.gcs_upload()
    workflow.bq_import()
    workflow.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config-path", type=str, required=False, help="Path to the configuration file", default="config.yaml"
    )
    args = parser.parse_args()

    main(config_path=args.config_path)
