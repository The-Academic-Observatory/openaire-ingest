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

# Author: James Diprose, Aniek Roelofs, Alex Massen-Hane


### Read in config file and create the cloud workspace data classes.

import os
import re
import yaml
import pathlib
import logging
import pendulum
from datetime import datetime
from typing import Tuple, List
from openaire.model import Table
from dataclasses import dataclass


@dataclass
class CloudWorkspace:

    """Dataclass to hold the revelant cloud workspace parameters.

    :param project_id: The ID of the Google project.
    :param dataset_id: The ID of the dataset of where the tables will be imported.
    :param"""

    project_id: str
    dataset_id: str
    bucket_id: str
    bucket_folder: str
    data_location: str


@dataclass
class WorkflowConfig:
    """Dataclass to hold the revelant cloud workspace parameters."""

    data_path: str
    zenodo_url_path: str
    release_date: str
    download_folder: str
    decompress_folder: str
    transform_folder: str
    tables: List[Table]


def create_config(config_path: str) -> Tuple[CloudWorkspace, WorkflowConfig]:
    """Create the config for the Openaire workflow."""

    try:
        with open(config_path, "r") as file:
            config_data = yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"Error: The file {config_path} was not found.")
    except yaml.YAMLError:
        logging.error(f"Error parsing {config_path}")

    ### Create Cloud Workspace config ###

    cloud_workspace = CloudWorkspace(
        project_id=config_data["cloud_workspace"]["project_id"],
        dataset_id=config_data["cloud_workspace"]["dataset_id"],
        bucket_id=config_data["cloud_workspace"]["bucket_id"],
        data_location=config_data["cloud_workspace"]["data_location"],
        bucket_folder=config_data["cloud_workspace"]["bucket_folder"],
    )

    ### Create Workflow config ###

    # Local storage files.
    # Create the download and decompress paths.
    working_path = config_data["workflow_config"]["working_path"]
    assert os.path.exists(working_path), f"Given path does not exist: {working_path}"

    # Create data folder.
    data_path = os.path.join(working_path, "data")
    pathlib.Path(data_path).mkdir(parents=True, exist_ok=True)

    # Create download, decompress and transform folder.
    download_folder = os.path.join(data_path, "download")
    pathlib.Path(download_folder).mkdir(parents=True, exist_ok=True)

    decompress_folder = os.path.join(data_path, "decompress")
    pathlib.Path(decompress_folder).mkdir(parents=True, exist_ok=True)

    transform_folder = os.path.join(data_path, "transform")
    pathlib.Path(transform_folder).mkdir(parents=True, exist_ok=True)

    # Loop through and create the Table objects

    config_tables = config_data["workflow_config"]["tables"]
    assert config_tables, f"No tables found for the workflow to process."

    release_date = config_data["workflow_config"]["release_date"]
    assert isinstance(
        pendulum.from_format(release_date, "YYYYMMDD"), datetime
    ), f"Given release date is not a valid datetime string: {release_date}"

    tables = []
    for name, params in config_tables.items():
        # Optional params in the config file.
        try:
            alt_name = params["alt_name"]
        except KeyError:
            alt_name = None

        try:
            remove_nulls = params["remove_nulls"].split(", ")
        except KeyError:
            remove_nulls = None

        uri_prefix = f"gs://{cloud_workspace.bucket_id}/{cloud_workspace.bucket_folder}/{name}"
        gcs_uri_pattern = f"{uri_prefix}/part-*_NR.json.gz" if remove_nulls else f"{uri_prefix}/part-*.json.gz"

        # Create table objects
        table = Table(
            name=name,
            full_table_id=f"{cloud_workspace.project_id}.{cloud_workspace.dataset_id}.{name}",
            zenodo_url_path=config_data["workflow_config"]["zenodo_url_path"],
            num_parts=params["num_parts"],
            alt_name=alt_name,
            remove_nulls=remove_nulls,
            download_folder=download_folder,
            decompress_folder=decompress_folder,
            transform_folder=transform_folder,
            gcs_uri_pattern=gcs_uri_pattern,
        )
        tables.append(table)

    workflow_config = WorkflowConfig(
        data_path=config_data["workflow_config"]["data_path"],
        zenodo_url_path=config_data["workflow_config"]["zenodo_url_path"],
        release_date=release_date,
        download_folder=download_folder,
        decompress_folder=decompress_folder,
        transform_folder=transform_folder,
        tables=tables,
    )

    return cloud_workspace, workflow_config
