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

import logging
import os
import pathlib
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple, List

import pendulum
import yaml

from openaire.model import Table


@dataclass
class CloudWorkspace:

    """Dataclass to hold the revelant cloud workspace parameters.

    :param project_id: The ID of the Google project.
    :param dataset_id: The ID of the dataset of where the tables will be imported.
    :param bucket_id: The ID of the cloud bucket.
    :param bucket_folder: Folder path of where to upload the Openaire data.
    :param data_location: Where the data centres are located that store the data."""

    project_id: str
    dataset_id: str
    bucket_id: str
    bucket_folder: str
    data_location: str


@dataclass
class WorkflowConfig:
    """Dataclass to hold the revelant cloud workspace parameters.

    :parm data_path: Where the data of the workflow will be stored locally.
    :param zenodo_url_path: Url of the Zenodo data dump.
    :param release_date: Release date of the data dump. Must be in YYYYMMDD for BQ table shard.
    :param download_folder: Absolute path to the download folder.
    :param decompress_folder: Absolute path to the decompress folder.
    :param tables: List of table objects that hold the table metadata.
    """

    data_path: str
    zenodo_url_path: str
    release_date: str
    download_folder: str
    decompress_folder: str
    tables: List[Table]


def create_config(config_path: str) -> Tuple[CloudWorkspace, WorkflowConfig]:
    """Create the config objects for the Openaire workflow.

    :param config_path: Path to the config path for the workflow."""

    # Load in the config file.
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

    # Create download  and decompress folder.
    download_folder = os.path.join(data_path, "download")
    pathlib.Path(download_folder).mkdir(parents=True, exist_ok=True)

    decompress_folder = os.path.join(data_path, "decompress")
    pathlib.Path(decompress_folder).mkdir(parents=True, exist_ok=True)

    # Set Google service account credentials for the workflow.
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config_data["workflow_config"]["google_secret_path"]

    # Loop through and create the table objects

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
        except TypeError:
            alt_name = None
        except KeyError:
            alt_name = None

        try:
            remove_nulls = params["remove_nulls"].split(", ")
        except TypeError:
            remove_nulls = None
        except KeyError:
            remove_nulls = None

        uri_prefix = f"gs://{cloud_workspace.bucket_id}/{cloud_workspace.bucket_folder}/{name}"
        gcs_uri_pattern = f"{uri_prefix}/*.json.gz"

        # Create table objects
        table = Table(
            name=name,
            full_table_id=f"{cloud_workspace.project_id}.{cloud_workspace.dataset_id}.{name}{release_date}",
            zenodo_url_path=config_data["workflow_config"]["zenodo_url_path"],
            num_parts=params["num_parts"],
            alt_name=alt_name,
            remove_nulls=remove_nulls,
            download_folder=download_folder,
            decompress_folder=decompress_folder,
            gcs_uri_pattern=gcs_uri_pattern,
        )
        tables.append(table)

    # Define the workflow config object
    workflow_config = WorkflowConfig(
        data_path=config_data["workflow_config"]["working_path"],
        zenodo_url_path=config_data["workflow_config"]["zenodo_url_path"],
        release_date=release_date,
        download_folder=download_folder,
        decompress_folder=decompress_folder,
        tables=tables,
    )

    return cloud_workspace, workflow_config
