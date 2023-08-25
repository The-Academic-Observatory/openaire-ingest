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
import yaml
import logging
import pathlib
import importlib

from typing import Tuple

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


@dataclass
class WorkflowConfig:
    """Dataclass to hold the revelant cloud workspace parameters."""

    data_path: str


def create_config(config_path: str) -> Tuple[CloudWorkspace, WorkflowConfig]:
    """Create the config for the Openaire workflow."""

    try:
        with open(config_path, "r") as file:
            config_data = yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"Error: The file {config_path} was not found.")
    except yaml.YAMLError:
        logging.error(f"Error parsing {config_path}")

    cloud_workspace = CloudWorkspace(
        project_id=config_data["cloud_workspace"]["project_id"],
        dataset_id=config_data["cloud_workspace"]["dataset_id"],
        bucket_id=config_data["cloud_workspace"]["bucket_id"],
        data_location=config_data["cloud_workspace"]["data_location"],
    )

    # Local storage files.
    # Create the download and decompress paths.
    # os.mkdir(os.p

    # Loop through and create the Table objects.

    return cloud_workspace, WorkflowConfig()
