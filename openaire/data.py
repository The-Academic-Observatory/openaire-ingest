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


### Download function

# Functions relevent for cleaning the Openaire data.

import os
import pycurl
from io import BytesIO
import subprocess
from typing import Set
from openaire.files import load_jsonl_gz, save_jsonl_gz


def download_from_zenodo(url: str, output_path: str):
    """Download a single file from Zenodo using the pycurl library.
    If the file has already been downloaded previously, it will not download it again.

    :param url: Url of the file to download.
    :param output_path: Path of the download on disk.
    :return: True if downloaded successfuflly, otherwise false.
    """

    c = pycurl.Curl()
    c.setopt(pycurl.URL, url)

    print(f"Downloading file {os.path.basename(output_path)} to {url}")

    try:
        # Check fiels already exists.
        if os.path.exists(output_path):
            print(f"Found old file. Deleting previous download and starting again.")
            os.remove(output_path)

        # Write download to file.
        with open(output_path, "wb") as f:
            c.setopt(pycurl.WRITEDATA, f)
            c.perform()

        if c.getinfo(pycurl.HTTP_CODE) == 200 or c.getinfo(pycurl.HTTP_CODE) == 206:
            print(f"File successfully resumed and downloaded: {url}")
        else:
            print(f"Failed to download the file. HTTP status code: {c.getinfo(pycurl.HTTP_CODE)} url: {url}")

    except pycurl.error as e:
        print("Error:", e)
        return False
    finally:
        c.close()

    return True


def remove_nulls(
    input_path: str,
    suspect_columns: Set[str],
    output_path: str,
):
    """
    Removes unnecessary nulls/Nones from top level columns.

    :param input_path: Path to the file with the Nones.
    :param suspect_columns: Set of columns that have the Nones. Top level to the data only.
    :param output_path: Where to write the data to file.
    """

    data = load_jsonl_gz(input_path)

    result_filtered = []
    # Go through each row of the data
    for row in data:
        # Loop through the suspect columns of data with Nones/null.
        for column in suspect_columns:
            # Sometimes this column does not exist in the data. Try is to avoid it.
            try:
                # Filter out the nones
                row[column] = [s for s in row[column] if s is not None]
            except KeyError:
                pass
                # print(f"No key of '{column}' found in file: {input_path}")

        # Add filtered data row to a list.
        result_filtered.append(row)

    save_jsonl_gz(output_path, result_filtered)
