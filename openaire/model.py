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
import pathlib
import re
from typing import Dict, Union, List, Optional

from openaire.files import schema_folder as default_schema_folder


class Table:

    """Table object to hold information about an OpenAIRE table.

    :param name: Name of the table.
    :param num_parts: Number of tar parts of the table on Zenodo data dump.
    :param zenodo_url_path: Url of the Zenodo record for the data dump.
    :param full_table_id: Fully qualified table name in Google Bigquery.
    :param download_folder: Absolute path to where the part table tars files will be downloaded.
    :param decompress_folder: Absolute path to where the part table tars files will be decompressed.
    :param gcs_uri_pattern: Uri glob pattern of all the part files for the table in GCS.
    :param alt_name: Alternative name of the table part file on Zenodo. e.g. otherresearchproduct_1.tar but only 1 part,
        so the file to download is otherresearchproduct_1.tar
    :param remove_nulls: Columns of where suspect nulls are that cause issues with importing to Bigquery.
    :param local_part_list_gz: List of where all the part files are locally stored (for the upload step).
    :param uri_part_list: List of all the uris of parts uploaded to Google Cloud Storage.

    """

    def __init__(
        self,
        name: str,
        num_parts: int,
        zenodo_url_path: str,
        full_table_id: str,
        download_folder: str,
        decompress_folder: str,
        gcs_uri_pattern: str,
        alt_name: Optional[str] = None,
        remove_nulls: Optional[Union[str, List[str]]] = None,
    ):
        self.name = name
        self.num_parts = num_parts
        self.zenodo_url_path = zenodo_url_path
        self.full_table_id = full_table_id
        self.remove_nulls = remove_nulls
        self.alt_name = alt_name
        self.gcs_uri_pattern = gcs_uri_pattern
        self.download_folder = os.path.join(download_folder, name)
        self.decompress_folder = os.path.join(decompress_folder)
        self.part_location = os.path.join(decompress_folder, name)
        self.zenodo_name = alt_name if alt_name else name

    @property
    def schema_path(self):
        return os.path.join(default_schema_folder(), "schemas", f"{self.name}.json")

    @property
    def download_paths(self) -> Dict[str, str]:
        """Dictionary of downloads[download_url] = download_local_file_location
        download_url ."""

        # Create the download folder for this table's data.
        pathlib.Path(self.download_folder).mkdir(parents=True, exist_ok=True)

        downloads = {}
        if self.num_parts > 1:
            for i in range(self.num_parts):
                downloads[f"{self.zenodo_url_path}/files/{self.zenodo_name}_{i+1}.tar"] = os.path.join(
                    self.download_folder, f"{self.name}_{i+1}.tar"
                )
        else:
            downloads[f"{self.zenodo_url_path}/files/{self.zenodo_name}.tar"] = os.path.join(
                self.download_folder, f"{self.name}.tar"
            )

        return downloads

    @property
    def extracted_files(self):
        files = [
            os.path.join(self.part_location, file)
            for file in os.listdir(self.part_location)
            if re.match(r".+((?<!_NR)\.json\.gz)$", file)
        ]
        files.sort()
        return files

    @property
    def transform_files(self):
        files = []
        for file in os.listdir(self.part_location):
            if (self.remove_nulls and re.match(r".+_NR\.json\.gz$", file)) or (
                not self.remove_nulls and re.match(r".+((?<!_NR)\.json\.gz)$", file)
            ):
                files.append(os.path.join(self.part_location, file))
        files.sort()
        return files
