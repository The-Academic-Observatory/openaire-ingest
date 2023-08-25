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
from dataclasses import dataclass
from typing import Union, List, Optional
from openaire.files import schema_folder as default_schema_folder


@dataclass
class Table:
    name: str
    num_parts: int
    full_table_id: str
    remove_nulls: Optional[Union[str, List[str]]]
    alt_name: Optional[str]  # Alternative name on Zenodo
    schema_path: str = os.path.join(default_schema_folder(), f"{name}.json")
