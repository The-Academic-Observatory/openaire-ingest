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

#!/bin/bash

### This script is to decompress all of the data from the Openaire data dump to it's *.gz components.
# You will need to delete the data/decompress folder and it's contents if you wish to run this script multiple times. 
 
# Move to data folder and create decompress
cd data
mkdir decompress
cd decompress

# Decompress each tar first and place parts in their own folder, .e.g

# data/decompress/relation/(files)
# data/decompress/publication/(files)
# ... 

echo "####################################################################"
echo "Decompressing *.tar files from the archives"
echo ""

for downloaded in  "../download"/*.tar; do

    part_string="${downloaded%.*}"
    expandDir="${part_string:12}"

    echo Decompressing "$downloaded"

    # Expand files    
    tar zxvf "$downloaded" --directory .


done

### 

# It is unnecessary to decompress all the gz parts as Bigquery can read in *.gz jsonl files.

###


