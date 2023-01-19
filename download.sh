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

#!/bin/sh

### Download Openaire data dump from Zenodo

# This bash script uses curl to download all of compressed files of the Openaire data dump on Zenodo.
# If download is stopped part way through, you can run this again and curl will start the download where it stopped. 

# URL of the Zenodo Openaire data dump
downloadUrl='https://zenodo.org/record/7488618/files/'

# Files from repository (in order of relative size)
filesToDownload=('communities_infrastructures' 'organization' 'software' 'project' 'otherresearchproduct' 'datasource' 'dataset' 'publication' 'relation' )
numFilesToDownload=(1 1 1 1 1 1 2 11 11)



### For testing
# numFilesToDownload=(1 1 1 1 1 1 1 1 1)
# filesToDownload=('dataset') 
# numFilesToDownload=(2)

# Create data folders
mkdir data
mkdir data/download
cd data/download/

echo "####################################################################"
echo "Please wait while curl downloads all of the Openaire data files from Zenodo."
echo ""
echo "$downloadUrl"
echo ""

for i in ${!filesToDownload[@]}; do 
    file=${filesToDownload[$i]}
    numDownload=${numFilesToDownload[$i]}

    # Download singular files

    if [ 1 -eq "$numDownload" ]; then

        echo "Downloading $downloadUrl$file.tar"
        # eval "curl -L -O -C - $downloadUrl$file.tar --output $file.tar"
        curl -L -O -C - "${downloadUrl}${file}.tar" --output "${file}.tar"

    else

    # Download part files 

        echo "Downloading $numDownload part archives of $file" 

        eval "curl -L -O -C - ${downloadUrl}${file}_{1..${numDownload}}.tar"

        # j=1
        # while [[ "$j" -lt "$numDownload" ]]; do
        #     echo "Downloading $downloadUrl${file}_$j.tar"
        #     curl -L -O -C - "${downloadUrl}${file}_$j.tar" --output "${file}_$j.tar"
        #     let j=j+1
        # done

    fi

    echo ""

done