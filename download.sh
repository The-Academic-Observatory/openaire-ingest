#!/bin/sh

### Download Openaire data dump from Zenodo

# This bash script uses curl to download all of compressed files of the Openaire data dump on Zenodo.

# URL of the Zenodo Openaire data dump
downloadUrl='https://zenodo.org/record/7488618/files/'

# # Files from repository (in order of relative size)
filesToDownload=('communities_infrastructures' 'organization' 'software' 'project' 'otherresearchproduct' 'datasource' 'dataset' 'publication' 'relation' )

# #numFilesToDownload=(1 1 1 1 1 1 2 11 11)
numFilesToDownload=(1 1 1 1 1 1 1 1 1)

### For testing
# filesToDownload=('dataset') 
# numFilesToDownload=(2)

# Create data folder
mkdir data

# Create data_zipped and move to data zipped folder
mkdir data/data_zipped
cd data/data_zipped/

echo "Please wait while curl downloads all of the files from Zenodo."

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

done