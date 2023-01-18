#!/bin/bash

### This script is to decompress all of the data from the Openaire data dump

# Uses a combination of "tar" and "gzip"
 
# Files from repository (in order of relative size)
openaireTarFiles=('communities_infrastructures' 'organization' 'software' 'project' 'otherresearchproduct' 'datasource' 'publication' 'relation' ) 
numFiles=(1 1 1 1 1 1 2 11 11)

# Move to data folder and create data_decompress
cd data
mkdir data_decompress
cd data_decompress

# Decompress each tar first

for downloaded in "../data_zipped"/*.tar; do

    echo Decompressing "$downloaded"
    
    tar zxvf "$downloaded" --directory .

done

# Decompresses the gz parts from each tar
# This process also deletes the *.gz secondary parts to save disk space

for expandedDir in */; do

    echo $expandedDir

    # Go into directory with gz parts
    cd $expandedDir

    for gzipPart in ./*.gz; do

        echo "Decompressing file - $gzipPart"
        gzip --decompress $gzipPart

    done

    # Exit directory
    cd ../

done


