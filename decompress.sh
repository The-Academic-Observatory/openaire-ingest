#!/bin/bash




### This script is to decompress all of the data from the Openaire data dump
# Uses a combination of packages tar and gzip. You will need to delete the data/decompress folder and it's contents
# if you wish to run this script multiple times. 
 
# Move to data folder and create data_decompress
cd data
mkdir decompress
cd decompress

# Decompress each tar first and place parts in their own folder, .e.g

# data/data_decompress/relation_1/(files)
# data/data_decompress/relation_2/(files)
# ... 

echo "####################################################################"
echo "Decompressing *.tar files from the archives"
echo ""

for downloaded in  "../download"/*.tar; do

    part_string="${downloaded%.*}"
    expandDir="${part_string:12}"
    mkdir $expandDir
    cd $expandDir

    echo Decompressing "$downloaded"

    # Expand files    
    tar zxvf "../$downloaded" --directory .

    cd ../

done

# Decompresses the gz parts from each tar
# This process also deletes the *.gz secondary parts to save disk space

echo "####################################################################"
echo "Decompressing *.gz files from the archives"
echo ""

for expandedDir in */; do

    echo $expandedDir

    # Go into directory with gz parts
    cd $expandedDir

    folder=$(ls)
    for gzipPart in ${folder}/*.gz; do

        echo "Decompressing file - $gzipPart"
        gzip --decompress $gzipPart --force

        # Move to parent folder
        mv "${gzipPart%.gz}" .

    done

    echo ""

    # Remove unnecessary folder
    rm -r "${folder}"

    # Exit directory
    cd ../

done
