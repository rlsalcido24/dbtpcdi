#!/bin/bash

cd $1
# remove audit files

find . -type f -name "*_audit.*" -exec rm {} +

rm digen_report.txt 

cd Batch1

find . -type f -name "*.*" -exec sh -c 'dir=$(basename {}); dir="${dir%.*}"; mkdir -p "./$dir"; mv {} "./$dir/"' \;

find . -type f -name "FINWIRE*" -exec sh -c '
    for file do 
        filename=$(basename "$file")
        dir="Finwire"
        mkdir -p "./$dir" 
        mv "$file" "./$dir/"
    done' sh {} +

cd ..

cd Batch2

find . -type f -name "*.*" -exec sh -c 'dir=$(basename {}); dir="${dir%.*}"; mkdir -p "./$dir"; mv {} "./$dir/"' \;

cd ..

cd Batch3

find . -type f -name "*.*" -exec sh -c 'dir=$(basename {}); dir="${dir%.*}"; mkdir -p "./$dir"; mv {} "./$dir/"' \;

cd ..

python ../dataprep_v2.py ./Batch1/CustomerMgmt/CustomerMgmt.xml ./Batch1/CustomerMgmt/CustomerMgmt.csv

rm ./Batch1/CustomerMgmt/CustomerMgmt.xml