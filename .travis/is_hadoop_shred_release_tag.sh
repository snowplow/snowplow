#!/bin/bash

tag=$1
cicd=${tag:0:13}
release=${tag:13}

if [ "${cicd}" == "hadoop_shred/" ]; then
    if [ "${release}" == "" ]; then 
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
    exit 0
else 
    exit 1
fi
