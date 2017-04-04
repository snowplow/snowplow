#!/bin/bash

tag=$1
cicd=${tag:0:14}
release=${tag:14}

if [ "${cicd}" == "hadoop_enrich/" ]; then
    if [ "${release}" == "" ]; then 
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
    exit 0
else 
    exit 1
fi
