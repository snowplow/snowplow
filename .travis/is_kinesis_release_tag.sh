#!/bin/bash

tag=$1
cicd=${tag:0:8}
release=${tag:8}

if [ "${cicd}" == "kinesis/" ]; then
    if [ "${release}" == "" ]; then 
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
    exit 0
else 
    exit 1
fi
