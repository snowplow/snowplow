#!/bin/bash

tag=$1
cicd=${tag:0:4}
release=${tag:4}

if [ "${cicd}" == "emr/" ]; then
    if [ "${release}" == "" ]; then 
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
    exit 0
else 
    exit 1
fi
