#!/bin/bash

tag=$1
cicd=${tag:0:25}
release=${tag:25}

if [ "${cicd}" == "event_manifest_populator/" ]; then
    if [ "${release}" == "" ]; then 
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
    exit 0
else 
    exit 1
fi
