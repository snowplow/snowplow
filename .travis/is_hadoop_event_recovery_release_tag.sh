#!/bin/bash

tag=$1
cicd=${tag:0:22}
release=${tag:22}

if [ "${cicd}" == "hadoop_event_recovery/" ]; then
    if [ "${release}" == "" ]; then 
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
    exit 0
else 
    exit 1
fi
