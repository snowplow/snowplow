#!/bin/bash

project=$1
tag=$2

slashed="${project}/"
slashed_len=${#slashed}

cicd=${tag:0:${slashed_len}}
release=${tag:${slashed_len}}

if [ "${cicd}" == "${slashed}" ]; then
    if [ "${release}" == "" ]; then
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
    exit 0
else
    exit 1
fi
