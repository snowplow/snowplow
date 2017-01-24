#!/bin/bash

tag=$1

project_name="relational_database_shredder/"
project_name_len=${#project_name}

cicd=${tag:0:${project_name_len}}
release=${tag:${project_name_len}}

if [ "${cicd}" == "${project_name}" ]; then
    if [ "${release}" == "" ]; then 
        echo "Warning! No release specified! Ignoring."
        exit 2
    fi
    exit 0
else 
    exit 1
fi
