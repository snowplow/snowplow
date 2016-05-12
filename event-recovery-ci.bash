#!/bin/bash
set -e

# Constants
scala_version=2.10
root=$(pwd)
# Next two arrays MUST match up: number of elements and order
app_path="3-enrich/hadoop-event-recovery"
# TODO: version numbers shouldn't be hard-coded
fatjar="snowplow-hadoop-event-recovery-0.1.0-rc1.jar"

# Similar to Perl die
function die() {
    echo "$@" 1>&2 ; exit 1;
}

# Go to parent-parent dir of this script
function cd_root() {
    cd $root
}

# Assemble our fat jars
function assemble_fatjar() {

    echo "================================================"
    echo "ASSEMBLING FATJAR FOR Hadoop-Event-Recovery"
    echo "------------------------------------------------"

    cd $app_path && sbt assembly
    cd_root
}

cd_root

version=$1

# TODO: get AWS credentials

assemble_fatjar

aws s3 cp 
