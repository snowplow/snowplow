#!/bin/bash

# Bash script to minify snowplow.js
# Depends on YUICompressor 2.4.2 and sed
#
# Copyright 2012-2013 Snowplow Analytics Ltd
# License: http://www.opensource.org/licenses/bsd-license.php Simplified BSD


# Constants which should apply on any box
DEPENDENCIES_FILE='dependencies.txt'
FULL_OUTPUTFILE="snowplow.js"
MIN_OUTPUTFILE="sp.js"
YUIC_JARPATH="build/yuicompressor-2.4.2.jar"

usage() {
  echo "Usage: ${0} [options]"
  echo ""
  echo "Specific options:"
  echo "  -y PATH             path to YUICompressor 2.4.2 *"
  echo "  -c                  combine only (no minification or removing debug)"
  echo ""
  echo "* or set env variable YUI_COMPRESSOR_PATH instead"
  echo ""
  echo "Common options:"
  echo "  -h                  Show this message"
  exit 1
}

parse_args() {

  while getopts “cy:” opt; do
    case $opt in
      c)
        COMBINE_ONLY=true
        ;;
      y)
        YUI_COMPRESSOR_PATH=$OPTARG
        ;;
      \?)
        usage
        exit 1
        ;;
      h)
        usage
        exit
        ;;
    esac
  done
}

validate_options() {

  if [ ! $YUI_COMPRESSOR_PATH ]; then
    echo "Path to YUICompressor 2.4.2 not provided"
    usage
    exit 1
  fi

  YUI_COMPRESSOR_PATH=$YUI_COMPRESSOR_PATH'/'$YUIC_JARPATH
  if [ ! -f ${YUI_COMPRESSOR_PATH} ];then
    echo "Cannot find YUICompressor 2.4.2 jarfile at ${YUI_COMPRESSOR_PATH}"
    usage
  fi  
}

combine_files() {
  while read F; do cat $F; done <$DEPENDENCIES_FILE
}

filter_out_debug() {
  sed '/<DEBUG>/,/<\/DEBUG>/d'
}

yui_compress() {
  java -jar ${YUI_COMPRESSOR_PATH} --type js --line-break 1000
}

compress() {
  sed 's/eval/replacedEvilString/' | yui_compress | sed 's/replacedEvilString/eval/'
}

parse_args "$@"
validate_options

if [ $COMBINE_ONLY ]; then
  echo "Combining source files only..."
  combine_files > $FULL_OUTPUTFILE
else
  echo "Running minification..."
  combine_files | filter_out_debug | compress > $MIN_OUTPUTFILE
fi

exit 0
