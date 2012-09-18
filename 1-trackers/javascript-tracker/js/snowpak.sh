#!/bin/bash

# Bash script to minify snowplow.js
# Depends on YUICompressor 2.4.2 and sed
#
# Copyright 2012 SnowPlow Analytics Ltd
# License: http://www.opensource.org/licenses/bsd-license.php Simplified BSD


# Constants which should apply on any box
DEPENDENCIES_FILE='dependencies.txt'
SP_OUTPUTFILE="sp.js"
YUIC_JARPATH="build/yuicompressor-2.4.2.jar"

ARGC=${#}
ARG1=${1}

usage() {
  echo "Usage: ${0} [yuicpath]"
  echo $'\twhere yuicpath = path to YUICompressor 2.4.2 e.g. /opt/java/yuicompressor-2.4.2'
  echo $'\tor set env variable YUI_COMPRESSOR_PATH instead'
  exit 1
}

validate_args() {
  if [ ! -f ${YUI_COMPRESSOR_PATH} ];then
    echo "Cannot find YUICompressor 2.4.2 jarfile at ${YUI_COMPRESSOR_PATH}"
    usage
  fi
}

set_yui_compressor() {
  suffix='/'$YUIC_JARPATH

  if [ ! $YUI_COMPRESSOR_PATH ]; then
    if [ $ARGC -ne 1 ]; then
      usage
    else
      YUI_COMPRESSOR_PATH=$ARG1$suffix
    fi
  else
    YUI_COMPRESSOR_PATH=$YUI_COMPRESSOR_PATH$suffix
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



set_yui_compressor
validate_args

echo "Running minification..."

combine_files | filter_out_debug | compress > $SP_OUTPUTFILE

exit 0
