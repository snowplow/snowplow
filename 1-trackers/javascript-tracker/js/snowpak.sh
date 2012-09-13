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

# Now set user-friendly variables for args.
yuic_path=${1}/$YUIC_JARPATH



usage() {
	echo "Usage: ${0} yuicpath"
	echo $'\tyuicpath = path to YUICompressor 2.4.2 e.g. /opt/java/yuicompressor-2.4.2'
	exit 1
}

# Initial validation.
if [ ${#} -ne 1 ]; then
  usage
fi

validate_args() {
  if [ ! -f ${yuic_path} ];then
    echo "Cannot find YUICompressor 2.4.2 jarfile at ${yuic_path}"
    usage	
  fi
}

combine_files() {
  cat $DEPENDENCIES_FILE | while read FILE; do cat $FILE; done
}

filter_out_debug() {
  sed '/<DEBUG>/,/<\/DEBUG>/d'
}

yui_compress() {
  java -jar ${yuic_path} --type js --line-break 1000 
}

compress() {
  sed 's/eval/replacedEvilString/' | yui_compress | sed 's/replacedEvilString/eval/'
}



validate_args

echo "Running minification..."

combine_files | filter_out_debug | compress > $SP_OUTPUTFILE

exit 0
