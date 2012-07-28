#!/bin/bash

# Bash script to minify snowplow.js
# Depends on YUICompressor 2.4.2 and sed

# Copyright 2012 Orderly Ltd

# Link: https://github.com/snowplow/snowplow/blob/master/docs/04_selfhosting_snowplow.md 
# Source: https://github.com/snowplow/snowplow/raw/master/tracker/js/snowpak.sh
# License: http://www.opensource.org/licenses/bsd-license.php Simplified BSD

# Constants which should apply on any box
SP_INPUTFILE="snowplow.js"
SP_OUTPUTFILE="sp.js"
YUIC_JARPATH="build/yuicompressor-2.4.2.jar"

# Usage
usage(){
	echo "Usage: ${0} yuicpath"
	echo $'\tyuicpath = path to YUICompressor 2.4.2 e.g. /opt/java/yuicompressor-2.4.2'
	exit 1
}

# Initial validation.
if [ ${#} -ne 1 ];then
	usage
fi

# Now set user-friendly variables for args.
yuic_path=${1}/$YUIC_JARPATH
sp_path=./$SP_INPUTFILE # Assume snowplow.js is in the same folder as this script

# Validate arguments
if [ ! -f ${yuic_path} ];then
	echo "Cannot find YUICompressor 2.4.2 jarfile at ${yuic_path}"
	usage	
fi
if [ ! -f ${sp_path} ];then
	echo "Cannot find snowplow.js file in this directory"
	usage	
fi

echo "Running minification..."

# Now run the minification
sed '/<DEBUG>/,/<\/DEBUG>/d' < snowplow.js | sed 's/eval/replacedEvilString/' | java -jar ${yuic_path} --type js --line-break 1000 | sed 's/replacedEvilString/eval/' > sp.js

exit 0
