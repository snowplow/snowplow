#!/bin/bash

PWD="`pwd`"
JAVA_LIB=GeoIPJava-1.2.0

cd ~/downloads

# First install the MaxMind Java library. It's only available in
# source form,
# so we have to download it and add it into our Java folder
wget http://www.maxmind.com/download/geoip/api/java/${JAVA_LIB}.zip
unzip ${JAVA_LIB}.zip
cp ${JAVA_LIB}/source/com ${PWD}/../src/main/java/
rm ~/downloads/${JAVA_LIB}

# Now we download the GeoIP Country (and City?) databases
# DL to /usr/local/share/GeoIP/GeoIP.dat
'
