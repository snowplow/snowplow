#!/bin/bash
set -e

declare -a kinesis_app_paths=( "2-collectors/scala-stream-collector" "3-enrich/scala-kinesis-enrich" "4-storage/kinesis-elasticsearch-sink" )
declare -a kinesis_fatjars=( "snowplow-stream-collector" "snowplow-kinesis-enrich" "snowplow-elasticsearch-sink" )

declare -a GREEN='\033[0;32m'
declare -a RED='\033[0;31m'
declare -a NC='\033[0m'

# Go to parent-parent dir of this script
function cd_root() {
  source="${BASH_SOURCE[0]}"
  while [ -h "${source}" ] ; do source="$(readlink "${source}")"; done
  dir="$( cd -P "$( dirname "${source}" )/.." && pwd )"
  cd ${dir}
}

function assemble_and_copy() {
  kinesis_fatjar=$1
  sbt assembly && \
  build_version=`cat target/scala-2.10/src_managed/main/settings.scala  | grep "version =" | awk -F\" '{ print $2 }'` && \
  cp target/scala-2.10/$kinesis_fatjar-$build_version ../../dist
}

cd_root

for i in "${!kinesis_app_paths[@]}"; do
  :
  kinesis_app_path="${kinesis_app_paths[$i]}"
  kinesis_fatjar="${kinesis_fatjars[$i]}"
  app="${kinesis_app_path##*/}"
  cd ${kinesis_app_path}

  echo "======================================"
  echo "ASSEMBLING ${app}"
  echo "======================================"

  if assemble_and_copy $kinesis_fatjar ; then
    echo -e "[${GREEN}success${NC}] Assembled $kinesis_fatjar and copied to 'dist/'"
  else
    echo -e "[${RED}error${NC}] Failed to assemble $app"
    exit 0
  fi

  cd ../../
done
