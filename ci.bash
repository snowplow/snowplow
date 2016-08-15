#!/bin/bash
set -e

# Constants
bintray_package=snowplow
bintray_artifact_prefix=snowplow_kinesis_
bintray_user=$BINTRAY_SNOWPLOW_GENERIC_USER
bintray_api_key=$BINTRAY_SNOWPLOW_GENERIC_API_KEY
bintray_repository=snowplow/snowplow-generic
scala_version=2.10
dist_path=dist
root=$(pwd)

# Next four arrays MUST match up: number of elements and order
declare -a kinesis_app_packages=( "snowplow-scala-stream-collector" "snowplow-stream-enrich" "snowplow-kinesis-elasticsearch-sink")
declare -a kinesis_app_paths=( "2-collectors/scala-stream-collector" "3-enrich/stream-enrich" "4-storage/kinesis-elasticsearch-sink" )
declare -a kinesis_fatjars=( "snowplow-stream-collector" "snowplow-stream-enrich" "snowplow-elasticsearch-sink" )
# TODO: version numbers shouldn't be hard-coded
declare -a kinesis_app_versions=( "0.7.0" "0.9.0" "0.7.0")

# Similar to Perl die
function die() {
    echo "$@" 1>&2 ; exit 1;
}

# Go to parent-parent dir of this script
function cd_root() {
    cd $root
}

# Assemble our fat jars
function assemble_fatjars() {
    for kinesis_app_path in "${kinesis_app_paths[@]}"
        do
            :
            app="${kinesis_app_path##*/}"
            echo "================================================"
            echo "ASSEMBLING FATJAR FOR ${app}"
            echo "------------------------------------------------"
            cd ${kinesis_app_path} && sbt assembly
            cd_root
        done
}

# Creates BinTray versions
#
# Parameters:
# 1. package_names
# 2. package_versions
# 3. out_error (out parameter)
function create_bintray_versions() {
    [ "$#" -eq 3 ] || die "3 arguments required, $# provided"
    local __package_names=$1[@]
    local __package_versions=$2[@]
    local __out_error=$3

    package_names=("${!__package_names}")
    package_versions=("${!__package_versions}")

    for i in "${!package_names[@]}"
        do
            :
            package_name="${package_names[$i]}"
            package_version="${package_versions[$i]}"

            echo "========================================"
            echo "CREATING BINTRAY VERSION ${package_version} in package ${package_name}*"
            echo "* if it doesn't already exist"
            echo "----------------------------------------"

            http_status=`echo '{"name":"'${package_version}'","desc":"Release of '${package_name}'"}' | curl -d @- \
                "https://api.bintray.com/packages/${bintray_repository}/${package_name}/versions" \
                --write-out "%{http_code}\n" --silent --output /dev/null \
                --header "Content-Type:application/json" \
                -u${bintray_user}:${bintray_api_key}`

            http_status_class=${http_status:0:1}
            ok_classes=("2" "3")

            if [ ${http_status} == "409" ] ; then
                echo "... version ${package_version} in package ${package_name} already exists, skipping."
            elif [[ ! ${ok_classes[*]} =~ ${http_status_class} ]] ; then
                eval ${__out_error}="'BinTray API response ${http_status} is not 409 (package already exists) nor in 2xx or 3xx range'"
                break
            fi
        done
}

# Zips all of our applications into a meta zip
#
# Parameters:
# 1. artifact_version
# 2. out_artifact_name (out parameter)
# 3. out_artifact_[atj] (out parameter)
function build_meta_artifact() {
    [ "$#" -eq 3 ] || die "3 arguments required, $# provided"
    local __artifact_version=$1
    local __out_artifact_name=$2
    local __out_artifact_path=$3

    artifact_root="${bintray_artifact_prefix}${__artifact_version}"
    meta_artifact_name=`echo ${artifact_root}.zip|tr '-' '_'`

    echo "==========================================="
    echo "BUILDING ARTIFACT ${meta_artifact_name}"
    echo "-------------------------------------------"

    artifact_folder=./${dist_path}/${artifact_root}
    mkdir -p ${artifact_folder}

    for i in "${!kinesis_app_paths[@]}"
        do 
            :
            kinesis_fatjar="${kinesis_fatjars[$i]}-${kinesis_app_versions[$i]}"
            fatjar_path="./${kinesis_app_paths[$i]}/target/scala-${scala_version}/${kinesis_fatjar}"
            [ -f "${fatjar_path}" ] || die "Cannot find required fatjar: ${fatjar_path}. Did you forget to update fatjar versions?"
            cp ${fatjar_path} ${artifact_folder}
        done

    meta_artifact_path=./${dist_path}/${meta_artifact_name}
    zip -rj ${meta_artifact_path} ${artifact_folder}
    eval ${__out_artifact_name}=${meta_artifact_name}
    eval ${__out_artifact_path}=${meta_artifact_path}
}

# Zips all of the individual applications
#
# Parameters:
# 1. out_artifact_names (out parameter)
# 2. out_artifact_paths (out parameter)
function build_single_artifacts() {
    [ "$#" -eq 2 ] || die "2 arguments required, $# provided"
    local __out_artifact_names=$1
    local __out_artifact_paths=$2

    single_artifact_names=()
    single_artifact_paths=()

    for i in "${!kinesis_app_paths[@]}"
        do 
            :
            kinesis_fatjar="${kinesis_fatjars[$i]}-${kinesis_app_versions[$i]}"

            # Create artifact folder
            artifact_root="${kinesis_fatjar}"
            artifact_name=`echo ${kinesis_fatjar}.zip|tr '-' '_'`

            echo "==========================================="
            echo "BUILDING ARTIFACT ${artifact_name}"
            echo "-------------------------------------------"

            artifact_folder=./${dist_path}/${artifact_root}
            mkdir -p ${artifact_folder}

            # Copy artifact to folder
            fatjar_path="./${kinesis_app_paths[$i]}/target/scala-${scala_version}/${kinesis_fatjar}"
            [ -f "${fatjar_path}" ] || die "Cannot find required fatjar: ${fatjar_path}. Did you forget to update fatjar versions?"
            cp ${fatjar_path} ${artifact_folder}

            # Zip artifact
            artifact_path=./${dist_path}/${artifact_name}
            zip -rj ${artifact_path} ${artifact_folder}

            single_artifact_names+=($artifact_name)
            single_artifact_paths+=($artifact_path)
        done
}

# Uploads seperated artifacts to BinTray
#
# Parameters:
# 1. artifact_names
# 2. artifact_paths
# 3. package_names
# 4. package_versions
# 5. out_error (out parameter)
function upload_artifacts_to_bintray() {
    [ "$#" -eq 5 ] || die "5 arguments required, $# provided"
    local __artifact_names=$1[@]
    local __artifact_paths=$2[@]
    local __package_names=$3[@]
    local __package_versions=$4[@]
    local __out_error=$5

    _artifact_names=("${!__artifact_names}")
    _artifact_paths=("${!__artifact_paths}")
    _package_names=("${!__package_names}")
    _package_versions=("${!__package_versions}")

    echo "==============================="
    echo "UPLOADING ARTIFACTS TO BINTRAY*"
    echo "* 5-10 minutes"
    echo "-------------------------------"

    for i in "${!_artifact_names[@]}"
        do
            :
            artifact_name="${_artifact_names[$i]}"
            artifact_path="${_artifact_paths[$i]}"
            package_name="${_package_names[$i]}"
            package_version="${_package_versions[$i]}"

            echo "Uploading ${artifact_name} to package ${package_name} under version ${package_version}..."  

            # Check if version file already exists
            uploaded_files=`curl \
                "https://api.bintray.com/packages/${bintray_repository}/${package_name}/versions/${package_version}/files/" \
                --silent \
                -u${bintray_user}:${bintray_api_key}`

            uploaded_file_exists=`echo ${uploaded_files} | python -c \
                "exec(\"import sys,json\\nobj=json.load(sys.stdin)\\nfor item in obj:\\n  if '${artifact_name}' == item['name']: print 'true'; break\")"`

            # If file exists within version skip
            if [ "${uploaded_file_exists}" == "true" ] ; then
                echo "... file already exists in version, skipping."
                continue
            fi

            # If file not yet uploaded
            http_status=`curl -T ${artifact_path} \
                "https://api.bintray.com/content/${bintray_repository}/${package_name}/${package_version}/${artifact_name}?publish=1&override=0" \
                -H "Transfer-Encoding: chunked" \
                --write-out "%{http_code}\n" --silent --output /dev/null \
                -u${bintray_user}:${bintray_api_key}`

            http_status_class=${http_status:0:1}
            ok_classes=("2" "3")

            if [[ ! ${ok_classes[*]} =~ ${http_status_class} ]] ; then
                eval ${__out_error}="'BinTray API response ${http_status} is not in 2xx or 3xx range'"
                break
            fi
        done
}


cd_root

version=$1

assemble_fatjars

package_names=(${kinesis_app_packages[@]} ${bintray_package})
package_versions=(${kinesis_app_versions[@]} ${version})

create_bintray_versions "package_names" "package_versions" "error"
[ "${error}" ] && die "Error creating version: ${error}"

single_artifact_names=() && single_artifact_paths=() && build_single_artifacts "single_artifact_names" "single_artifact_paths"

meta_artifact_name="" && meta_artifact_path="" && build_meta_artifact "${version}" "meta_artifact_name" "meta_artifact_path"

artifact_names=(${single_artifact_names[@]} ${meta_artifact_name})
artifact_paths=(${single_artifact_paths[@]} ${meta_artifact_path})

upload_artifacts_to_bintray "artifact_names" "artifact_paths" "package_names" "package_versions" "error"
if [ "${error}" != "" ]; then
    die "Error uploading package: ${error}"
fi
