#!/bin/bash
set -e

# Constants
bintray_package=snowplow
bintray_artifact_prefix=snowplow_kinesis_
bintray_user=$BINTRAY_SNOWPLOW_GENERIC_USER
bintray_repository=snowplow/snowplow-generic
scala_version=2.10
dist_path=dist
root=$(pwd)
# Next two arrays MUST match up: number of elements and order
declare -a kinesis_app_paths=( "2-collectors/scala-stream-collector" "3-enrich/stream-enrich" "4-storage/kinesis-elasticsearch-sink" )
# TODO: version numbers shouldn't be hard-coded
declare -a kinesis_fatjars=( "snowplow-stream-collector-0.7.0-rc2" "snowplow-stream-enrich-0.8.0-rc2" "snowplow-elasticsearch-sink-0.6.0-rc2" )

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

# Create our version in BinTray. Does nothing
# if the version already exists
#
# Parameters:
# 1. package_version
# 2. out_error (out parameter)
function create_bintray_package() {
    [ "$#" -eq 2 ] || die "2 arguments required, $# provided"
    local __package_version=$1
    local __out_error=$2

	echo "========================================"
	echo "CREATING BINTRAY VERSION ${__package_version}*"
	echo "* if it doesn't already exist"
	echo "----------------------------------------"

	http_status=`echo '{"name":"'${__package_version}'","desc":"Release of '${bintray_package}'"}' | curl -d @- \
		"https://api.bintray.com/packages/${bintray_repository}/${bintray_package}/versions" \
		--write-out "%{http_code}\n" --silent --output /dev/null \
		--header "Content-Type:application/json" \
		-u${bintray_user}:${bintray_api_key}`

	http_status_class=${http_status:0:1}
	ok_classes=("2" "3")

	if [ ${http_status} == "409" ] ; then
		echo "... version ${__package_version} already exists, skipping."
	elif [[ ! ${ok_classes[*]} =~ ${http_status_class} ]] ; then
		eval ${__out_error}="'BinTray API response ${http_status} is not 409 (package already exists) nor in 2xx or 3xx range'"
	fi
}

# Zips all of our applications
#
# Parameters:
# 1. artifact_version
# 2. out_artifact_name (out parameter)
# 3. out_artifact_[atj] (out parameter)
function build_artifact() {
    [ "$#" -eq 3 ] || die "3 arguments required, $# provided"
    local __artifact_version=$1
    local __out_artifact_name=$2
    local __out_artifact_path=$3

    artifact_root="${bintray_artifact_prefix}${__artifact_version}"
    artifact_name=`echo ${artifact_root}.zip|tr '-' '_'`
	echo "==========================================="
	echo "BUILDING ARTIFACT ${artifact_name}"
	echo "-------------------------------------------"

	artifact_folder=./${dist_path}/${artifact_root}
	mkdir -p ${artifact_folder}

	for i in "${!kinesis_app_paths[@]}"
		do 
			:
			fatjar_path="./${kinesis_app_paths[$i]}/target/scala-${scala_version}/${kinesis_fatjars[$i]}"
			[ -f "${fatjar_path}" ] || die "Cannot find required fatjar: ${fatjar_path}. Did you forget to update fatjar versions?"
			cp ${fatjar_path} ${artifact_folder}
		done

	artifact_path=./${dist_path}/${artifact_name}
	zip -rj ${artifact_path} ${artifact_folder}
	eval ${__out_artifact_name}=${artifact_name}
	eval ${__out_artifact_path}=${artifact_path}
}

# Uploads our artifact to BinTray
#
# Parameters:
# 1. artifact_name
# 2. artifact_path
# 3. out_error (out parameter)
function upload_artifact_to_bintray() {
    [ "$#" -eq 3 ] || die "3 arguments required, $# provided"
    local __artifact_name=$1
    local __artifact_path=$2
    local __out_error=$3

	echo "==============================="
	echo "UPLOADING ARTIFACT TO BINTRAY*"
	echo "* 5-10 minutes"
	echo "-------------------------------"

	http_status=`curl -T ${__artifact_path} \
		"https://api.bintray.com/content/${bintray_repository}/${bintray_package}/${version}/${__artifact_name}?publish=1&override=1" \
		-H "Transfer-Encoding: chunked" \
		--write-out "%{http_code}\n" --silent --output /dev/null \
		-u${bintray_user}:${bintray_api_key}`

	http_status_class=${http_status:0:1}
	ok_classes=("2" "3")

	if [[ ! ${ok_classes[*]} =~ ${http_status_class} ]] ; then
		eval ${__out_error}="'BinTray API response ${http_status} is not in 2xx or 3xx range'"
	fi
}


cd_root

version=$1

bintray_api_key=$BINTRAY_SNOWPLOW_GENERIC_API_KEY

assemble_fatjars

create_bintray_package "${version}" "error"
[ "${error}" ] && die "Error creating package: ${error}"

artifact_name="" && artifact_path="" && build_artifact "${version}" "artifact_name" "artifact_path"

upload_artifact_to_bintray "${artifact_name}" "${artifact_path}" "error"
if [ "${error}" != "" ]; then
    die "Error uploading package: ${error}"
fi
