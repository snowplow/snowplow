#!/bin/bash
set -e

# Similar to Perl die
function die() {
	echo "$@" 1>&2 ; exit 1;
}

# Check if our Vagrant box is running. Expects `vagrant status` to look like:
#
# > Current machine states:
# >
# > default                   poweroff (virtualbox)
# >
# > The VM is powered off. To restart the VM, simply run `vagrant up`
#
# Parameters:
# 1. out_running (out parameter)
function is_running {
	[ "$#" -eq 1 ] || die "1 argument required, $# provided"
	local __out_running=$1

	set +e
	vagrant status | sed -n 3p | grep -q "^default\s*running (virtualbox)$"
	local retval=${?}
	set -e
	if [ ${retval} -eq "0" ] ; then
		eval ${__out_running}=1
	else
		eval ${__out_running}=0
	fi
}

# Get version, checking we are on the latest
#
# Parameters:
# 1. out_version (out parameter)
# 2. out_error (out parameter)
function get_version {
	[ "$#" -eq 2 ] || die "2 arguments required, $# provided"
	local __out_version=$1
	local __out_error=$2

	file_version=`cat VERSION`
	tag_version=`git describe --abbrev=0 --tags`
	if [ ${file_version} != ${tag_version} ] ; then
		eval ${__out_error}="'File version ${file_version} != tag version ${tag_version}'"
	else
		eval ${__out_version}=${file_version}
	fi
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

file_version=`cat VERSION`

# Can't pass args thru vagrant push so have to prompt
read -e -p "Please enter API key for Bintray user ${bintray_user}: " bintray_api_key

vagrant ssh -c "cd /vagrant/3-enrich/emr-etl-runner && ./build.sh"

upload_artifact_to_bintray "snowplow-emr-etl-runner-$file_version" "3-enrich/emr-etl-runner/deploy/snowplow-emr-etl-runner" ""

vagrant ssh -c "cd /vagrant/4-storage/storage-loader && ./build.sh"

upload_artifact_to_bintray "snowplow-storage-loader-$file_version" "4-storage/storage-loader/snowplow-deploy/storage-loader" ""
