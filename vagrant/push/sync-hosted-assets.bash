#!/bin/bash
set -e

# Note - this runs on HOST: https://groups.google.com/forum/#!topic/vagrant-up/LgqE-JFAqZc 
#
# awscli in Homebrew is out of date. Installation:
#   $ curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
#   $ unzip awscli-bundle.zip
#   $ sudo ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
#   $ aws configure --profile=snowplow

master="snowplow-hosted-assets"
master_region="eu-west-1"
slave_stem="${master}-"
declare -a regions=( "us-east-1" "us-west-1" "us-west-2" "sa-east-1" "eu-central-1" "ap-southeast-1" "ap-southeast-2" "ap-northeast-1" )

for region in "${regions[@]}"
do
	:
	echo "============================================"
	echo "SYNCHRONIZING ASSETS TO ${region}"
	echo "--------------------------------------------"
	aws s3 --profile=snowplow cp s3://${master} s3://${slave_stem}${region} --include "*.*" --recursive --source-region=${master_region} --region=${region}
done
