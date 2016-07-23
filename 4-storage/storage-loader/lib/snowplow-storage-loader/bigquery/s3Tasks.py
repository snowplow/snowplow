#!/usr/bin/python
import boto3
import os
import re
import yaml
from optparse import OptionParser
from process import process

s3=boto3.client('s3')
basePath = os.path.abspath(__file__ + "/..")
config = {}
DOWNLOAD_PATH=""
PROCESSED_PATH=""

def parseConfig(configPath):
	with open(configPath, 'r') as stream:
	    try:
	    	global config
	        config = yaml.load(stream)

	        print(config)
	        stream.close()
	    except yaml.YAMLError as exc:
	        print(exc)
	        stream.close()


def downloadFiles( list ):
	for s3_key in list:
	    s3_object = s3_key['Key']
	    if (not s3_object.endswith("/")) & s3_object.endswith(".gzip"):
	        s3.download_file(config["s3"]["buckets"]["enriched"]["good"]["bucket"], s3_object, DOWNLOAD_PATH + s3_object)
	    else:
	        if (not os.path.exists( DOWNLOAD_PATH + s3_object)) & (not s3_object.endswith('_SUCCESS')):
	            os.makedirs(DOWNLOAD_PATH + s3_object)

def main(config_path):
	if config_path == None:
		config_path = 'config/config.yml'
	parseConfig(config_path)
	global DOWNLOAD_PATH
	DOWNLOAD_PATH =  os.path.join( basePath , config["download"]["folder"] + "/")
	PROCESSED_PATH =  os.path.join( basePath , config["processed"]["folder"] + "/")
	list=s3.list_objects(Bucket=config["s3"]["buckets"]["enriched"]["good"]["bucket"], Prefix=(config["s3"]["buckets"]["enriched"]["good"]["path"] + "/"))['Contents']
	downloadFiles( list )
	process(in_dir=DOWNLOAD_PATH, out_dir=PROCESSED_PATH)

if __name__ == "__main__":
	parser = OptionParser()
	parser.add_option("-c","--config",dest="config_path")
	(options, args) = parser.parse_args()
	main(config_path=options.config_path)