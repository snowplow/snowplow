# Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

from invoke import run, task

import boto.s3
from boto.s3.connection import Location
from boto.s3.key import Key

import boto.emr
from boto.emr.step import InstallHiveStep, ScriptRunnerStep
from boto.emr.bootstrap_action import BootstrapAction

JAR_FILE  = "spark-data-modeling-0.1.0.jar"

S3_REGIONS = { 'us-east-1': Location.DEFAULT,
                 'us-west-1': Location.USWest,
                 'us-west-2': Location.USWest2,
                 'eu-west-1': Location.EU,
                 'ap-southeast-1': Location.APSoutheast,
                 'ap-southeast-2': Location.APSoutheast2,
                 'ap-northeast-1': Location.APNortheast,
                 'sa-east-1': Location.SAEast }

S3_LOCATIONS = {v: k for k, v in S3_REGIONS.items()}

# Taken from https://github.com/bencpeters/save-tweets/blob/ac276fac41e676ee12a426df56cbc60138a12e62/save-tweets.py
def get_valid_location(region):
    if region not in [i for i in dir(boto.s3.connection.Location) \
                               if i[0].isupper()]:
        try:
            return S3_REGIONS[region]
        except KeyError:
            raise ValueError("%s is not a known AWS location. Valid choices " \
                 "are:\n%s" % (region,  "\n".join( \
                 ["  *%s" % i for i in S3_REGIONS.keys()])))
    else:
        return getattr(Location, region)

def get_valid_region(location):
    return S3_LOCATIONS[location]

@task
def test():
    run("sbt test", pty=True)

@task
def assembly():
   run("sbt assembly", pty=True)

#@task
#def create_ec2_key(profile, ...)

#@task
#def create_public_subnet(profile, ...)

#@task
#def create_bucket(profile, bucket, region):
#    c = boto.connect_s3(profile_name=profile)
#    c.create_bucket(bucket, location=get_valid_location(region))

@task
def upload(profile, bucket):
    c = boto.connect_s3(profile_name=profile)    
    b = c.get_bucket(bucket)
    
    k2 = Key(b)
    k2.key = "jar/" + JAR_FILE
    k2.set_contents_from_filename("./target/scala-2.10/" + JAR_FILE)

@task
def run_emr(profile, bucket, ec2_keyname, vpc_subnet_id):
    c = boto.connect_s3(profile_name=profile)    
    b = c.get_bucket(bucket)    
    r = get_valid_region(b.get_location())

    bootstrap_actions = [
        BootstrapAction("Install Spark", "s3://support.elasticmapreduce/spark/install-spark", ["-x"])
    ]

    args = [
        "/home/hadoop/spark/bin/spark-submit",
        "--deploy-mode",
        "cluster",
        "--master",
        "yarn-cluster",
        "--class",
        "com.snowplowanalytics.snowplow.datamodeling.spark.DataModelingJob",
        "s3://" + bucket + "/jar/" + JAR_FILE,
        "s3n://" + bucket + "/in",
        "s3n://" + bucket + "/out"
    ]
    steps = [
        InstallHiveStep(),
        ScriptRunnerStep("Run Data Modeling", step_args=args)
    ]

    conn = boto.emr.connect_to_region(r, profile_name=profile)
    job_id = conn.run_jobflow(
        name="Spark Data Modeling",
        log_uri="s3://" + bucket + "/logs",
        ec2_keyname=ec2_keyname,
        master_instance_type="m3.xlarge",
        slave_instance_type="m3.xlarge",
        num_instances=3,
        enable_debugging=True,
        ami_version="3.6",
        steps=steps,
        bootstrap_actions=bootstrap_actions,
        job_flow_role="EMR_EC2_DefaultRole",
        service_role="EMR_DefaultRole"
    )
    print "Started jobflow " + job_id
