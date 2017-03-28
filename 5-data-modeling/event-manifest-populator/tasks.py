# Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

import base64
import sys
from datetime import datetime

from invoke import run, task

import boto.s3
from boto.s3.connection import Location
from boto.s3.key import Key

import boto.emr
from boto.emr.step import JarStep


DIR_WITH_JAR = "./target/scala-2.11/"
JAR_FILE = "event-manifest-populator-0.1.0-rc1.jar"

S3_REGIONS = {'us-east-1': Location.DEFAULT,
              'us-west-1': Location.USWest,
              'us-west-2': Location.USWest2,
              'eu-west-1': Location.EU,
              'ap-southeast-1': Location.APSoutheast,
              'ap-southeast-2': Location.APSoutheast2,
              'ap-northeast-1': Location.APNortheast,
              'sa-east-1': Location.SAEast}

S3_LOCATIONS = {v: k for k, v in S3_REGIONS.items()}

SINCE_SHORT_FORMAT = "%Y-%m-%d"
SINCE_PRECISE_FORMAT = "%Y-%m-%d-%H-%M-%s"


def validate_input_path(s3_path):
    if not s3_path.startswith("s3://"):
        raise ValueError("Invalid enriched archive path. It should start with s3:// protocol")


def validate_since(since):
    try:
        datetime.strptime(since, SINCE_SHORT_FORMAT)
    except ValueError:
        try:
            datetime.strptime(since, SINCE_PRECISE_FORMAT)
        except ValueError:
            print("Incorrect --since format. Should conform YY-MM-dd or YY-MM-dd-HH-mm-ss format")
            sys.exit(1)


def get_valid_region(location):
    return S3_LOCATIONS[location]


def base64encode(file):
    content = open(file, 'r').read()
    if sys.version_info >= (3, 0):
        content = str.encode(content)
    return base64.urlsafe_b64encode(content)


@task
def test(ctx):
    run('sbt test', pty=True)


@task
def assembly(ctx):
    run('sbt assembly', pty=True)


@task
def upload(ctx, profile, jar_bucket):
    c = boto.connect_s3(profile_name=profile)
    b = c.get_bucket(jar_bucket)

    k2 = Key(b)
    k2.key = "jar/" + JAR_FILE
    k2.set_contents_from_filename(DIR_WITH_JAR + JAR_FILE)


@task
def run_emr(ctx, enriched_archive, storage_config, resolver, since=None, log_path=None, profile=None, ec2_keyname=None, jar=None):
    validate_input_path(enriched_archive)
    if since is not None:
        validate_since(since)
        since_arg = ["--since", since]
    else:
        since_arg = []

    c = boto.connect_s3(profile_name=profile)
    jar_bucket = c.get_bucket(enriched_archive.split("/")[2])
    r = get_valid_region(jar_bucket.get_location())

    if jar is None:
        path = "s3://snowplow-hosted-assets/5-data-modeling/event-manifest-populator/" + JAR_FILE
    else:
        path = jar

    args = [
        "spark-submit",
        "--deploy-mode",
        "cluster",

        "--class",
        "com.snowplowanalytics.snowplow.eventpopulator.Main",

        path,

        "--enriched-archive",
        enriched_archive,

        "--storage-config",
        base64encode(storage_config),

        "--resolver",
        base64encode(resolver),
    ] + since_arg

    steps = [
        JarStep("Run Event Manifest Populator Spark job", "command-runner.jar", step_args=args)
    ]

    conn = boto.emr.connect_to_region(r, profile_name=profile)
    job_id = conn.run_jobflow(
        name="Snowplow Event Manifest Populator",
        log_uri=log_path,
        ec2_keyname=ec2_keyname,
        master_instance_type="m3.xlarge",
        slave_instance_type="m3.xlarge",
        num_instances=3,
        enable_debugging=True,
        steps=steps,
        job_flow_role="EMR_EC2_DefaultRole",
        service_role="EMR_DefaultRole",
        api_params={
            'ReleaseLabel': 'emr-5.4.0',
            'Applications.member.1.Name': 'Spark',
            'Applications.member.2.Name': 'Hadoop',
        }
    )
    print("Started jobflow " + job_id)
