#!/usr/bin/env python

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
import subprocess
from datetime import datetime

import argparse

import boto.s3
from boto.s3.key import Key

import boto.emr
from boto.emr.step import JarStep


DIR_WITH_JAR = "./target/scala-2.11/"
JAR_FILE = "event-manifest-populator-0.1.1.jar"

SINCE_SHORT_FORMAT = "%Y-%m-%d"
SINCE_PRECISE_FORMAT = "%Y-%m-%d-%H-%M-%S"


def split_full_path(path):
    """Return pair of bucket without protocol and path
    Arguments:
    path - valid S3 path, such as s3://somebucket/events
    >>> split_full_path('s3://mybucket/path-to-events')
    ('mybucket', 'path-to-events/')
    >>> split_full_path('s3://mybucket')
    ('mybucket', None)
    """
    if path.startswith('s3://'):
        path = path.lstrip('s3://')
    elif path.startswith('s3n://'):
        path = path.lstrip('s3n://')
    else:
        raise ValueError("S3 path should start with s3:// or s3n:// prefix")
    parts = path.split('/')
    bucket = parts[0]
    path = '/'.join(parts[1:])
    return (bucket, normalize_prefix(path))


def normalize_prefix(path):
    """Add trailing slash to prefix if it is not present
    >>> normalize_prefix("somepath")
    'somepath/'
    >>> normalize_prefix("somepath/")
    'somepath/'
    """
    if path is None or path is '' or path is '/':
        return None
    elif path.endswith('/'):
        return path
    else:
        return path + '/'


def run(command):
    output = subprocess.Popen(command.split(),
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    (stdout, stderr) = output.communicate()
    print(stdout)
    if output.returncode != 0:
        sys.exit(stderr)


def validate_input_path(s3_path):
    if not s3_path.startswith("s3://"):
        raise ValueError("Invalid enriched archive path. It should start with s3:// protocol")


def validate_since(since):
    try:
        datetime.strptime(since, SINCE_SHORT_FORMAT)
    except ValueError:
        try:
            datetime.strptime(since, SINCE_PRECISE_FORMAT)
        except ValueError as e:
            print("Incorrect --since format. Should conform {} or {} format".format(SINCE_SHORT_FORMAT, SINCE_PRECISE_FORMAT))
            sys.exit(1)


def get_valid_region(location):
    """Make sure that region is not `Location.DEFAULT` (empty string)"""
    if not location:
        return 'us-east-1'
    else:
        return location


def base64encode(file):
    content = open(file, 'r').read()
    if sys.version_info >= (3, 0):
        content = str.encode(content)
    return base64.urlsafe_b64encode(content)


def test(args):
    print("Running 'sbt test'. This may take few minutes")
    run('sbt test')


def assembly(args):
    print("Running 'sbt assembly'. This may take few minutes")
    run('sbt assembly')


def upload(args):
    (bucket, path) = split_full_path(args.jar_bucket)

    c = boto.connect_s3(profile_name=args.profile)
    b = c.get_bucket(bucket)

    k2 = Key(b)
    if path is None:
        k2.key = JAR_FILE
    else:
        k2.key = path + JAR_FILE
    k2.set_contents_from_filename(DIR_WITH_JAR + JAR_FILE)


def run_emr(args):
    validate_input_path(args.enriched_archive)
    if args.since is not None:
        validate_since(args.since)
        since_arg = ["--since", args.since]
    else:
        since_arg = []

    c = boto.connect_s3(profile_name=args.profile)
    jar_bucket = c.get_bucket(args.enriched_archive.split("/")[2])
    r = get_valid_region(jar_bucket.get_location())

    if args.jar is None:
        path = "s3://snowplow-hosted-assets/5-data-modeling/event-manifest-populator/" + JAR_FILE
    else:
        path = args.jar

    step_args = [
        "spark-submit",
        "--deploy-mode",
        "cluster",

        "--class",
        "com.snowplowanalytics.snowplow.eventpopulator.Main",

        path,

        "--enriched-archive",
        args.enriched_archive,

        "--storage-config",
        base64encode(args.storage_config),

        "--resolver",
        base64encode(args.resolver),
    ] + since_arg

    steps = [
        JarStep("Run Event Manifest Populator Spark job", "command-runner.jar", step_args=step_args)
    ]

    conn = boto.emr.connect_to_region(r, profile_name=args.profile)
    job_id = conn.run_jobflow(
        name="Snowplow Event Manifest Populator",
        log_uri=args.log_path,
        ec2_keyname=args.ec2_keyname,
        master_instance_type="m3.xlarge",
        slave_instance_type="m3.xlarge",
        num_instances=3,
        enable_debugging=True,
        steps=steps,
        job_flow_role="EMR_EC2_DefaultRole",
        service_role="EMR_DefaultRole",
        visible_to_all_users=True,
        api_params={
            'ReleaseLabel': 'emr-5.4.0',
            'Applications.member.1.Name': 'Spark',
            'Applications.member.2.Name': 'Hadoop',
        }
    )
    print("Started jobflow " + job_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Snowplow Event Manifest Populator Spark job")

    subparsers = parser.add_subparsers()

    parser_run_emr = subparsers.add_parser(
        'run_emr',
        help="Run Event Manifest Populator on EMR cluster")
    parser_run_emr.add_argument(
        'enriched_archive',
        metavar='enriched-archive',
        type=str,
        help="S3 path to enriched events archive")
    parser_run_emr.add_argument(
        'storage_config',
        metavar='storage-config',
        type=str,
        help="DynamoDB duplicate storage configuration JSON")
    parser_run_emr.add_argument(
        'resolver',
        type=str,
        help="Local path to Iglu resolver JSON configuration")
    parser_run_emr.add_argument(
        'log_path',
        type=str,
        help="S3 Log path")
    parser_run_emr.add_argument(
        '--since',
        required=False,
        type=str,
        help="Date since when upload events to duplicate storage. " \
        "{} or {} format".format(SINCE_SHORT_FORMAT, SINCE_PRECISE_FORMAT))
    parser_run_emr.add_argument(
        '--log-path',
        required=False,
        type=str,
        help="S3 path to to store EMR logs")
    parser_run_emr.add_argument(
        '--profile',
        required=False,
        type=str,
        help="AWS profile name")
    parser_run_emr.add_argument(
        '--ec2-keyname',
        required=False,
        type=str,
        help="S3 path to enriched events archive")
    parser_run_emr.add_argument(
        '--jar',
        required=False,
        type=str,
        help="S3 path for custom jar with job")
    parser_run_emr.set_defaults(func=run_emr)

    parser_test = subparsers.add_parser(
        'test',
        help="Run test suite")
    parser_test.set_defaults(func=test)

    parser_assembly = subparsers.add_parser(
        'assembly',
        help="Assembly a fat jar with Spark job")
    parser_assembly.set_defaults(func=assembly)

    parser_upload = subparsers.add_parser(
        'upload',
        help="Upload fatjar to S3 bucket")
    parser_upload.add_argument(
        'profile',
        type=str,
        help="AWS profile name")
    parser_upload.add_argument(
        'jar_bucket',
        metavar='jar-bucket',
        type=str,
        help="S3 path to upload fatjar")
    parser_upload.set_defaults(func=upload)

    args = parser.parse_args()

    args.func(args)
