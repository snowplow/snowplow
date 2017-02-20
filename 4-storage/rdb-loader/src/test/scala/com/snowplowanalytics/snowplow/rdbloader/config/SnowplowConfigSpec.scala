/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader
package config

import org.specs2.Specification

import cats.syntax.either._

import io.circe._
import io.circe.yaml.parser

// This project
import SnowplowConfig.Codecs._

class SnowplowConfigSpec extends Specification { def is = s2"""
  Parse storage without folder, but with comment, using auto decoder $e1
  Parse monitoring.snowplow with custom decoder $e2
  Parse monitoring with custom decoder $e3
  Parse enrich with custom decoder $e4
  Parse s3 with custom decoder $e7
  Parse whole configuration using parse method $e8
  Get correct path in decoding failure $e9
  """

  def e1 = {

    val storageYaml =
      """
        |versions:
        |  rdb_shredder: 1.8.0
        |  hadoop_elasticsearch: 0.1.0
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(storageYaml)
    val storage = ast.flatMap(_.as[SnowplowConfig.Storage])
    storage must beRight(SnowplowConfig.Storage(SnowplowConfig.StorageVersions(Semver(1,8,0), Semver(0,1,0))))
  }

  def e2 = {
    val monitoringYaml =
      """
        |method: get
        |app_id: ADD HERE # e.g. snowplow
        |collector: ADD HERE # e.g. d3rkrsqld9gmqf.cloudfront.net
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(monitoringYaml)
    val storage = ast.flatMap(_.as[SnowplowConfig.SnowplowMonitoring])
    storage must beRight(SnowplowConfig.SnowplowMonitoring(Some(SnowplowConfig.GetMethod), Some("ADD HERE"), Some("ADD HERE")))
  }

  def e3 = {
    val monitoringYaml =
      """
        |tags: {fromString: bar} # Name-value pairs describing this job
        |logging:
        |  level: DEBUG # You can optionally switch to INFO for production
        |snowplow:
        |  method: get
        |  app_id: ADD HERE # e.g. snowplow
        |  collector: ADD HERE # e.g. d3rkrsqld9gmqf.cloudfront.net
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(monitoringYaml)
    val storage = ast.flatMap(_.as[SnowplowConfig.Monitoring])

    val snowplow = SnowplowConfig.SnowplowMonitoring(Some(SnowplowConfig.GetMethod), Some("ADD HERE"), Some("ADD HERE"))
    val logging = SnowplowConfig.Logging(SnowplowConfig.DebugLevel)
    val expected = SnowplowConfig.Monitoring(Map("fromString" -> "bar"), logging, Some(snowplow))

    storage must beRight(expected)
  }

  def e4 = {
    val enrichYaml =
      """
        |versions:
        |  spark_enrich: 1.8.0 # Version of the Hadoop Enrichment process
        |continue_on_unexpected_error: false # Set to 'true' (and set :out_errors: above) if you don't want any exceptions thrown from ETL
        |output_compression: NONE # Compression only supported with Redshift, set to NONE if you have Postgres targets. Allowed formats: NONE, GZIP
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(enrichYaml)
    val storage = ast.flatMap(_.as[SnowplowConfig.Enrich])

    val versions = SnowplowConfig.EnrichVersions(Semver(1,8,0))
    val expected = SnowplowConfig.Enrich(versions, SnowplowConfig.NoneCompression)

    storage must beRight(expected)
  }

  def e7 = {
    val s3Yaml =
      """
        |region: ADD HERE
        |buckets:
        |  assets: s3://snowplow-hosted-assets # DO NOT CHANGE unless you are hosting the jarfiles etc yourself in your own bucket
        |  jsonpath_assets: s3://myownassets/foo # If you have defined your own JSON Schemas, add the s3:// path to your own JSON Path files in your own bucket here
        |  log: s3://logs
        |  raw:
        |    in:                  # Multiple in buckets are permitted
        |      - s3n://my-in-bucket
        |    processing: s3://processing
        |    archive: s3n://archive          # e.g. s3://my-archive-bucket/raw
        |  enriched:
        |    good: s3://enriched-good       # e.g. s3://my-out-bucket/enriched/good
        |    bad: s3://enriched-bad         # e.g. s3://my-out-bucket/enriched/bad
        |    errors: s3://errors            # Leave blank unless :continue_on_unexpected_error: set to true below
        |    archive: s3://path/to/archive  # Where to archive enriched events to, e.g. s3://my-archive-bucket/enriched
        |  shredded:
        |    good: s3://shredded       # e.g. s3://my-out-bucket/shredded/good
        |    bad: s3://shredded/bad    # e.g. s3://my-out-bucket/shredded/bad
        |    errors: s3://errors     # Leave blank unless :continue_on_unexpected_error: set to true below
        |    archive: s3://archive   # Where to archive shredded events to, e.g. s3://my-archive-bucket/shredded
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(s3Yaml)
    val s3 = ast.flatMap(_.as[SnowplowConfig.SnowplowS3])

    s3 must beRight
  }

  def e8 = {
    val configYaml =
      """
        |aws:
        |  # Credentials can be hardcoded or set in environment variables
        |  access_key_id: <%= ENV['AWS_SNOWPLOW_ACCESS_KEY'] %>
        |  secret_access_key: <%= ENV['AWS_SNOWPLOW_SECRET_KEY'] %>
        |  s3:
        |    region: ADD HERE
        |    buckets:
        |      assets: s3://snowplow-hosted-assets # DO NOT CHANGE unless you are hosting the jarfiles etc yourself in your own bucket
        |      jsonpath_assets: # If you have defined your own JSON Schemas, add the s3:// path to your own JSON Path files in your own bucket here
        |      log: s3://log-bucket
        |      raw:
        |        in:                  # Multiple in buckets are permitted
        |          - s3://in-first/          # e.g. s3://my-in-bucket
        |          - s3://in-second/path/to/logs
        |        processing: s3://processing-logs/
        |        archive: s3://my-archive/    # e.g. s3://my-archive-bucket/raw
        |      enriched:
        |        good: s3://enriched-good/path/to/1       # e.g. s3://my-out-bucket/enriched/good
        |        bad: s3://snowplow-bad/PATH/to/1         # e.g. s3://my-out-bucket/enriched/bad
        |        errors:                    # Leave blank unless :continue_on_unexpected_error: set to true below
        |        archive: s3://enriched/    # Where to archive enriched events to, e.g. s3://my-archive-bucket/enriched
        |      shredded:
        |        good: s3://my-shredded-events/path/to/some/destination    # e.g. s3://my-out-bucket/shredded/good
        |        bad: s3://my_not_shredded-events/                         # e.g. s3://my-out-bucket/shredded/bad
        |        errors: s3://bucket/with/errors     # Leave blank unless :continue_on_unexpected_error: set to true below
        |        archive: s3://path                  # Where to archive shredded events to, e.g. s3://my-archive-bucket/shredded
        |  emr:
        |    ami_version: 4.5.0
        |    region: ADD HERE        # Always set this
        |    jobflow_role: EMR_EC2_DefaultRole # Created using $ aws emr create-default-roles
        |    service_role: EMR_DefaultRole     # Created using $ aws emr create-default-roles
        |    placement: ADD HERE     # Set this if not running in VPC. Leave blank otherwise
        |    ec2_subnet_id: ADD HERE # Set this if running in VPC. Leave blank otherwise
        |    ec2_key_name: ADD HERE
        |    bootstrap: []           # Set this to specify custom boostrap actions. Leave empty otherwise
        |    software:
        |      hbase:                # Optional. To launch on cluster, provide version, "0.92.0", keep quotes. Leave empty otherwise.
        |      lingual:              # Optional. To launch on cluster, provide version, "1.1", keep quotes. Leave empty otherwise.
        |    # Adjust your Hadoop cluster below
        |    jobflow:
        |      job_name: Snowplow ETL # Give your job a name
        |      master_instance_type: m1.medium
        |      core_instance_count: 2
        |      core_instance_type: m1.medium
        |      task_instance_count: 0 # Increase to use spot instances
        |      task_instance_type: m1.medium
        |      task_instance_bid: 0.015 # In USD. Adjust bid, or leave blank for non-spot-priced (i.e. on-demand) task instances
        |    bootstrap_failure_tries: 3 # Number of times to attempt the job in the event of bootstrap failures
        |    additional_info:        # Optional JSON string for selecting additional features
        |collectors:
        |  format: cloudfront # For example: 'clj-tomcat' for the Clojure Collector, 'thrift' for Thrift records, 'tsv/com.amazon.aws.cloudfront/wd_access_log' for Cloudfront access logs or 'ndjson/urbanairship.connect/v1' for UrbanAirship Connect events
        |enrich:
        |  versions:
        |    spark_enrich: 1.8.0 # Version of the Hadoop Enrichment process
        |  continue_on_unexpected_error: false # Set to 'true' (and set :out_errors: above) if you don't want any exceptions thrown from ETL
        |  output_compression: NONE # Compression only supported with Redshift, set to NONE if you have Postgres targets. Allowed formats: NONE, GZIP
        |storage:
        |  download:
        |    folder: # Postgres-only config option. Where to store the downloaded files. Leave blank for Redshift
        |  versions:
        |    rdb_shredder: 0.10.0 # Version of the Hadoop Shredding process
        |    hadoop_elasticsearch: 0.1.0 # Version of the Hadoop to Elasticsearch copying process
        |monitoring:
        |  tags: {} # Name-value pairs describing this job
        |  logging:
        |    level: DEBUG # You can optionally switch to INFO for production
        |  snowplow:
        |    method: get
        |    app_id: ADD HERE # e.g. snowplow
        |    collector: ADD HERE # e.g. d3rkrsqld9gmqf.cloudfront.net
      """.stripMargin

    val result = SnowplowConfig.parse(configYaml)
    result must beRight
  }

  def e9 = {
    // buckets.shredded.good cannot be integer
    val s3Yaml =
      """
        |region: ADD HERE
        |buckets:
        |  assets: s3://snowplow-hosted-assets # DO NOT CHANGE unless you are hosting the jarfiles etc yourself in your own bucket
        |  jsonpath_assets: # If you have defined your own JSON Schemas, add the s3:// path to your own JSON Path files in your own bucket here
        |  log: s3://log-bucket/
        |  raw:
        |    in:                  # Multiple in buckets are permitted
        |      - s3://in-first/          # e.g. s3://my-in-bucket
        |      - s3://in-second/path/to/logs
        |    processing: s3://processing-logs/
        |    archive: s3://my-archive/    # e.g. s3://my-archive-bucket/raw
        |  shredded:
        |    good: 0       # e.g. s3://my-out-bucket/shredded/good
        |    bad: s3://foo        # e.g. s3://my-out-bucket/shredded/bad
        |    errors:      # Leave blank unless :continue_on_unexpected_error: set to true below
        |    archive: s3://bar # Where to archive shredded events to, e.g. s3://my-archive-bucket/shredded
      """.stripMargin

    val ast: Either[Error, Json] = parser.parse(s3Yaml)
    val s3 = ast.flatMap(_.as[SnowplowConfig.SnowplowS3])

    val path = List(CursorOp.DownField("good"), CursorOp.DownField("shredded"), CursorOp.DownField("buckets"))
    s3.leftMap(_.asInstanceOf[DecodingFailure].history) must beLeft(path)
  }
}
